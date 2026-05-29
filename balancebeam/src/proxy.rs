use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::cache::{
    cache_key_for_request, clone_response, maybe_store_cached_response, try_get_cached_response,
};
use crate::request;
use crate::response;
use crate::state::{ProxyState, IO_TIMEOUT};
use crate::upstream::connect_to_upstream;

fn check_rate_limit(state: &ProxyState, client_ip: &str) -> bool {
    if state.max_requests_per_minute == 0 {
        return true;
    }

    let mut request_counts = state.request_counts.lock();
    let now = Instant::now();
    let window_start = now - Duration::from_secs(60);
    let entry = request_counts
        .entry(client_ip.to_string())
        .or_insert_with(std::collections::VecDeque::new);

    while let Some(&timestamp) = entry.front() {
        if timestamp < window_start {
            entry.pop_front();
        } else {
            break;
        }
    }

    if entry.len() >= state.max_requests_per_minute {
        return false;
    }

    entry.push_back(now);
    true
}

pub(crate) async fn send_response(
    client_conn: &mut TcpStream,
    response: &http::Response<Vec<u8>>,
) {
    let client_ip = client_conn
        .peer_addr()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    log::info!("{} <- {}", client_ip, response::format_response_line(response));
    if let Err(error) = response::write_to_stream(response, client_conn).await {
        log::warn!("Failed to send response to client: {}", error);
    }
}

pub(crate) async fn handle_connection(mut client_conn: TcpStream, state: &ProxyState) {
    let client_ip = client_conn
        .peer_addr()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    log::info!("Connection received from {}", client_ip);

    loop {
        let mut request = match request::read_from_stream(&mut client_conn).await {
            Ok(request) => request,
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(error) => {
                log::debug!("Error parsing request: {}", error);
                let response = response::make_http_error(match error {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(&mut client_conn, &response).await;
                continue;
            }
        };
        log::info!(
            "{} -> upstream: {}",
            client_ip,
            request::format_request_line(&request)
        );

        if !check_rate_limit(state, &client_ip) {
            let response = response::make_http_error(http::StatusCode::TOO_MANY_REQUESTS);
            send_response(&mut client_conn, &response).await;
            continue;
        }

        let cache_key = cache_key_for_request(&request);
        if let Some(cached_response) = try_get_cached_response(state, &cache_key) {
            log::debug!("Cache hit for {}", request.uri());
            send_response(&mut client_conn, &cached_response).await;
            continue;
        }

        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);

        let mut tried = vec![false; state.upstream_addresses.len()];
        let mut response_from_upstream = None;
        let mut reusable_connection = None;
        while tried.iter().any(|attempted| !attempted) {
            let (mut upstream_conn, upstream_idx) = match connect_to_upstream(state, &tried).await {
                Ok((stream, idx)) => (stream, idx),
                Err(error) => {
                    log::error!("Failed to connect to any upstream: {}", error);
                    break;
                }
            };
            tried[upstream_idx] = true;
            let upstream_ip = &state.upstream_addresses[upstream_idx];

            let upstream_result = timeout(IO_TIMEOUT, async {
                request::write_to_stream(&request, &mut upstream_conn).await?;
                response::read_from_stream(&mut upstream_conn, request.method())
                    .await
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))
            })
            .await;

            match upstream_result {
                Ok(Ok(response)) => {
                    if response.status().as_u16() >= 500 {
                        log::warn!(
                            "Upstream {} returned error status {}",
                            upstream_ip,
                            response.status()
                        );
                        state.mark_upstream_dead(upstream_ip);
                        state.decrement_active_requests(upstream_idx);
                        continue;
                    }
                    state.mark_upstream_alive(upstream_ip);
                    reusable_connection = Some((upstream_ip.clone(), upstream_conn));
                    response_from_upstream = Some(response);
                    state.decrement_active_requests(upstream_idx);
                    break;
                }
                Ok(Err(error)) => {
                    log::warn!("Error talking to upstream {}: {}", upstream_ip, error);
                    state.mark_upstream_dead(upstream_ip);
                    state.decrement_active_requests(upstream_idx);
                }
                Err(_) => {
                    log::warn!("Timed out waiting for upstream {}", upstream_ip);
                    state.mark_upstream_dead(upstream_ip);
                    state.decrement_active_requests(upstream_idx);
                }
            }
        }

        let response = match response_from_upstream {
            Some(response) => response,
            None => {
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_conn, &response).await;
                return;
            }
        };
        maybe_store_cached_response(state, &cache_key, &response);
        let client_response = clone_response(&response);
        send_response(&mut client_conn, &client_response).await;
        if let Some((upstream_ip, upstream_conn)) = reusable_connection.take() {
            state.return_connection_to_pool(&upstream_ip, upstream_conn);
        }
        log::debug!("Forwarded response to client");
    }
}
