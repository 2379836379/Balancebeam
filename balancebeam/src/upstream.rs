use rand::{Rng, SeedableRng};
use std::net::SocketAddr;
use tokio::net::{lookup_host, TcpStream};
use tokio::time::timeout;

use crate::request;
use crate::response;
use crate::state::{ProxyState, IO_TIMEOUT};

fn get_candidate_upstreams(state: &ProxyState, tried: &[bool]) -> Vec<usize> {
    let dead_upstreams = state.dead_upstreams.lock();
    let mut healthy = Vec::new();
    let mut dead = Vec::new();
    for (idx, upstream) in state.upstream_addresses.iter().enumerate() {
        if tried[idx] {
            continue;
        }
        if dead_upstreams.contains(upstream) {
            dead.push(idx);
        } else {
            healthy.push(idx);
        }
    }
    if healthy.is_empty() {
        dead
    } else {
        healthy
    }
}

fn choose_upstream_power_of_two(state: &ProxyState, candidates: &[usize]) -> usize {
    if candidates.len() == 1 {
        return candidates[0];
    }

    let mut rng = rand::rngs::StdRng::from_entropy();
    let first_pos = rng.gen_range(0..candidates.len());
    let mut second_pos = rng.gen_range(0..candidates.len() - 1);
    if second_pos >= first_pos {
        second_pos += 1;
    }

    let first_idx = candidates[first_pos];
    let second_idx = candidates[second_pos];
    let active_requests = state.active_requests.lock();

    if active_requests[first_idx] <= active_requests[second_idx] {
        first_idx
    } else {
        second_idx
    }
}

async fn resolve_upstream(upstream: &str) -> Result<SocketAddr, std::io::Error> {
    let mut addrs = lookup_host(upstream).await?;
    addrs.next().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("upstream {} did not resolve to any addresses", upstream),
        )
    })
}

pub(crate) async fn connect_to_upstream(
    state: &ProxyState,
    tried: &[bool],
) -> Result<(TcpStream, usize), std::io::Error> {
    let mut last_error = None;
    let mut attempted = tried.to_vec();

    loop {
        let candidates = get_candidate_upstreams(state, &attempted);
        if candidates.is_empty() {
            break;
        }

        let upstream_idx = choose_upstream_power_of_two(state, &candidates);
        attempted[upstream_idx] = true;
        let upstream_ip = &state.upstream_addresses[upstream_idx];
        if let Some(stream) = state.take_pooled_connection(upstream_ip) {
            state.increment_active_requests(upstream_idx);
            return Ok((stream, upstream_idx));
        }
        let upstream_addr = match resolve_upstream(upstream_ip).await {
            Ok(addr) => addr,
            Err(err) => {
                log::warn!("Failed to resolve upstream {}: {}", upstream_ip, err);
                last_error = Some(err);
                continue;
            }
        };

        match timeout(IO_TIMEOUT, TcpStream::connect(upstream_addr)).await {
            Ok(Ok(stream)) => {
                state.increment_active_requests(upstream_idx);
                return Ok((stream, upstream_idx));
            }
            Ok(Err(err)) => {
                log::warn!("Failed to connect to upstream {}: {}", upstream_ip, err);
                state.mark_upstream_dead(upstream_ip);
                last_error = Some(err);
            }
            Err(_) => {
                let err = std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Timed out connecting to upstream {}", upstream_ip),
                );
                log::warn!("Failed to connect to upstream {}: {}", upstream_ip, err);
                state.mark_upstream_dead(upstream_ip);
                last_error = Some(err);
            }
        }
    }

    Err(last_error.expect("connect_to_upstream called without any upstreams"))
}

pub(crate) async fn run_active_health_checks(state: &ProxyState) {
    for upstream in &state.upstream_addresses {
        let upstream_addr = match resolve_upstream(upstream).await {
            Ok(addr) => addr,
            Err(_) => {
                state.mark_upstream_dead(upstream);
                continue;
            }
        };

        let mut stream = match timeout(IO_TIMEOUT, TcpStream::connect(upstream_addr)).await {
            Ok(Ok(stream)) => stream,
            _ => {
                state.mark_upstream_dead(upstream);
                continue;
            }
        };

        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri(&state.active_health_check_path)
            .version(http::Version::HTTP_11)
            .header("Host", upstream)
            .header("Content-Length", "0")
            .body(Vec::new())
            .unwrap();

        let healthy = timeout(IO_TIMEOUT, async {
            request::write_to_stream(&request, &mut stream).await?;
            response::read_from_stream(&mut stream, &http::Method::GET)
                .await
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))
        })
        .await
        .ok()
        .and_then(Result::ok)
        .map(|response| response.status().as_u16() < 400)
        .unwrap_or(false);

        if healthy {
            state.mark_upstream_alive(upstream);
        } else {
            state.mark_upstream_dead(upstream);
        }
    }
}
