use crate::common::server::Server;
use async_trait::async_trait;
use parking_lot::Mutex;
use rand::Rng;
use std::sync::{atomic, Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::oneshot;

#[derive(Debug)]
struct ServerState {
    pub requests_received: atomic::AtomicUsize,
}

async fn read_http_request(stream: &mut TcpStream) -> std::io::Result<Option<()>> {
    let mut buffer = Vec::new();
    let mut temp = [0_u8; 1024];
    let mut header_end = None;

    loop {
        let bytes_read = stream.read(&mut temp).await?;
        if bytes_read == 0 {
            if buffer.is_empty() {
                return Ok(None);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed mid-request",
            ));
        }
        buffer.extend_from_slice(&temp[..bytes_read]);

        if header_end.is_none() {
            header_end = buffer.windows(4).position(|window| window == b"\r\n\r\n");
        }
        if let Some(header_end_idx) = header_end {
            let headers_len = header_end_idx + 4;
            let headers = String::from_utf8_lossy(&buffer[..headers_len]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("content-length") {
                        value.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            if buffer.len() >= headers_len + content_length {
                return Ok(Some(()));
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    server_state: Arc<ServerState>,
) -> std::io::Result<()> {
    while read_http_request(&mut stream).await?.is_some() {
        server_state
            .requests_received
            .fetch_add(1, atomic::Ordering::SeqCst);
        stream
            .write_all(
                b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
            )
            .await?;
    }
    Ok(())
}

pub struct ErrorServer {
    shutdown_signal_sender: oneshot::Sender<()>,
    server_task: tokio::task::JoinHandle<()>,
    connection_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    pub address: String,
    state: Arc<ServerState>,
}

impl ErrorServer {
    #[allow(dead_code)]
    pub async fn new() -> ErrorServer {
        let mut rng = rand::thread_rng();
        ErrorServer::new_at_address(format!("127.0.0.1:{}", rng.gen_range(1024..65535))).await
    }

    #[allow(dead_code)]
    pub async fn new_at_address(bind_addr_string: String) -> ErrorServer {
        let bind_addr = bind_addr_string.parse().unwrap();
        let socket = TcpSocket::new_v4().expect("failed to create error server socket");
        socket
            .set_reuseaddr(true)
            .expect("failed to set SO_REUSEADDR for error server");
        socket
            .bind(bind_addr)
            .expect("failed to bind error server socket");
        let listener = socket.listen(1024).expect("failed to listen on error server");
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let server_state = Arc::new(ServerState {
            requests_received: atomic::AtomicUsize::new(0),
        });
        let server_task_state = server_state.clone();
        let connection_tasks = Arc::new(Mutex::new(Vec::new()));
        let server_task_connections = connection_tasks.clone();
        let server_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, _)) => {
                                let conn_state = server_task_state.clone();
                                let task = tokio::spawn(async move {
                                    if let Err(error) = handle_connection(stream, conn_state).await {
                                        log::debug!("ErrorServer connection closed: {}", error);
                                    }
                                });
                                server_task_connections.lock().push(task);
                            }
                            Err(error) => {
                                log::error!("Error accepting ErrorServer connection: {}", error);
                                break;
                            }
                        }
                    }
                }
            }
        });

        ErrorServer {
            shutdown_signal_sender: shutdown_tx,
            server_task,
            connection_tasks,
            state: server_state,
            address: bind_addr_string,
        }
    }
}

#[async_trait]
impl Server for ErrorServer {
    async fn stop(self: Box<Self>) -> usize {
        let _ = self.shutdown_signal_sender.send(());
        for task in self.connection_tasks.lock().drain(..) {
            task.abort();
        }
        self.server_task.abort();
        let _ = self.server_task.await;

        self.state.requests_received.load(atomic::Ordering::SeqCst)
    }

    fn address(&self) -> String {
        self.address.clone()
    }
}
