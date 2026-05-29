mod cache;
mod config;
mod proxy;
mod request;
mod response;
mod state;
mod upstream;

use clap::Parser;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use crate::config::CmdOptions;
use crate::proxy::handle_connection;
use crate::state::{HealthUpdate, ProxyState};
use crate::upstream::run_active_health_checks;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    let options = CmdOptions::parse();
    if options.upstream.is_empty() {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    let listener = match TcpListener::bind(&options.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);

    let (health_update_tx, mut health_update_rx) = mpsc::unbounded_channel();
    let state = Arc::new(ProxyState::new(options, health_update_tx));

    let health_state = Arc::clone(&state);
    tokio::spawn(async move {
        while let Some(update) = health_update_rx.recv().await {
            match update {
                HealthUpdate::MarkDead(upstream) => {
                    health_state.dead_upstreams.lock().insert(upstream);
                }
                HealthUpdate::MarkAlive(upstream) => {
                    health_state.dead_upstreams.lock().remove(&upstream);
                }
            }
        }
    });

    if state.active_health_check_interval > 0 {
        let health_check_state = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(
                    health_check_state.active_health_check_interval as u64,
                ))
                .await;
                run_active_health_checks(&health_check_state).await;
            }
        });
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    handle_connection(stream, &state).await;
                });
            }
            Err(error) => log::warn!("Failed to accept incoming connection: {}", error),
        }
    }
}
