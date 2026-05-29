use lru::LruCache;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::cache::CachedResponse;
use crate::config::CmdOptions;

pub(crate) enum HealthUpdate {
    MarkDead(String),
    MarkAlive(String),
}

pub(crate) const IO_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) struct ProxyState {
    pub(crate) active_health_check_interval: usize,
    pub(crate) active_health_check_path: String,
    pub(crate) max_requests_per_minute: usize,
    pub(crate) upstream_addresses: Vec<String>,
    pub(crate) dead_upstreams: Mutex<HashSet<String>>,
    pub(crate) health_updates: mpsc::UnboundedSender<HealthUpdate>,
    pub(crate) request_counts: Mutex<HashMap<String, VecDeque<Instant>>>,
    pub(crate) connection_pool: Mutex<HashMap<String, Vec<TcpStream>>>,
    pub(crate) active_requests: Mutex<Vec<usize>>,
    pub(crate) response_cache: Mutex<Option<LruCache<String, CachedResponse>>>,
}

impl ProxyState {
    pub(crate) fn new(
        options: CmdOptions,
        health_updates: mpsc::UnboundedSender<HealthUpdate>,
    ) -> Self {
        let upstream_count = options.upstream.len();
        Self {
            upstream_addresses: options.upstream,
            active_health_check_interval: options.active_health_check_interval,
            active_health_check_path: options.active_health_check_path,
            max_requests_per_minute: options.max_requests_per_minute,
            dead_upstreams: Mutex::new(HashSet::new()),
            health_updates,
            request_counts: Mutex::new(HashMap::new()),
            connection_pool: Mutex::new(HashMap::new()),
            active_requests: Mutex::new(vec![0; upstream_count]),
            response_cache: Mutex::new(
                NonZeroUsize::new(options.max_cache_entries).map(LruCache::new),
            ),
        }
    }

    pub(crate) fn mark_upstream_dead(&self, upstream: &str) {
        let _ = self
            .health_updates
            .send(HealthUpdate::MarkDead(upstream.to_string()));
    }

    pub(crate) fn mark_upstream_alive(&self, upstream: &str) {
        let _ = self
            .health_updates
            .send(HealthUpdate::MarkAlive(upstream.to_string()));
    }

    pub(crate) fn increment_active_requests(&self, upstream_idx: usize) {
        self.active_requests.lock()[upstream_idx] += 1;
    }

    pub(crate) fn decrement_active_requests(&self, upstream_idx: usize) {
        let mut active_requests = self.active_requests.lock();
        if active_requests[upstream_idx] > 0 {
            active_requests[upstream_idx] -= 1;
        }
    }

    pub(crate) fn take_pooled_connection(&self, upstream: &str) -> Option<TcpStream> {
        self.connection_pool
            .lock()
            .get_mut(upstream)
            .and_then(|connections| connections.pop())
    }

    pub(crate) fn return_connection_to_pool(&self, upstream: &str, stream: TcpStream) {
        self.connection_pool
            .lock()
            .entry(upstream.to_string())
            .or_default()
            .push(stream);
    }
}
