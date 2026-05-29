use clap::Parser;

#[derive(Parser, Debug)]
#[command(about = "Fun with load balancing")]
pub(crate) struct CmdOptions {
    #[arg(
        short,
        long,
        help = "IP/port to bind to",
        default_value = "0.0.0.0:1100"
    )]
    pub(crate) bind: String,
    #[arg(short, long, help = "Upstream host to forward requests to")]
    pub(crate) upstream: Vec<String>,
    #[arg(
        long,
        help = "Perform active health checks on this interval (in seconds)",
        default_value = "10"
    )]
    pub(crate) active_health_check_interval: usize,
    #[arg(
        long,
        help = "Path to send request to for active health checks",
        default_value = "/"
    )]
    pub(crate) active_health_check_path: String,
    #[arg(
        long,
        help = "Maximum number of requests to accept per IP per minute (0 = unlimited)",
        default_value = "0"
    )]
    pub(crate) max_requests_per_minute: usize,
    #[arg(
        long,
        help = "Maximum number of cache entries to keep in memory (0 = disabled)",
        default_value = "256"
    )]
    pub(crate) max_cache_entries: usize,
}
