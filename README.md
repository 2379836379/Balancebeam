# Balancebeam

`balancebeam` is an asynchronous HTTP load balancer and reverse proxy written in Rust with Tokio.
It accepts client HTTP requests, forwards them to upstream application servers, and returns the
upstream responses back to the client.

This project started as a course assignment and has been extended with health checking, rate
limiting, in-memory caching, connection pooling, and a modern async architecture.

## Features

- Asynchronous, nonblocking I/O built on Tokio
- Reverse proxying for HTTP/1.1 requests and responses
- Multiple upstream servers
- Passive failover when an upstream connection or response fails
- Active health checks on a configurable interval and path
- IP-based sliding-window rate limiting
- In-memory response caching with LRU eviction
- Simple upstream connection pooling
- Load balancing with the Power of Two Choices algorithm
- `X-Forwarded-For` header propagation

## Architecture

The source tree is split by responsibility:

- `src/main.rs`
  Application entry point, Tokio runtime bootstrap, listener setup, and background task startup.
- `src/config.rs`
  Command-line configuration parsing with Clap.
- `src/state.rs`
  Shared proxy state such as upstream metadata, health updates, rate limiting state, connection
  pools, active request counters, and cache storage.
- `src/cache.rs`
  Cache key generation, cacheability rules, response cloning, and LRU cache access.
- `src/upstream.rs`
  Upstream selection, DNS resolution, health checks, connection reuse, and load balancing logic.
- `src/proxy.rs`
  Client connection handling, request forwarding, failover, cache lookup, and response delivery.
- `src/request.rs`
  HTTP request parsing and serialization.
- `src/response.rs`
  HTTP response parsing and serialization.

## Load Balancing Strategy

The proxy uses the Power of Two Choices algorithm:

1. Build a candidate set of currently healthy upstreams.
2. Randomly sample two candidates.
3. Compare their active in-flight request counts.
4. Choose the less loaded upstream.

If no healthy upstream is available, the proxy falls back to upstreams previously marked as dead
and retries them opportunistically.

## Caching

The proxy keeps an in-memory LRU cache for cacheable `GET` responses.

Current cache behavior:

- Only `GET` requests without a body are considered
- Requests with `Authorization` or `Cookie` headers are not cached
- Requests or responses marked with restrictive cache directives such as `no-store`, `no-cache`,
  or `private` are not cached
- Only `200 OK` responses are cached
- Responses with `Set-Cookie` are not cached

Cache capacity is limited by entry count and can be configured at startup.

## Connection Pooling

Each upstream has a simple reusable connection pool.
After a successful request/response exchange, the upstream connection may be returned to the pool
and reused by future requests.
Failed or timed-out connections are discarded instead of being recycled.

## Health Checking

The proxy supports both passive and active health checks.

- Passive health checks mark an upstream as unhealthy after connection failures, response failures,
  upstream timeouts, or upstream `5xx` responses.
- Active health checks periodically send a `GET` request to a configured path on every upstream and
  mark upstreams healthy or unhealthy based on the result.

Health updates are sent through an internal channel and applied by a dedicated state-management
task.

## Command-Line Options

The binary supports these options:

- `--bind <ADDR>`
  Address to bind the proxy to. Default: `0.0.0.0:1100`
- `--upstream <ADDR>`
  Upstream server address. This flag can be repeated multiple times.
- `--active-health-check-interval <SECONDS>`
  Interval for active health checks. Default: `10`
- `--active-health-check-path <PATH>`
  Request path used for active health checks. Default: `/`
- `--max-requests-per-minute <N>`
  Per-IP sliding-window request limit. `0` disables rate limiting. Default: `0`
- `--max-cache-entries <N>`
  Maximum number of cached responses kept in memory. `0` disables caching. Default: `256`

## Running

Example:

```bash
cargo run -- \
  --bind 127.0.0.1:1100 \
  --upstream 127.0.0.1:8001 \
  --upstream 127.0.0.1:8002 \
  --active-health-check-interval 5 \
  --active-health-check-path /health \
  --max-requests-per-minute 120 \
  --max-cache-entries 512
```

## Testing

Run the test suite with:

```bash
cargo test
```

The test suite covers:

- Basic proxying with a single upstream
- Multiple requests per client connection
- Load distribution across multiple upstreams
- Passive health checks and failover
- Active health checks
- Upstream restoration after recovery
- IP rate limiting

## Notes

- This project currently focuses on HTTP/1.1-style request/response proxying.
- The cache is fully memory-resident and optimized for low latency rather than persistence.
- The connection pool is intentionally simple and can be extended with idle eviction, maximum pool
  sizes, or smarter reuse policies.
