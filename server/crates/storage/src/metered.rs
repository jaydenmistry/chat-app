//! Timing wrappers for the two stores without a query hook: Postgres (sqlx)
//! and Redis. Each records a histogram labelled by the operation, for
//! `pg_query_seconds{query}` and `redis_command_seconds{cmd}` (docs/design.md
//! 13). The metrics recorder is installed by the server at startup; before
//! that — and in unit tests — these record into the no-op recorder.

use std::{future::Future, time::Instant};

/// Times a Postgres query.
pub async fn pg<T>(query: &'static str, fut: impl Future<Output = T>) -> T {
    let start = Instant::now();
    let out = fut.await;
    metrics::histogram!("pg_query_seconds", "query" => query).record(start.elapsed().as_secs_f64());
    out
}

/// Times a Redis command.
pub async fn redis<T>(cmd: &'static str, fut: impl Future<Output = T>) -> T {
    let start = Instant::now();
    let out = fut.await;
    metrics::histogram!("redis_command_seconds", "cmd" => cmd)
        .record(start.elapsed().as_secs_f64());
    out
}
