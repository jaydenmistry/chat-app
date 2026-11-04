//! Connecting to the stores at startup.
//!
//! Containers start in any order, so the first connection to Redis and Scylla
//! is retried for up to a minute before the process gives up (and the
//! orchestrator restarts it). Postgres connects lazily, on first use.

use std::{fmt::Display, future::Future, time::Duration};

use anyhow::Context;
use redis::aio::ConnectionManager;
use scylla::client::session::Session;
use sqlx::PgPool;
use tokio::time::{Instant, sleep};

use crate::config::Config;

const CONNECT_DEADLINE: Duration = Duration::from_secs(60);

pub struct Stores {
    pub pg: PgPool,
    pub redis: ConnectionManager,
    pub scylla: Session,
}

pub async fn connect(config: &Config) -> anyhow::Result<Stores> {
    let pg = storage::postgres::pool(
        config.database_url.expose(),
        config.database_max_connections,
    )
    .context("invalid DATABASE_URL")?;
    let redis = retry("redis", || {
        storage::redisdb::connect(config.redis_url.expose())
    })
    .await?;
    let scylla = connect_scylla(config, Some(&config.scylla_keyspace)).await?;
    Ok(Stores { pg, redis, scylla })
}

/// Connects and waits until a query succeeds (see `storage::scylladb::connect`).
pub async fn connect_scylla(config: &Config, keyspace: Option<&str>) -> anyhow::Result<Session> {
    retry("scylla", || async {
        let session = storage::scylladb::connect(&config.scylla_nodes, keyspace)
            .await
            .map_err(|e| e.to_string())?;
        storage::scylladb::ping(&session)
            .await
            .map_err(|e| e.to_string())?;
        Ok::<_, String>(session)
    })
    .await
}

/// Calls `attempt` until it succeeds or a minute has passed, backing off from
/// 0.5 s to 5 s between tries.
pub async fn retry<T, E, F, Fut>(what: &str, mut attempt: F) -> anyhow::Result<T>
where
    E: Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let deadline = Instant::now() + CONNECT_DEADLINE;
    let mut delay = Duration::from_millis(500);
    loop {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(err) if Instant::now() + delay < deadline => {
                tracing::warn!(error = %err, retry_in_ms = delay.as_millis(), "connecting to {what} failed");
                sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(err) => anyhow::bail!("could not connect to {what}: {err}"),
        }
    }
}
