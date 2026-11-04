//! Redis connection.
//!
//! [`ConnectionManager`] is a single multiplexed connection that many tasks
//! share: it is cheap to clone, pipelines concurrent commands over one TCP
//! connection, and reconnects by itself after a failure.

use std::time::Duration;

use redis::{
    Client, RedisResult,
    aio::{ConnectionManager, ConnectionManagerConfig},
};

/// Connects to Redis. Fails if Redis is unreachable right now; the caller
/// decides whether to retry.
pub async fn connect(url: &str) -> RedisResult<ConnectionManager> {
    let client = Client::open(url)?;
    let config = ConnectionManagerConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(2)))
        .set_response_timeout(Some(Duration::from_secs(2)));
    client.get_connection_manager_with_config(config).await
}

pub async fn ping(conn: &mut ConnectionManager) -> RedisResult<()> {
    redis::cmd("PING")
        .query_async::<String>(conn)
        .await
        .map(|_| ())
}
