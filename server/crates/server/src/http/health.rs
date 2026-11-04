//! `/healthz`: the process is up. `/readyz`: it can serve traffic.

use std::{future::Future, sync::atomic::Ordering, time::Duration};

use axum::{Json, extract::State, http::StatusCode};
use serde::Serialize;

use crate::state::AppState;

const CHECK_TIMEOUT: Duration = Duration::from_secs(1);

pub async fn healthz() -> &'static str {
    "ok"
}

#[derive(Serialize)]
pub struct Readiness {
    postgres: bool,
    redis: bool,
    scylla: bool,
    shutting_down: bool,
}

/// 200 only if all three stores answer within a second each and the server
/// is not shutting down. The checks run concurrently. Failures are logged
/// here; the response says only which store is down.
pub async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<Readiness>) {
    let mut redis = state.redis.clone();
    let (postgres, redis, scylla) = tokio::join!(
        check("postgres", storage::postgres::ping(&state.pg)),
        check("redis", storage::redisdb::ping(&mut redis)),
        check("scylla", storage::scylladb::ping(&state.scylla)),
    );
    let shutting_down = state.shutting_down.load(Ordering::Relaxed);
    let ready = postgres && redis && scylla && !shutting_down;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(Readiness {
            postgres,
            redis,
            scylla,
            shutting_down,
        }),
    )
}

async fn check<E: std::fmt::Display>(
    name: &str,
    ping: impl Future<Output = Result<(), E>>,
) -> bool {
    match tokio::time::timeout(CHECK_TIMEOUT, ping).await {
        Ok(Ok(())) => true,
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "readiness: {name} check failed");
            false
        }
        Err(_) => {
            tracing::warn!("readiness: {name} check timed out");
            false
        }
    }
}
