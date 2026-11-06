//! Guest cleanup (docs/design.md 11.3).
//!
//! Postgres can't expire rows on its own, so a job deletes expired guests and
//! their DM rooms (memberships and sessions follow by cascade). It collects
//! each guest's rooms first and publishes `user.expired` to them, so open
//! clients hide the guest's messages at once; the stored messages expire
//! natively in Scylla and by session TTL in Redis.
//!
//! One instance runs it at a time, chosen by a short Redis lock, like the
//! presence sweep.

use std::time::Duration;

use protocol::frames::ServerFrame;
use tokio::sync::watch;

use crate::{
    realtime::events::{self, Event},
    state::AppState,
};

/// At most this many guests per pass. A minute's expiries are far fewer; the
/// bound just keeps one pass bounded if a backlog ever builds up.
const BATCH: i64 = 500;

/// Runs a cleanup pass on `interval` until shutdown.
pub async fn run_background(
    state: AppState,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(interval);
    // The first tick is immediate; drop it so the first sweep (and the lock it
    // takes, which lasts almost a full interval) waits one interval. Without
    // this, every instance would grab the lock the moment it starts.
    tick.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tick.tick() => {
                if holds_lock(&state, interval).await {
                    sweep(&state).await;
                }
            }
        }
    }
}

/// Takes the shared lock, so only one instance sweeps per tick. The lock
/// expires before the next tick, so the next pass is free to run anywhere.
async fn holds_lock(state: &AppState, interval: Duration) -> bool {
    // Release a touch before the next tick; never below 1 ms (short test
    // intervals).
    let ttl_ms = (interval.as_millis() as u64).saturating_sub(500).max(1);
    let lock: redis::RedisResult<Option<String>> = redis::cmd("SET")
        .arg("lock:job:guest-cleanup")
        .arg(state.instance_id.to_string())
        .arg("NX")
        .arg("PX")
        .arg(ttl_ms)
        .query_async(&mut state.redis.clone())
        .await;
    matches!(lock, Ok(Some(_)))
}

/// Deletes expired guests and announces them. Returns how many were deleted.
/// Public so tests can run one pass without waiting for the loop.
pub async fn sweep(state: &AppState) -> usize {
    let expired = match storage::users::delete_expired_guests(&state.pg, BATCH).await {
        Ok(expired) => expired,
        Err(err) => {
            tracing::warn!(error = %err, "deleting expired guests failed");
            return 0;
        }
    };
    let mut redis = state.redis.clone();
    for guest in &expired {
        for &room_id in &guest.rooms {
            let frame = ServerFrame::UserExpired {
                room_id,
                user_id: guest.user_id,
            };
            events::publish_best_effort(
                &mut redis,
                &events::room_channel(room_id),
                &Event::deliver(&frame),
            )
            .await;
        }
    }
    if !expired.is_empty() {
        tracing::info!(count = expired.len(), "deleted expired guests");
    }
    expired.len()
}
