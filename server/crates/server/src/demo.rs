//! Demo-room reset (docs/design.md 11.4).
//!
//! The seeded public rooms (owned by the system user) are wiped and re-seeded
//! once a day, at 04:00 UTC. Nothing is deleted: the reset raises each room's
//! `history_floor` above the previous day's messages and sets
//! `message_expiry_at` to the next reset, so the day's messages expire natively
//! in Scylla and fall below the floor for history and search. Then it writes
//! the seed messages and publishes `room.reset` so open clients reload.
//!
//! A room is "due" when its day has ended (`message_expiry_at` is null or
//! past), so the job is a poll, not a cron: whichever instance holds the lock
//! resets every due room. A fresh deploy's rooms have a null expiry, so they
//! are seeded on the first pass.

use std::time::Duration;

use protocol::frames::ServerFrame;
use storage::{messages::NewMessage, rooms::DueDemoRoom, users::SYSTEM_USER_ID};
use time::{Duration as TimeDuration, OffsetDateTime, Time};
use tokio::sync::watch;

use crate::{
    realtime::events::{self, Event},
    search,
    state::AppState,
};

/// The daily reset happens at this UTC time.
const RESET_HOUR: u8 = 4;

/// Runs a reset pass on `interval` until shutdown.
pub async fn run_background(
    state: AppState,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Drop the immediate first tick so instances don't all grab the lock the
    // moment they start; the first reset is one interval after boot.
    tick.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tick.tick() => {
                if holds_lock(&state, interval).await {
                    reset_due(&state).await;
                }
            }
        }
    }
}

/// Takes the shared lock, so only one instance resets per tick. The lock
/// expires before the next tick (like the guest-cleanup lock).
async fn holds_lock(state: &AppState, interval: Duration) -> bool {
    let ttl_ms = (interval.as_millis() as u64).saturating_sub(500).max(1);
    let lock: redis::RedisResult<Option<String>> = redis::cmd("SET")
        .arg("lock:job:demo-reset")
        .arg(state.instance_id.to_string())
        .arg("NX")
        .arg("PX")
        .arg(ttl_ms)
        .query_async(&mut state.redis.clone())
        .await;
    matches!(lock, Ok(Some(_)))
}

/// Resets every demo room whose day has ended. Public so a test can run one
/// pass without waiting for the loop.
pub async fn reset_due(state: &AppState) -> usize {
    let due = match storage::rooms::due_demo_rooms(&state.pg).await {
        Ok(due) => due,
        Err(err) => {
            tracing::warn!(error = %err, "listing due demo rooms failed");
            return 0;
        }
    };
    let mut done = 0;
    for room in &due {
        if reset_room(state, room).await {
            done += 1;
        }
    }
    if done > 0 {
        tracing::info!(count = done, "reset demo rooms");
    }
    done
}

/// Resets one room: seed messages first, then the floor and expiry, then the
/// announcement. If it fails partway the room stays due and the next pass
/// redoes it; orphaned seeds from the failed attempt fall below the new floor.
async fn reset_room(state: &AppState, room: &DueDemoRoom) -> bool {
    let now = OffsetDateTime::now_utc();
    let next = next_reset_after(now);
    let ttl = match Duration::try_from(next - now) {
        Ok(ttl) if ttl >= Duration::from_secs(1) => ttl,
        // next_reset_after always returns a future time, so this can't happen.
        _ => return false,
    };

    // A boundary ID for "now": every seed message below gets a later ID, so
    // the floor hides the previous day and keeps the seeds.
    let floor = match state.ids.next_id() {
        Ok(id) => id,
        Err(err) => {
            tracing::warn!(error = %err, "demo reset can't issue IDs");
            return false;
        }
    };
    for content in seed_messages(&room.name) {
        let id = match state.ids.next_id() {
            Ok(id) => id,
            Err(err) => {
                tracing::warn!(error = %err, "demo reset can't issue IDs");
                return false;
            }
        };
        let new = NewMessage {
            room_id: room.id,
            id,
            author_id: SYSTEM_USER_ID,
            content,
            ttl: Some(ttl),
        };
        if let Err(err) = state.messages.insert(&new).await {
            tracing::warn!(error = %err, room = %room.id, "writing a seed message failed");
            return false;
        }
        search::notify(state, room.id, id);
    }

    if let Err(err) =
        storage::rooms::apply_demo_reset(&state.pg, room.id, floor.as_i64(), next).await
    {
        tracing::warn!(error = %err, room = %room.id, "applying a demo reset failed");
        return false;
    }

    let frame = ServerFrame::RoomReset { room_id: room.id };
    events::publish_best_effort(
        &mut state.redis.clone(),
        &events::room_channel(room.id),
        &Event::deliver(&frame),
    )
    .await;
    true
}

/// The next 04:00 UTC strictly after `now`.
fn next_reset_after(now: OffsetDateTime) -> OffsetDateTime {
    let reset = Time::from_hms(RESET_HOUR, 0, 0).expect("04:00 is a valid time");
    let today = now.replace_time(reset);
    if now < today {
        today
    } else {
        today + TimeDuration::days(1)
    }
}

/// The messages seeded into a room on each reset. A small, fixed set so the
/// demo always has something to show; the room name picks the flavour.
fn seed_messages(room_name: &str) -> &'static [&'static str] {
    match room_name {
        "general" => &[
            "Welcome to the general room. This is a live demo of the chat app.",
            "Everything here resets every day at 04:00 UTC, so feel free to try things out.",
            "Say hi, start a thread, or open a DM with someone.",
        ],
        "rust" => &[
            "This room is for talking about Rust.",
            "The server behind this chat is written in Rust: axum, tokio, and a workspace of crates.",
            "Messages live in ScyllaDB, rooms and accounts in Postgres, and Redis handles fan-out.",
        ],
        "random" => &[
            "Anything goes in here.",
            "Pineapple on pizza: yes or no?",
            "The demo wipes this room daily, so no pressure.",
        ],
        _ => &["Welcome! This room resets every day at 04:00 UTC."],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> OffsetDateTime {
        OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).unwrap()
    }

    #[test]
    fn next_reset_is_the_coming_0400_utc() {
        // Before 04:00: later the same day.
        assert_eq!(
            next_reset_after(at("2026-11-09T01:30:00Z")),
            at("2026-11-09T04:00:00Z")
        );
        // After 04:00: the next day.
        assert_eq!(
            next_reset_after(at("2026-11-09T09:00:00Z")),
            at("2026-11-10T04:00:00Z")
        );
        // Exactly 04:00 counts as the day having started: the next one.
        assert_eq!(
            next_reset_after(at("2026-11-09T04:00:00Z")),
            at("2026-11-10T04:00:00Z")
        );
    }

    #[test]
    fn every_demo_room_has_seed_messages() {
        for name in ["general", "rust", "random", "unknown"] {
            assert!(!seed_messages(name).is_empty(), "{name}");
        }
    }
}
