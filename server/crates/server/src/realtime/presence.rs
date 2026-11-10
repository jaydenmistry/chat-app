//! Presence: online, away or offline, across every instance
//! (docs/design.md 6.6).
//!
//! Each connection is one field of the hash `presence:<user>`, worth
//! `active` or `idle`, with its own 75 s expiry (`HEXPIRE`, Redis 7.4+). Each
//! instance refreshes its connections' fields every 30 s. A user's status is
//! derived: any active connection means online, else any idle one means away,
//! else offline.
//!
//! One Lua script updates a field, recomputes the status, and compares it
//! with the last published one (`presence_status:<user>`). It runs
//! atomically, so when two instances change the same user at once, exactly
//! one of them sees the transition and publishes it.
//!
//! A connection that vanishes without saying goodbye (an instance crash)
//! simply expires. Nobody runs the script then, so a sweeper (one instance at
//! a time, by lock) re-checks every user marked present.

use std::{collections::HashSet, sync::LazyLock, time::Duration};

use axum::extract::ws::Utf8Bytes;
use protocol::frames::{PresenceState, PresenceStatus, ServerFrame};
use redis::{Script, aio::ConnectionManager};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::{
    realtime::{
        events::{self, Event},
        hub::{ConnId, encode},
    },
    state::AppState,
};

/// A connection's field expires this long after its last refresh.
const FIELD_TTL_SECS: u64 = 75;
const REFRESH_EVERY: Duration = Duration::from_secs(30);
const SWEEP_EVERY: Duration = Duration::from_secs(15);
const ONLINE_SET: &str = "presence_online";

/// KEYS: presence:<user>, presence_status:<user>, presence_online.
/// ARGV: operation (set | del | check), connection, state, field TTL, user.
/// Returns the new status if it changed, else false.
static UPDATE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
        local op = ARGV[1]
        if op == 'set' then
          redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])
          redis.call('HEXPIRE', KEYS[1], ARGV[4], 'FIELDS', 1, ARGV[2])
        elseif op == 'del' then
          redis.call('HDEL', KEYS[1], ARGV[2])
        end
        local status = 'offline'
        for _, state in ipairs(redis.call('HVALS', KEYS[1])) do
          if state == 'active' then
            status = 'online'
            break
          elseif state == 'idle' then
            status = 'away'
          end
        end
        local previous = redis.call('GET', KEYS[2]) or 'offline'
        if status == 'offline' then
          redis.call('DEL', KEYS[2])
          redis.call('SREM', KEYS[3], ARGV[5])
        else
          redis.call('SET', KEYS[2], status)
          redis.call('SADD', KEYS[3], ARGV[5])
        end
        if status ~= previous then
          return status
        end
        return false
        ",
    )
});

fn status_from_str(s: &str) -> PresenceStatus {
    match s {
        "online" => PresenceStatus::Online,
        "away" => PresenceStatus::Away,
        _ => PresenceStatus::Offline,
    }
}

enum Op {
    Set(PresenceState),
    Delete,
    Check,
}

async fn update(
    redis: &mut ConnectionManager,
    user_id: Uuid,
    conn_id: Option<ConnId>,
    op: Op,
) -> Option<PresenceStatus> {
    let (name, tab_state) = match op {
        Op::Set(PresenceState::Active) => ("set", "active"),
        Op::Set(PresenceState::Idle) => ("set", "idle"),
        Op::Delete => ("del", ""),
        Op::Check => ("check", ""),
    };
    let changed: redis::RedisResult<Option<String>> = storage::metered::redis(
        "presence.update",
        UPDATE
            .key(format!("presence:{user_id}"))
            .key(format!("presence_status:{user_id}"))
            .key(ONLINE_SET)
            .arg(name)
            .arg(conn_id.map(|c| c.to_string()).unwrap_or_default())
            .arg(tab_state)
            .arg(FIELD_TTL_SECS)
            .arg(user_id.to_string())
            .invoke_async(redis),
    )
    .await;
    match changed {
        Ok(changed) => changed.as_deref().map(status_from_str),
        // Presence is best effort: a failure here only delays a status.
        Err(err) => {
            tracing::debug!(error = %err, "presence update failed");
            None
        }
    }
}

async fn announce(redis: &mut ConnectionManager, user_id: Uuid, status: PresenceStatus) {
    let frame = ServerFrame::Presence { user_id, status };
    let event = Event::Presence {
        user_id,
        frame: serde_json::to_string(&frame).expect("server frames always serialize"),
    };
    events::publish_best_effort(redis, events::PRESENCE_CHANNEL, &event).await;
}

async fn apply(state: &AppState, user_id: Uuid, conn_id: Option<ConnId>, op: Op) {
    let mut redis = state.redis.clone();
    if let Some(status) = update(&mut redis, user_id, conn_id, op).await {
        announce(&mut redis, user_id, status).await;
    }
}

pub async fn connected(state: &AppState, user_id: Uuid, conn_id: ConnId) {
    apply(
        state,
        user_id,
        Some(conn_id),
        Op::Set(PresenceState::Active),
    )
    .await;
}

pub async fn disconnected(state: &AppState, user_id: Uuid, conn_id: ConnId) {
    apply(state, user_id, Some(conn_id), Op::Delete).await;
}

pub async fn set(state: &AppState, user_id: Uuid, conn_id: ConnId, tab_state: PresenceState) {
    apply(state, user_id, Some(conn_id), Op::Set(tab_state)).await;
}

/// Sends the current status of each watched user to one socket, so a client
/// that starts watching doesn't wait for the next transition.
pub async fn send_current(
    state: &AppState,
    outbound: &mpsc::Sender<Utf8Bytes>,
    users: &HashSet<Uuid>,
) {
    if users.is_empty() {
        return;
    }
    let users: Vec<Uuid> = users.iter().copied().collect();
    let mut pipe = redis::pipe();
    for user in &users {
        pipe.cmd("GET").arg(format!("presence_status:{user}"));
    }
    let statuses: Vec<Option<String>> = match storage::metered::redis(
        "presence.statuses",
        pipe.query_async(&mut state.redis.clone()),
    )
    .await
    {
        Ok(statuses) => statuses,
        Err(err) => {
            tracing::debug!(error = %err, "reading presence failed");
            return;
        }
    };
    for (user_id, status) in users.into_iter().zip(statuses) {
        let status = status
            .as_deref()
            .map_or(PresenceStatus::Offline, status_from_str);
        let _ = outbound.try_send(encode(&ServerFrame::Presence { user_id, status }));
    }
}

/// Refreshes this instance's fields, and (on whichever instance holds the
/// lock) sweeps users whose connections expired. Runs until shutdown.
pub async fn run_background(state: AppState, mut shutdown: watch::Receiver<bool>) {
    let mut refresh = tokio::time::interval(REFRESH_EVERY);
    let mut sweep = tokio::time::interval(SWEEP_EVERY);
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = refresh.tick() => {
                refresh_fields(&state).await;
                // The connection caps use the same per-field TTL scheme, so
                // their sockets are refreshed on the same tick.
                super::conn_cap::refresh(&state).await;
            }
            _ = sweep.tick() => sweep_expired(&state).await,
        }
    }
}

async fn refresh_fields(state: &AppState) {
    let connections = state.hub.connections();
    if connections.is_empty() {
        return;
    }
    let mut pipe = redis::pipe();
    for (user_id, conn_id) in connections {
        pipe.cmd("HEXPIRE")
            .arg(format!("presence:{user_id}"))
            .arg(FIELD_TTL_SECS)
            .arg("FIELDS")
            .arg(1)
            .arg(conn_id.to_string())
            .ignore();
    }
    if let Err(err) = storage::metered::redis(
        "presence.refresh",
        pipe.query_async::<()>(&mut state.redis.clone()),
    )
    .await
    {
        tracing::debug!(error = %err, "refreshing presence failed");
    }
}

async fn sweep_expired(state: &AppState) {
    let mut redis = state.redis.clone();
    // One sweeper at a time; the lock expires before the next tick.
    let lock: redis::RedisResult<Option<String>> = storage::metered::redis(
        "job.lock",
        redis::cmd("SET")
            .arg("lock:job:presence-sweep")
            .arg(state.instance_id.to_string())
            .arg("NX")
            .arg("PX")
            .arg(SWEEP_EVERY.as_millis() as u64 - 1_000)
            .query_async(&mut redis),
    )
    .await;
    if !matches!(lock, Ok(Some(_))) {
        return;
    }
    let users: Vec<String> = match storage::metered::redis(
        "presence.online_members",
        redis::cmd("SMEMBERS")
            .arg(ONLINE_SET)
            .query_async(&mut redis),
    )
    .await
    {
        Ok(users) => users,
        Err(_) => return,
    };
    for user in users.iter().filter_map(|u| u.parse::<Uuid>().ok()) {
        if let Some(status) = update(&mut redis, user, None, Op::Check).await {
            announce(&mut redis, user, status).await;
        }
    }
}
