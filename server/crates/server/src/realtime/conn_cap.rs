//! Global caps on open WebSocket connections: at most N per user and M per
//! IP, counted across every instance (docs/design.md 6.3, 11.2).
//!
//! Each live socket is one field of a Redis hash: `conns:u:<user>` and
//! `conns:ip:<ip>`. A field is set at connect and given its own TTL
//! (`HEXPIRE`, Redis 7.4+), exactly like a presence field. Each instance
//! refreshes its sockets' fields every 30 s; a socket whose instance crashes
//! simply expires, so a leaked count heals itself within one TTL. The count
//! is `HLEN`, so it is shared by all instances and needs no central tally.
//!
//! Admission is one Lua script: it reads both counts and, only if both are
//! under their cap, adds the field. Running atomically means two instances
//! admitting the same user at once can't both slip past the last slot.

use std::{net::IpAddr, sync::LazyLock};

use redis::Script;
use uuid::Uuid;

use crate::state::AppState;

/// A socket's fields expire this long after their last refresh. Matches
/// presence, and comfortably longer than its 30 s refresh interval (this
/// cap is refreshed on the same tick).
const FIELD_TTL_SECS: u64 = 75;

fn user_key(user_id: Uuid) -> String {
    format!("conns:u:{user_id}")
}

fn ip_key(ip: IpAddr) -> String {
    format!("conns:ip:{ip}")
}

/// KEYS: conns:u:<user>, conns:ip:<ip>.
/// ARGV: conn_id, max per user, max per IP, field TTL (seconds).
/// Returns 'ok' if admitted, else 'user' or 'ip' naming the cap hit.
static ADMIT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
        if redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[2]) then return 'user' end
        if redis.call('HLEN', KEYS[2]) >= tonumber(ARGV[3]) then return 'ip' end
        redis.call('HSET', KEYS[1], ARGV[1], '1')
        redis.call('HEXPIRE', KEYS[1], ARGV[4], 'FIELDS', 1, ARGV[1])
        redis.call('HSET', KEYS[2], ARGV[1], '1')
        redis.call('HEXPIRE', KEYS[2], ARGV[4], 'FIELDS', 1, ARGV[1])
        return 'ok'
        ",
    )
});

/// Tries to take a connection slot for `user_id` and `ip`. Returns the cap
/// that was hit (`"user"` or `"ip"`) if the connection must be refused, or
/// `None` if it is admitted. Fails open: a Redis error admits the connection
/// (the upgrade also needs pub/sub, which is checked right after and refuses
/// when Redis is truly down).
pub async fn admit(
    state: &AppState,
    conn_id: Uuid,
    user_id: Uuid,
    ip: IpAddr,
) -> Option<&'static str> {
    let result: redis::RedisResult<String> = ADMIT
        .key(user_key(user_id))
        .key(ip_key(ip))
        .arg(conn_id.to_string())
        .arg(state.config.max_sockets_per_user)
        .arg(state.config.max_sockets_per_ip)
        .arg(FIELD_TTL_SECS)
        .invoke_async(&mut state.redis.clone())
        .await;
    match result.as_deref() {
        Ok("user") => Some("user"),
        Ok("ip") => Some("ip"),
        Ok(_) => None,
        Err(err) => {
            tracing::warn!(error = %err, "connection cap check failed; admitting");
            None
        }
    }
}

/// Drops a connection's slot when it closes.
pub async fn release(state: &AppState, conn_id: Uuid, user_id: Uuid, ip: IpAddr) {
    let mut pipe = redis::pipe();
    pipe.cmd("HDEL")
        .arg(user_key(user_id))
        .arg(conn_id.to_string())
        .ignore();
    pipe.cmd("HDEL")
        .arg(ip_key(ip))
        .arg(conn_id.to_string())
        .ignore();
    if let Err(err) = pipe.query_async::<()>(&mut state.redis.clone()).await {
        // The fields expire on their own if this fails.
        tracing::debug!(error = %err, "releasing connection slot failed");
    }
}

/// Pushes every local socket's TTL out again, so live sockets stay counted
/// while a crashed instance's sockets expire. Called from the presence
/// refresh loop.
pub async fn refresh(state: &AppState) {
    let connections = state.hub.connections_with_ip();
    if connections.is_empty() {
        return;
    }
    let mut pipe = redis::pipe();
    for (user_id, ip, conn_id) in connections {
        let conn = conn_id.to_string();
        pipe.cmd("HEXPIRE")
            .arg(user_key(user_id))
            .arg(FIELD_TTL_SECS)
            .arg("FIELDS")
            .arg(1)
            .arg(&conn)
            .ignore();
        pipe.cmd("HEXPIRE")
            .arg(ip_key(ip))
            .arg(FIELD_TTL_SECS)
            .arg("FIELDS")
            .arg(1)
            .arg(&conn)
            .ignore();
    }
    if let Err(err) = pipe.query_async::<()>(&mut state.redis.clone()).await {
        tracing::debug!(error = %err, "refreshing connection slots failed");
    }
}
