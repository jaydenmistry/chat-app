//! Sessions: the durable record in Postgres and the live copy in Redis.
//!
//! Every authenticated request reads only Redis (`sess:<hash>`). Postgres
//! holds the metadata used to list and revoke sessions. Both are keyed by the
//! SHA-256 of the cookie token, so neither store holds a usable token.

use std::{collections::HashMap, fmt::Write as _, net::IpAddr, sync::LazyLock};

use redis::{RedisResult, Script, aio::ConnectionManager};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

/// SHA-256 of a session token.
pub type TokenHash = [u8; 32];

pub struct NewSession<'a> {
    pub user_id: Uuid,
    pub token_hash: &'a TokenHash,
    pub expires_at: OffsetDateTime,
    pub user_agent: Option<&'a str>,
    pub ip: Option<IpAddr>,
}

/// Records a new session and returns its ID.
pub async fn insert(pool: &PgPool, new: &NewSession<'_>) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query!(
        r#"
        INSERT INTO sessions (id, user_id, token_hash, expires_at, user_agent, ip)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
        id,
        new.user_id,
        &new.token_hash[..],
        new.expires_at,
        new.user_agent,
        new.ip as Option<IpAddr>,
    )
    .execute(pool)
    .await?;
    Ok(id)
}

pub struct SessionRow {
    pub id: Uuid,
    pub created_at: OffsetDateTime,
    pub last_seen_at: OffsetDateTime,
    pub user_agent: Option<String>,
    pub ip: Option<IpAddr>,
}

/// The user's sessions that are neither revoked nor expired, most recently
/// used first.
pub async fn list_active(pool: &PgPool, user_id: Uuid) -> Result<Vec<SessionRow>, sqlx::Error> {
    sqlx::query_as!(
        SessionRow,
        r#"
        SELECT id, created_at, last_seen_at, user_agent, ip AS "ip: IpAddr"
        FROM sessions
        WHERE user_id = $1 AND revoked_at IS NULL AND expires_at > now()
        ORDER BY last_seen_at DESC
        "#,
        user_id,
    )
    .fetch_all(pool)
    .await
}

/// Marks a session revoked and returns its token hash, so the caller can
/// delete the live copy. `None` if the session doesn't exist, belongs to
/// another user, or was already revoked.
pub async fn revoke(
    pool: &PgPool,
    user_id: Uuid,
    session_id: Uuid,
) -> Result<Option<TokenHash>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        UPDATE sessions SET revoked_at = now()
        WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL
        RETURNING token_hash
        "#,
        session_id,
        user_id,
    )
    .fetch_optional(pool)
    .await?;
    // The CHECK constraint guarantees 32 bytes.
    Ok(row.and_then(|r| r.token_hash.try_into().ok()))
}

/// A session revoked in bulk: its ID (to announce) and token hash (to delete
/// the live copy).
pub struct RevokedSession {
    pub id: Uuid,
    pub token_hash: TokenHash,
}

/// Revokes every active session of a user and returns them, so the caller can
/// delete the live copies and close the sockets. Used when banning an account.
pub async fn revoke_all(pool: &PgPool, user_id: Uuid) -> Result<Vec<RevokedSession>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        UPDATE sessions SET revoked_at = now()
        WHERE user_id = $1 AND revoked_at IS NULL
        RETURNING id, token_hash
        "#,
        user_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        // The CHECK constraint guarantees 32 bytes.
        .filter_map(|r| {
            r.token_hash
                .try_into()
                .ok()
                .map(|token_hash| RevokedSession {
                    id: r.id,
                    token_hash,
                })
        })
        .collect())
}

/// Records that the session was used, and its new expiry for sliding sessions.
pub async fn touch(
    pool: &PgPool,
    session_id: Uuid,
    expires_at: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "UPDATE sessions SET last_seen_at = now(), expires_at = $2 WHERE id = $1",
        session_id,
        expires_at,
    )
    .execute(pool)
    .await
    .map(|_| ())
}

/// The live copy of a session, in Redis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSession {
    pub session_id: Uuid,
    pub user_id: Uuid,
    /// Unix seconds. Redis expires the key at this time.
    pub expires_at: i64,
    /// Sliding sessions are extended on use; guest sessions are not.
    pub sliding: bool,
    /// Unix seconds of the last extension, to extend at most once an hour.
    pub refreshed_at: i64,
}

fn live_key(hash: &TokenHash) -> String {
    let mut key = String::with_capacity(5 + 64);
    key.push_str("sess:");
    for byte in hash {
        write!(key, "{byte:02x}").expect("writing to a String cannot fail");
    }
    key
}

fn ttl_until(expires_at: i64) -> i64 {
    (expires_at - OffsetDateTime::now_utc().unix_timestamp()).max(1)
}

pub async fn store_live(
    conn: &mut ConnectionManager,
    hash: &TokenHash,
    live: &LiveSession,
) -> RedisResult<()> {
    let key = live_key(hash);
    // MULTI/EXEC, so the hash never exists without its TTL.
    redis::pipe()
        .atomic()
        .cmd("HSET")
        .arg(&key)
        .arg("session_id")
        .arg(live.session_id.to_string())
        .arg("user_id")
        .arg(live.user_id.to_string())
        .arg("expires_at")
        .arg(live.expires_at)
        .arg("sliding")
        .arg(u8::from(live.sliding))
        .arg("refreshed_at")
        .arg(live.refreshed_at)
        .ignore()
        .cmd("EXPIRE")
        .arg(&key)
        .arg(ttl_until(live.expires_at))
        .ignore()
        .query_async(conn)
        .await
}

pub async fn load_live(
    conn: &mut ConnectionManager,
    hash: &TokenHash,
) -> RedisResult<Option<LiveSession>> {
    let fields: HashMap<String, String> = redis::cmd("HGETALL")
        .arg(live_key(hash))
        .query_async(conn)
        .await?;
    if fields.is_empty() {
        return Ok(None);
    }
    let parsed = (|| {
        Some(LiveSession {
            session_id: fields.get("session_id")?.parse().ok()?,
            user_id: fields.get("user_id")?.parse().ok()?,
            expires_at: fields.get("expires_at")?.parse().ok()?,
            sliding: fields.get("sliding")? == "1",
            refreshed_at: fields.get("refreshed_at")?.parse().ok()?,
        })
    })();
    if parsed.is_none() {
        // Only this module writes these keys, so this means a bug or manual
        // tampering. Treat it as signed out rather than failing every request.
        tracing::warn!("ignoring malformed session record in redis");
    }
    Ok(parsed)
}

/// Extends a sliding session, but only if it still exists. Checking and
/// writing in one Lua script means a concurrent logout can't be undone by an
/// extension that recreates the deleted key.
static EXTEND_IF_PRESENT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
        if redis.call('EXISTS', KEYS[1]) == 0 then
          return 0
        end
        redis.call('HSET', KEYS[1], 'expires_at', ARGV[1], 'refreshed_at', ARGV[2])
        redis.call('EXPIRE', KEYS[1], ARGV[3])
        return 1
        ",
    )
});

/// Returns false if the session no longer exists.
pub async fn extend_live(
    conn: &mut ConnectionManager,
    hash: &TokenHash,
    expires_at: i64,
    refreshed_at: i64,
) -> RedisResult<bool> {
    let extended: i64 = EXTEND_IF_PRESENT
        .key(live_key(hash))
        .arg(expires_at)
        .arg(refreshed_at)
        .arg(ttl_until(expires_at))
        .invoke_async(conn)
        .await?;
    Ok(extended == 1)
}

/// Which of these sessions still exist, in one pipelined round trip. Used to
/// close sockets whose session ended while a revocation event was missed.
pub async fn live_exists_many(
    conn: &mut ConnectionManager,
    hashes: &[TokenHash],
) -> RedisResult<Vec<bool>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    let mut pipe = redis::pipe();
    for hash in hashes {
        pipe.cmd("EXISTS").arg(live_key(hash));
    }
    pipe.query_async(conn).await
}

pub async fn delete_live(conn: &mut ConnectionManager, hash: &TokenHash) -> RedisResult<()> {
    redis::cmd("DEL")
        .arg(live_key(hash))
        .query_async(conn)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_key_is_lowercase_hex() {
        let mut hash = [0u8; 32];
        hash[0] = 0xab;
        hash[31] = 0x01;
        let key = live_key(&hash);
        assert_eq!(key.len(), 5 + 64);
        assert!(key.starts_with("sess:ab00"));
        assert!(key.ends_with("0001"));
    }
}
