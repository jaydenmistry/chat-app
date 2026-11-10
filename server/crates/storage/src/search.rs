//! The full-text search index (Postgres), fed asynchronously from the
//! `search:events` Redis stream (docs/design.md 10).
//!
//! The index answers "which messages match?". What a message says, and
//! whether it still exists, always comes from Scylla.

use protocol::ids::MessageId;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

/// The text search configuration: English stemming ("running" matches
/// "run").
const CONFIG: &str = "english";

pub struct IndexEntry<'a> {
    pub message_id: MessageId,
    pub room_id: Uuid,
    pub author_id: Uuid,
    /// Empty when deleted.
    pub content: &'a str,
    pub deleted: bool,
    pub version: i64,
    pub expires_at: Option<OffsetDateTime>,
}

/// Inserts or updates a message's entry, but only with a newer version than
/// the one indexed: index events can arrive late, twice, or out of order.
/// A delete is kept as a row with `deleted = true`, so a late, older event
/// can't bring the message back into results.
pub async fn upsert(pool: &PgPool, entry: &IndexEntry<'_>) -> Result<(), sqlx::Error> {
    crate::metered::pg("search.upsert", sqlx::query!(
        r#"
        INSERT INTO message_search (message_id, room_id, author_id, tsv, deleted, version, expires_at)
        VALUES ($1, $2, $3, to_tsvector($4::regconfig, $5), $6, $7, $8)
        ON CONFLICT (message_id) DO UPDATE
        SET tsv = EXCLUDED.tsv, deleted = EXCLUDED.deleted, version = EXCLUDED.version,
            expires_at = EXCLUDED.expires_at
        WHERE message_search.version < EXCLUDED.version
        "#,
        entry.message_id.as_i64(),
        entry.room_id,
        entry.author_id,
        CONFIG as &str,
        if entry.deleted { "" } else { entry.content },
        entry.deleted,
        entry.version,
        entry.expires_at,
    )
    .execute(pool))
    .await
    .map(|_| ())
}

/// Matching messages in rooms `user_id` belongs to, newest first. The
/// membership join is in the same query, against the same database as the
/// memberships themselves, so it is always current: leaving a room removes
/// it from results immediately.
pub async fn search(
    pool: &PgPool,
    user_id: Uuid,
    query: &str,
    room_id: Option<Uuid>,
    before: Option<MessageId>,
    limit: i64,
) -> Result<Vec<(MessageId, Uuid)>, sqlx::Error> {
    let rows = crate::metered::pg(
        "search.search",
        sqlx::query!(
            r#"
        SELECT s.message_id, s.room_id
        FROM message_search s
        JOIN memberships m ON m.room_id = s.room_id AND m.user_id = $1
        JOIN rooms r ON r.id = s.room_id
        WHERE s.tsv @@ websearch_to_tsquery($2::regconfig, $3)
          AND NOT s.deleted
          AND (s.expires_at IS NULL OR s.expires_at > now())
          AND s.message_id >= r.history_floor
          AND ($4::uuid IS NULL OR s.room_id = $4)
          AND ($5::bigint IS NULL OR s.message_id < $5)
        ORDER BY s.message_id DESC
        LIMIT $6
        "#,
            user_id,
            CONFIG as &str,
            query,
            room_id,
            before.map(MessageId::as_i64),
            limit,
        )
        .fetch_all(pool),
    )
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| Some((MessageId::from_i64(r.message_id)?, r.room_id)))
        .collect())
}
