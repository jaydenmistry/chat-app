//! Feeding the search index (docs/design.md 10).
//!
//! The `search:events` stream carries notifications, not content: an entry
//! is `{room_id, message_id}`. New messages are added inside the send
//! script, before the Scylla write, so a crash between the two can't lose
//! an entry. Edits and deletes are added after their write.
//!
//! Every instance runs an indexer in one consumer group. An entry is handled
//! once it has been pending for `INDEX_DELAY`, which is longer than the send
//! path's write timeout: `XREADGROUP` takes new entries, and `XAUTOCLAIM`
//! with that minimum idle time hands them out again, both this instance's
//! and those of an instance that died. Handling means reading the message's
//! current state from Scylla and upserting it with a version guard. An
//! entry whose message is not in Scylla stays pending and is retried; after
//! `GIVE_UP_AFTER` it's dropped, because that write never happened.

use std::{collections::HashMap, time::Duration};

use protocol::ids::MessageId;
use redis::{
    AsyncCommands,
    streams::{
        StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamInfoConsumersReply,
        StreamReadOptions, StreamReadReply,
    },
};
use storage::search::IndexEntry;
use time::OffsetDateTime;
use tokio::sync::watch;
use uuid::Uuid;

use crate::state::AppState;

pub const STREAM: &str = "search:events";
const GROUP: &str = "indexers";
/// Entries are handled once they're this old: more than the 2 s write
/// timeout, so a message that will be written has been.
pub const INDEX_DELAY: Duration = Duration::from_secs(3);
/// A message still missing from Scylla this long after its entry was never
/// written; its entry is dropped. A client retry adds a fresh one.
const GIVE_UP_AFTER: Duration = Duration::from_secs(30);
const POLL_EVERY: Duration = Duration::from_secs(1);
const BATCH: usize = 100;
/// Approximate cap on the stream's length, so a stopped indexer can't grow
/// it without bound. Entries trimmed before they're indexed are recovered
/// by a reindex.
pub const MAX_STREAM_LEN: usize = 100_000;
/// Consumers left behind by stopped instances are removed once idle this
/// long with nothing pending.
const STALE_CONSUMER: Duration = Duration::from_secs(3600);

/// Adds an entry for a changed message (edit, delete), retrying in the
/// background. The change is already stored; if every attempt fails, the
/// index stays stale for that message, which hydration makes harmless for
/// deletes (docs/design.md 10).
pub fn notify(state: &AppState, room_id: Uuid, message_id: MessageId) {
    let mut redis = state.redis.clone();
    tokio::spawn(async move {
        let mut delay = Duration::from_millis(200);
        for attempt in 1..=5 {
            let added = enqueue(&mut redis, room_id, message_id).await;
            match added {
                Ok(_) => return,
                Err(err) if attempt == 5 => {
                    tracing::warn!(error = %err, %message_id, "giving up on a search index event");
                }
                Err(_) => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
            }
        }
    });
}

/// Adds one index entry (room and message IDs only; the indexer reads the
/// content from Scylla). One attempt, for callers that handle retries.
pub async fn enqueue(
    redis: &mut redis::aio::ConnectionManager,
    room_id: Uuid,
    message_id: MessageId,
) -> redis::RedisResult<String> {
    storage::metered::redis(
        "search.enqueue",
        redis::cmd("XADD")
            .arg(STREAM)
            .arg("MAXLEN")
            .arg("~")
            .arg(MAX_STREAM_LEN)
            .arg("*")
            .arg("room_id")
            .arg(room_id.to_string())
            .arg("message_id")
            .arg(message_id.to_string())
            .query_async(redis),
    )
    .await
}

pub async fn run_indexer(state: AppState, mut shutdown: watch::Receiver<bool>) {
    let consumer = state.instance_id.to_string();
    let mut tick = tokio::time::interval(POLL_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut group_ready = false;
    let mut cleaned = false;
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tick.tick() => {}
        }
        let mut redis = state.redis.clone();
        if !group_ready {
            group_ready = create_group(&mut redis).await;
            if !group_ready {
                continue;
            }
        }
        if !cleaned {
            cleaned = remove_stale_consumers(&mut redis, &consumer).await;
        }
        if let Err(err) = index_once(&state, &mut redis, &consumer).await {
            // NOGROUP: the stream was deleted (a Redis flush); recreate.
            if err.code() == Some("NOGROUP") {
                group_ready = false;
            }
            tracing::debug!(error = %err, "indexing round failed");
        }
    }
}

async fn create_group(redis: &mut redis::aio::ConnectionManager) -> bool {
    // From the start of the stream: entries added before the first indexer
    // ever ran are indexed too.
    let created: redis::RedisResult<()> = redis.xgroup_create_mkstream(STREAM, GROUP, "0").await;
    match created {
        Ok(()) => true,
        Err(err) if err.code() == Some("BUSYGROUP") => true,
        Err(err) => {
            tracing::warn!(error = %err, "creating the indexer group failed");
            false
        }
    }
}

async fn remove_stale_consumers(redis: &mut redis::aio::ConnectionManager, me: &str) -> bool {
    let Ok(info) = redis
        .xinfo_consumers::<_, _, StreamInfoConsumersReply>(STREAM, GROUP)
        .await
    else {
        return false;
    };
    for c in info.consumers {
        let stale = c.pending == 0 && c.idle as u128 > STALE_CONSUMER.as_millis();
        if stale && c.name != me {
            let _: redis::RedisResult<usize> =
                redis.xgroup_delconsumer(STREAM, GROUP, &c.name).await;
        }
    }
    true
}

/// One round: take new entries, then handle every entry that has waited
/// long enough.
async fn index_once(
    state: &AppState,
    redis: &mut redis::aio::ConnectionManager,
    consumer: &str,
) -> redis::RedisResult<()> {
    // New entries become pending for this consumer; they're handled below
    // once they've been idle for INDEX_DELAY.
    let _: StreamReadReply = redis
        .xread_options(
            &[STREAM],
            &[">"],
            &StreamReadOptions::default()
                .group(GROUP, consumer)
                .count(BATCH * 10),
        )
        .await?;

    let mut start = "0-0".to_string();
    // A bounded number of pages per round keeps a backlog from starving
    // shutdown; the next round continues.
    for _ in 0..10 {
        let reply: StreamAutoClaimReply = redis
            .xautoclaim_options(
                STREAM,
                GROUP,
                consumer,
                INDEX_DELAY.as_millis() as u64,
                &start,
                StreamAutoClaimOptions::default().count(BATCH),
            )
            .await?;
        let mut done = Vec::new();
        for entry in &reply.claimed {
            if handle(state, entry).await {
                done.push(entry.id.clone());
            }
        }
        if !done.is_empty() {
            let _: usize = redis.xack(STREAM, GROUP, &done).await?;
        }
        if reply.next_stream_id == "0-0" {
            break;
        }
        start = reply.next_stream_id;
    }
    Ok(())
}

/// Indexes one entry. True when it's finished with (indexed, or given up
/// on); false to leave it pending for a retry.
async fn handle(state: &AppState, entry: &StreamId) -> bool {
    let field = |name: &str| entry.get::<String>(name);
    let (Some(room_id), Some(message_id)) = (
        field("room_id").and_then(|v| v.parse::<Uuid>().ok()),
        field("message_id").and_then(|v| v.parse::<MessageId>().ok()),
    ) else {
        tracing::warn!(id = entry.id, "malformed search index event");
        return true;
    };
    let loaded = match state.messages.get_with_ttl(room_id, message_id).await {
        Ok(loaded) => loaded,
        Err(err) => {
            tracing::debug!(error = %err, "search indexer can't read scylla");
            return false;
        }
    };
    let Some((message, ttl)) = loaded else {
        return entry_age(&entry.id, state.ids.unix_ms()) > GIVE_UP_AFTER;
    };
    let expires_at = ttl.map(|ttl| OffsetDateTime::now_utc() + ttl);
    let indexed = storage::search::upsert(
        &state.pg,
        &IndexEntry {
            message_id,
            room_id,
            author_id: message.author_id,
            content: &message.content,
            deleted: message.deleted,
            version: message.version,
            expires_at,
        },
    )
    .await;
    match indexed {
        Ok(()) => {
            // How far behind the indexer is: the age of the entry it just
            // handled (docs/design.md 13).
            let lag = entry_age(&entry.id, state.ids.unix_ms());
            crate::observe::index_lag(lag.as_secs_f64());
            true
        }
        Err(err) => {
            tracing::debug!(error = %err, "search indexer can't write postgres");
            false
        }
    }
}

/// How old a stream entry is, from its ID (`<unix ms>-<seq>`, Redis's
/// clock). Unparseable IDs count as old.
fn entry_age(id: &str, now_unix_ms: u64) -> Duration {
    let ms = id
        .split('-')
        .next()
        .and_then(|ms| ms.parse::<u64>().ok())
        .unwrap_or(0);
    Duration::from_millis(now_unix_ms.saturating_sub(ms))
}

/// Groups candidates by room, keeping the order within each.
pub fn by_room(candidates: &[(MessageId, Uuid)]) -> HashMap<Uuid, Vec<MessageId>> {
    let mut rooms: HashMap<Uuid, Vec<MessageId>> = HashMap::new();
    for (id, room) in candidates {
        rooms.entry(*room).or_default().push(*id);
    }
    rooms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_ages_come_from_the_stream_id() {
        assert_eq!(entry_age("1000-0", 4000), Duration::from_secs(3));
        assert_eq!(entry_age("5000-3", 4000), Duration::ZERO);
        assert!(entry_age("garbage", 4000) >= Duration::from_secs(4));
    }

    #[test]
    fn candidates_group_by_room_in_order() {
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let id = |n| MessageId::from_parts(n, 0, 0);
        let grouped = by_room(&[(id(3), a), (id(2), b), (id(1), a)]);
        assert_eq!(grouped[&a], [id(3), id(1)]);
        assert_eq!(grouped[&b], [id(2)]);
    }
}
