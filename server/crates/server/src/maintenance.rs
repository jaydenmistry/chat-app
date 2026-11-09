//! Operator maintenance commands for the message store (docs/design.md 10,
//! 4.2), run on the host with `docker exec`:
//!
//!   server reindex --room <id>    rebuild a room's search entries
//!   server reclaim --room <id>    delete a deleted room's Scylla partitions
//!
//! `reindex` re-enqueues every message of a room onto the search stream, so
//! the running indexers rebuild its entries — this also recovers entries that
//! were trimmed from the stream while no indexer ran. `reclaim` frees the
//! Scylla space of a room that has already been deleted from Postgres: its
//! message rows have no TTL and would otherwise linger forever.

use anyhow::{Context, bail};
use protocol::ids::MessageId;
use scylla::client::session::Session;
use storage::messages::MessageStore;
use uuid::Uuid;

use crate::{config::Config, search, startup};

const USAGE: &str = "usage: server <reindex | reclaim> --room <room_id>";

pub async fn run(config: &Config, args: &[String]) -> anyhow::Result<()> {
    match args {
        [cmd, flag, id] if flag == "--room" => {
            let room_id: Uuid = id.parse().context("room id must be a UUID")?;
            match cmd.as_str() {
                "reindex" => reindex(config, room_id).await,
                "reclaim" => reclaim(config, room_id).await,
                _ => bail!("{USAGE}"),
            }
        }
        _ => bail!("{USAGE}"),
    }
}

/// Re-enqueues every message of a (live) room for indexing.
async fn reindex(config: &Config, room_id: Uuid) -> anyhow::Result<()> {
    let stores = startup::connect(config).await?;
    if storage::rooms::room_kind(&stores.pg, room_id)
        .await?
        .is_none()
    {
        bail!("no such room: {room_id}");
    }
    let messages = MessageStore::new(stores.scylla)
        .await
        .context("preparing scylla statements")?;
    let mut redis = stores.redis.clone();

    let ids = message_ids(messages.session(), room_id).await?;
    for id in &ids {
        search::enqueue(&mut redis, room_id, *id)
            .await
            .context("enqueueing a message for reindex")?;
    }
    println!(
        "re-enqueued {} message(s) in {room_id} for indexing",
        ids.len()
    );
    Ok(())
}

/// Deletes the Scylla partitions of a room that no longer exists in Postgres.
async fn reclaim(config: &Config, room_id: Uuid) -> anyhow::Result<()> {
    let stores = startup::connect(config).await?;
    if storage::rooms::room_kind(&stores.pg, room_id)
        .await?
        .is_some()
    {
        bail!("room {room_id} still exists; delete it first");
    }
    let messages = MessageStore::new(stores.scylla)
        .await
        .context("preparing scylla statements")?;
    let session = messages.session();

    // Each (room_id, bucket) is one partition of messages and of reactions;
    // deleting by the partition key is a single tombstone, not one per row.
    let buckets = buckets(session, room_id).await?;
    for bucket in &buckets {
        session
            .query_unpaged(
                "DELETE FROM messages WHERE room_id = ? AND bucket = ?",
                (room_id, *bucket),
            )
            .await
            .map_err(|e| anyhow::anyhow!("deleting messages: {e}"))?;
        session
            .query_unpaged(
                "DELETE FROM reactions WHERE room_id = ? AND bucket = ?",
                (room_id, *bucket),
            )
            .await
            .map_err(|e| anyhow::anyhow!("deleting reactions: {e}"))?;
    }
    session
        .query_unpaged("DELETE FROM room_buckets WHERE room_id = ?", (room_id,))
        .await
        .map_err(|e| anyhow::anyhow!("deleting room_buckets: {e}"))?;
    println!(
        "reclaimed {} bucket(s) of room {room_id}; room_changes expire on their own",
        buckets.len()
    );
    Ok(())
}

/// The non-empty hour buckets of a room.
async fn buckets(session: &Session, room_id: Uuid) -> anyhow::Result<Vec<i32>> {
    let result = session
        .query_unpaged(
            "SELECT bucket FROM room_buckets WHERE room_id = ?",
            (room_id,),
        )
        .await
        .map_err(|e| anyhow::anyhow!("reading room_buckets: {e}"))?;
    let rows = result
        .into_rows_result()
        .map_err(|e| anyhow::anyhow!("reading room_buckets: {e}"))?;
    let mut buckets = Vec::new();
    for row in rows
        .rows::<(i32,)>()
        .map_err(|e| anyhow::anyhow!("reading room_buckets: {e}"))?
    {
        buckets.push(
            row.map_err(|e| anyhow::anyhow!("reading room_buckets: {e}"))?
                .0,
        );
    }
    Ok(buckets)
}

/// Every message ID in a room, across its buckets.
async fn message_ids(session: &Session, room_id: Uuid) -> anyhow::Result<Vec<MessageId>> {
    let mut ids = Vec::new();
    for bucket in buckets(session, room_id).await? {
        let result = session
            .query_unpaged(
                "SELECT message_id FROM messages WHERE room_id = ? AND bucket = ?",
                (room_id, bucket),
            )
            .await
            .map_err(|e| anyhow::anyhow!("reading messages: {e}"))?;
        let rows = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("reading messages: {e}"))?;
        for row in rows
            .rows::<(i64,)>()
            .map_err(|e| anyhow::anyhow!("reading messages: {e}"))?
        {
            let (id,) = row.map_err(|e| anyhow::anyhow!("reading messages: {e}"))?;
            if let Some(id) = MessageId::from_i64(id) {
                ids.push(id);
            }
        }
    }
    Ok(ids)
}
