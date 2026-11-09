//! The operator admin command (docs/design.md 11.4).
//!
//! `server admin ...`, run on the host with `docker exec`. There is no
//! endpoint or UI. Two actions:
//!
//!   server admin ban <handle>                     suspend an account
//!   server admin delete-message <room_id> <id>    remove a message
//!
//! Both act through the same stores and the same events the live paths use, so
//! open clients see the result at once and resuming clients learn of it from
//! the change feed. Deleting a message therefore needs a real message ID for
//! the change row, so the command briefly leases a worker ID like a server
//! does — which is why it takes a few seconds to start (the lease quarantine).

use std::{sync::Arc, time::Duration};

use anyhow::{Context, bail};
use protocol::{frames::ServerFrame, ids::MessageId};
use storage::messages::{ChangeKind, MessageStore};
use tokio::sync::watch;
use uuid::Uuid;

use crate::{
    config::Config,
    ids::IdGenerator,
    realtime::events::{self, Event},
    search,
    startup::{self, Stores},
};

const USAGE: &str = "usage: server admin <ban <handle> | delete-message <room_id> <message_id>>";

pub async fn run(config: &Config, args: &[String]) -> anyhow::Result<()> {
    match args {
        [sub, handle] if sub == "ban" => {
            let stores = startup::connect(config).await?;
            ban(&stores, handle).await
        }
        [sub, room, id] if sub == "delete-message" => {
            let room_id: Uuid = room.parse().context("room id must be a UUID")?;
            let message_id: MessageId = id.parse().context("message id must be a number")?;
            let stores = startup::connect(config).await?;
            delete_message(config, stores, room_id, message_id).await
        }
        _ => bail!("{USAGE}"),
    }
}

/// Suspends an account and ends its sessions everywhere.
async fn ban(stores: &Stores, handle: &str) -> anyhow::Result<()> {
    let user = storage::users::find_by_handle(&stores.pg, handle)
        .await?
        .with_context(|| format!("no such user: {handle}"))?;
    storage::users::ban(&stores.pg, user.id).await?;

    // End every session: revoke in Postgres, drop the live copy in Redis, and
    // tell the running instances to close its sockets. Login already refuses a
    // banned account (http::auth::login), so no new session can start.
    let revoked = storage::sessions::revoke_all(&stores.pg, user.id).await?;
    let mut redis = stores.redis.clone();
    for session in &revoked {
        if let Err(err) = storage::sessions::delete_live(&mut redis, &session.token_hash).await {
            tracing::warn!(error = %err, "deleting a live session failed");
        }
        events::publish_best_effort(
            &mut redis,
            &events::user_channel(user.id),
            &Event::SessionRevoked {
                session_id: session.id,
            },
        )
        .await;
    }

    println!(
        "banned {handle} ({}); ended {} session(s)",
        user.id,
        revoked.len()
    );
    Ok(())
}

/// Soft-deletes a message, exactly as the author's own delete would
/// (docs/design.md 6.5): a change-feed row, the delete itself, a search-index
/// update, and a `message.deleted` fan-out.
async fn delete_message(
    config: &Config,
    stores: Stores,
    room_id: Uuid,
    message_id: MessageId,
) -> anyhow::Result<()> {
    let mut redis = stores.redis.clone();
    let messages = MessageStore::new(stores.scylla)
        .await
        .context("preparing scylla statements")?;

    let Some((message, ttl)) = messages.get_with_ttl(room_id, message_id).await? else {
        bail!("no such message in that room");
    };
    if message.deleted {
        println!("message {message_id} is already deleted");
        return Ok(());
    }

    // A worker-ID lease, so the change_id is a unique snowflake like a
    // server's. Released when this command exits.
    let ids = Arc::new(IdGenerator::new(0));
    let (stop, stopped) = watch::channel(false);
    let lease = tokio::spawn(crate::ids::run_lease(
        Arc::clone(&ids),
        redis.clone(),
        Uuid::now_v7(),
        config.id_lease,
        stopped,
    ));
    let result = async {
        wait_until_ready(&ids, config.id_lease).await?;
        let change_id = ids.next_id().context("issuing a change ID")?;
        messages
            .record_change(room_id, change_id, message_id, ChangeKind::Delete, ttl)
            .await
            .context("recording the change")?;
        let now_us = i64::try_from(ids.unix_ms()).unwrap_or(i64::MAX / 1000) * 1000;
        let version = now_us.max(message.version + 1);
        messages
            .delete(room_id, message_id, version, ttl)
            .await
            .context("deleting the message")?;
        if let Err(err) = search::enqueue(&mut redis, room_id, message_id).await {
            tracing::warn!(error = %err, "updating the search index failed");
        }
        events::publish_best_effort(
            &mut redis,
            &events::room_channel(room_id),
            &Event::deliver(&ServerFrame::MessageDeleted {
                room_id,
                message_id,
                version,
                change_id,
            }),
        )
        .await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let _ = stop.send(true);
    let _ = lease.await;
    result?;
    println!("deleted message {message_id} in room {room_id}");
    Ok(())
}

/// Waits for the worker-ID lease to become usable (it clears quarantine first).
async fn wait_until_ready(
    ids: &IdGenerator,
    timings: crate::ids::LeaseTimings,
) -> anyhow::Result<()> {
    let deadline =
        std::time::Instant::now() + timings.quarantine + timings.ttl + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if ids.is_ready() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    bail!("could not acquire a worker ID (all leased, or Redis unreachable)")
}
