//! The Redis pub/sub connection: one per instance, owned by one task.
//!
//! The task subscribes to the channels local sockets need (asked through
//! [`Subscriber`]), dispatches each incoming event to the [`Hub`], and
//! handles losing Redis (docs/design.md 16.2):
//!
//! 1. It notices: the message stream ends, a command fails, or a `PING`
//!    sent every 5 s gets no answer (which catches a half-open connection).
//! 2. It reconnects with backoff and resubscribes every channel.
//! 3. It rebuilds routing from Postgres, so removals it missed take effect,
//!    and re-checks every local session.
//! 4. Only then does it send `resync` to every local socket. Each client
//!    fetches what it missed from Scylla.
//!
//! Resubscribing before telling clients to resync leaves no window between
//! "caught up" and "live".

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::extract::ws::Utf8Bytes;
use futures_util::StreamExt;
use protocol::frames::{LeaveReason, ServerFrame, close};
use redis::{aio::ConnectionManager, aio::PubSubSink};
use sqlx::PgPool;
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::timeout,
};
use uuid::Uuid;

use crate::realtime::{
    events::{self, Event},
    hub::{Hub, encode},
};

const PING_EVERY: Duration = Duration::from_secs(5);
/// Any Redis command on the pub/sub connection that takes longer than this
/// counts as a lost connection.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(4);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// How often every local socket's session is re-checked, in case a
/// revocation event was missed.
const SESSION_SWEEP_EVERY: Duration = Duration::from_secs(30);

enum Command {
    Ensure {
        channels: Vec<String>,
        done: oneshot::Sender<bool>,
    },
    Release {
        channels: Vec<String>,
    },
}

/// A handle for asking the subscriber task to (un)subscribe.
#[derive(Clone)]
pub struct Subscriber {
    commands: mpsc::UnboundedSender<Command>,
    connected: Arc<AtomicBool>,
}

impl Subscriber {
    /// Subscribes to `channels` and waits until Redis confirms. False if the
    /// pub/sub connection is down: the caller should refuse the socket, and
    /// the client will retry.
    pub async fn ensure(&self, channels: Vec<String>) -> bool {
        if channels.is_empty() {
            return self.is_connected();
        }
        let (done, confirmed) = oneshot::channel();
        if self
            .commands
            .send(Command::Ensure { channels, done })
            .is_err()
        {
            return false;
        }
        confirmed.await.unwrap_or(false)
    }

    /// Unsubscribes from channels no local socket needs. The task checks the
    /// hub again before unsubscribing, so a concurrent new interest wins.
    pub fn release(&self, channels: Vec<String>) {
        if !channels.is_empty() {
            let _ = self.commands.send(Command::Release { channels });
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }
}

pub struct Deps {
    pub redis_url: String,
    pub hub: Arc<Hub>,
    pub pg: PgPool,
    pub redis: ConnectionManager,
}

pub fn spawn(
    deps: Deps,
    shutdown: watch::Receiver<bool>,
) -> (Subscriber, tokio::task::JoinHandle<()>) {
    let (commands, receiver) = mpsc::unbounded_channel();
    let connected = Arc::new(AtomicBool::new(false));
    let handle = Subscriber {
        commands,
        connected: Arc::clone(&connected),
    };
    let task = tokio::spawn(run(deps, receiver, connected, shutdown));
    (handle, task)
}

async fn run(
    deps: Deps,
    mut commands: mpsc::UnboundedReceiver<Command>,
    connected: Arc<AtomicBool>,
    mut shutdown: watch::Receiver<bool>,
) {
    let client = match redis::Client::open(deps.redis_url.as_str()) {
        Ok(client) => client,
        Err(err) => {
            tracing::error!(error = %err, "invalid REDIS_URL for pub/sub");
            return;
        }
    };
    let mut first_connection = true;
    let mut backoff = Duration::from_millis(200);
    let mut sweep = tokio::time::interval(SESSION_SWEEP_EVERY);

    loop {
        if *shutdown.borrow() {
            return;
        }
        let Some((mut sink, mut stream)) = connect(&client, &deps.hub).await else {
            connected.store(false, Ordering::Relaxed);
            // Requests made while disconnected fail now rather than hang.
            while let Ok(command) = commands.try_recv() {
                if let Command::Ensure { done, .. } = command {
                    let _ = done.send(false);
                }
            }
            tokio::select! {
                () = tokio::time::sleep(backoff) => {}
                _ = shutdown.changed() => return,
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
            continue;
        };
        connected.store(true, Ordering::Relaxed);
        backoff = Duration::from_millis(200);
        if !first_connection {
            reconcile(&deps, &mut sink).await;
            let sockets = deps.hub.connection_count();
            deps.hub.deliver_to_all(&encode(&ServerFrame::Resync {
                reason: "reconnected to fan-out".into(),
            }));
            tracing::info!(sockets, "pub/sub reconnected; told every socket to resync");
        }
        first_connection = false;

        let mut ping = tokio::time::interval(PING_EVERY);
        ping.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.changed() => return,
                command = commands.recv() => {
                    let Some(command) = command else { return };
                    if !handle_command(command, &mut sink, &deps.hub).await {
                        break;
                    }
                }
                message = stream.next() => {
                    let Some(message) = message else {
                        tracing::warn!("pub/sub connection lost");
                        break;
                    };
                    if !dispatch(&deps, &mut sink, &message).await {
                        break;
                    }
                }
                _ = ping.tick() => {
                    let pong = timeout(COMMAND_TIMEOUT, sink.ping::<redis::Value>()).await;
                    if !matches!(pong, Ok(Ok(_))) {
                        tracing::warn!("pub/sub ping failed; reconnecting");
                        break;
                    }
                }
                _ = sweep.tick() => check_sessions(&deps.hub, &mut deps.redis.clone()).await,
            }
        }
        connected.store(false, Ordering::Relaxed);
    }
}

/// Opens a pub/sub connection and subscribes to everything the hub needs.
async fn connect(
    client: &redis::Client,
    hub: &Hub,
) -> Option<(PubSubSink, redis::aio::PubSubStream)> {
    let pubsub = match timeout(COMMAND_TIMEOUT, client.get_async_pubsub()).await {
        Ok(Ok(pubsub)) => pubsub,
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "pub/sub connect failed");
            return None;
        }
        Err(_) => {
            tracing::warn!("pub/sub connect timed out");
            return None;
        }
    };
    let (mut sink, stream) = pubsub.split();
    let channels = hub.all_channels();
    match timeout(COMMAND_TIMEOUT, sink.subscribe(&channels)).await {
        Ok(Ok(())) => Some((sink, stream)),
        _ => {
            tracing::warn!("resubscribing failed");
            None
        }
    }
}

async fn subscribe(sink: &mut PubSubSink, channels: &[String]) -> bool {
    channels.is_empty()
        || matches!(
            timeout(COMMAND_TIMEOUT, sink.subscribe(channels)).await,
            Ok(Ok(()))
        )
}

async fn unsubscribe_unwanted(sink: &mut PubSubSink, hub: &Hub, channels: Vec<String>) -> bool {
    let unwanted: Vec<String> = channels.into_iter().filter(|c| !hub.wants(c)).collect();
    unwanted.is_empty()
        || matches!(
            timeout(COMMAND_TIMEOUT, sink.unsubscribe(&unwanted)).await,
            Ok(Ok(()))
        )
}

/// Returns false if the connection failed.
async fn handle_command(command: Command, sink: &mut PubSubSink, hub: &Hub) -> bool {
    match command {
        Command::Ensure { channels, done } => {
            let ok = subscribe(sink, &channels).await;
            let _ = done.send(ok);
            ok
        }
        Command::Release { channels } => unsubscribe_unwanted(sink, hub, channels).await,
    }
}

fn left_frame(room_id: Uuid, reason: LeaveReason) -> Utf8Bytes {
    encode(&ServerFrame::RoomLeft { room_id, reason })
}

/// Applies one event. Returns false if the connection failed.
async fn dispatch(deps: &Deps, sink: &mut PubSubSink, message: &redis::Msg) -> bool {
    let channel = message.get_channel_name();
    let Ok(payload) = message.get_payload::<String>() else {
        return true;
    };
    let event: Event = match serde_json::from_str(&payload) {
        Ok(event) => event,
        Err(err) => {
            tracing::warn!(error = %err, channel, "ignoring malformed event");
            return true;
        }
    };
    let id_after = |prefix: &str| {
        channel
            .strip_prefix(prefix)
            .and_then(|id| id.parse::<Uuid>().ok())
    };
    let hub = &deps.hub;
    match event {
        Event::Deliver { frame } => {
            let frame = Utf8Bytes::from(frame);
            if let Some(room) = id_after("room:") {
                hub.deliver_to_room(room, &frame);
            } else if let Some(user) = id_after("user:") {
                hub.deliver_to_user(user, &frame);
            }
        }
        Event::Presence { user_id, frame } => {
            hub.deliver_to_watchers(user_id, &Utf8Bytes::from(frame));
        }
        Event::Joined { room } => {
            let Some(user) = id_after("user:") else {
                return true;
            };
            // Route, then subscribe, then tell the sockets: once they hear
            // `room.joined` and load history, live delivery is already on.
            if hub.route_user_to_room(user, room.id)
                && !subscribe(sink, &[events::room_channel(room.id)]).await
            {
                return false;
            }
            hub.deliver_to_user(user, &encode(&ServerFrame::RoomJoined { room }));
        }
        Event::MemberLeft {
            room_id,
            user_id,
            reason,
        } => {
            let (senders, released) = hub.unroute_user_from_room(user_id, room_id);
            let frame = left_frame(room_id, reason);
            for sender in senders {
                let _ = sender.try_send(frame.clone());
            }
            if released
                && !unsubscribe_unwanted(sink, hub, vec![events::room_channel(room_id)]).await
            {
                return false;
            }
        }
        Event::RoomDeleted { room_id } => {
            let frame = left_frame(room_id, LeaveReason::Deleted);
            for sender in hub.drop_room(room_id) {
                let _ = sender.try_send(frame.clone());
            }
            if !unsubscribe_unwanted(sink, hub, vec![events::room_channel(room_id)]).await {
                return false;
            }
        }
        Event::SessionRevoked { session_id } => {
            hub.close_session(session_id, (close::SESSION_ENDED, "signed out"));
        }
    }
    true
}

/// After a reconnect: bring routing back in line with Postgres (removals or
/// joins we missed), and close sockets whose session ended meanwhile.
async fn reconcile(deps: &Deps, sink: &mut PubSubSink) {
    for user in deps.hub.local_users() {
        let rooms: HashSet<Uuid> = match storage::rooms::room_ids_for_user(&deps.pg, user).await {
            Ok(rooms) => rooms.into_iter().collect(),
            Err(err) => {
                tracing::warn!(error = %err, %user, "reconciling rooms failed; keeping current routing");
                continue;
            }
        };
        let (added, removed) = deps.hub.set_user_rooms(user, &rooms);
        let new_channels: Vec<String> = added
            .iter()
            .map(|&room| events::room_channel(room))
            .collect();
        subscribe(sink, &new_channels).await;
        for room in removed {
            deps.hub
                .deliver_to_user(user, &left_frame(room, LeaveReason::Removed));
            unsubscribe_unwanted(sink, &deps.hub, vec![events::room_channel(room)]).await;
        }
    }
    check_sessions(&deps.hub, &mut deps.redis.clone()).await;
}

/// Closes local sockets whose session no longer exists. Revocations are
/// also announced by event; this catches the ones a disconnect lost.
pub async fn check_sessions(hub: &Hub, redis: &mut ConnectionManager) {
    let sessions = hub.sessions();
    let hashes: Vec<_> = sessions.iter().map(|(_, hash)| *hash).collect();
    match storage::sessions::live_exists_many(redis, &hashes).await {
        Ok(exists) => {
            for ((session_id, _), alive) in sessions.iter().zip(exists) {
                if !alive {
                    hub.close_session(*session_id, (close::SESSION_ENDED, "session ended"));
                }
            }
        }
        // Redis is down; the next sweep or reconnect tries again.
        Err(err) => tracing::debug!(error = %err, "session sweep skipped"),
    }
}
