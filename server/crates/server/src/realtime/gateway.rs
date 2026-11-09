//! The WebSocket endpoint, `/ws`.
//!
//! The upgrade request is checked like any state-changing request: the
//! `Origin` must be ours (against cross-site WebSocket hijacking, including
//! from other `*.jmistry.com` sites) and the session cookie must be valid.
//! The client must offer the `chat.v1` subprotocol.
//!
//! Each connection then runs as two tasks:
//! - the reader (this task): parses frames and handles them in order;
//! - the writer: owns the socket's send half, drains the connection's
//!   bounded queue, and sends `ping` every 25 s.
//!
//! Everything sent to the socket goes through the queue, so one task writes.
//! Either task ending ends the connection.

use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    time::Duration,
};

use axum::{
    extract::{
        State,
        ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header::ORIGIN},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use protocol::frames::{
    ClientFrame, FrameErrorCode, HEARTBEAT_MS, IDLE_TIMEOUT_MS, MAX_FRAME_BYTES, SUBPROTOCOL,
    ServerFrame, close,
};
use storage::sessions::TokenHash;
use time::OffsetDateTime;
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, sleep_until},
};
use uuid::Uuid;

use crate::{
    auth::session::CurrentSession,
    http::{client_ip::ClientIp, error::AppError, origin::origin_allowed},
    realtime::{
        conn_cap,
        events::{self, Event},
        hub::{CloseReason, ConnHandle, encode},
        mutate, presence,
        send::{self, Sender},
    },
    state::AppState,
};

/// Frames queued for one socket. Fan-out never waits for a full queue: that
/// socket is closed instead (`Hub::send_all`).
pub const OUTBOUND_CAPACITY: usize = 256;
/// Typing events from one connection, per room, at most this often.
const TYPING_INTERVAL: Duration = Duration::from_secs(2);
const MAX_REQ_ID: usize = 64;
const MAX_WATCHED: usize = 200;

pub async fn upgrade(
    State(state): State<AppState>,
    headers: HeaderMap,
    ClientIp(ip): ClientIp,
    session: Result<CurrentSession, AppError>,
    ws: WebSocketUpgrade,
) -> Response {
    if !origin_allowed(&state.config.allowed_origins, headers.get(ORIGIN)) {
        return AppError::Forbidden("origin not allowed").into_response();
    }
    // While draining, send new sockets to the other instance (docs/design.md 13).
    if state
        .shutting_down
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "server is shutting down").into_response();
    }
    let session = match session {
        Ok(session) => session,
        Err(err) => return err.into_response(),
    };
    let ws = ws.protocols([SUBPROTOCOL]);
    if ws.selected_protocol().is_none() {
        return (
            StatusCode::BAD_REQUEST,
            format!("offer the {SUBPROTOCOL} WebSocket subprotocol"),
        )
            .into_response();
    }
    let user = match storage::users::find_by_id(&state.pg, session.user_id).await {
        Ok(Some(user)) => user,
        Ok(None) => return AppError::Unauthorized("sign in required").into_response(),
        Err(err) => return AppError::from(err).into_response(),
    };
    let ctx = ConnCtx {
        conn_id: Uuid::now_v7(),
        user_id: user.id,
        ip,
        session_id: session.session_id,
        token_hash: session.token_hash,
        expires_at: user.expires_at,
    };
    ws.max_message_size(MAX_FRAME_BYTES)
        .max_frame_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| run(state, socket, ctx))
}

struct ConnCtx {
    conn_id: Uuid,
    user_id: Uuid,
    ip: IpAddr,
    session_id: Uuid,
    token_hash: TokenHash,
    expires_at: Option<OffsetDateTime>,
}

/// Every log line from the connection carries its `conn_id` and `user_id`.
async fn run(state: AppState, socket: WebSocket, ctx: ConnCtx) {
    use tracing::Instrument;
    let span = tracing::info_span!("ws", conn_id = %ctx.conn_id, user_id = %ctx.user_id);
    serve(state, socket, ctx).instrument(span).await;
}

async fn serve(state: AppState, socket: WebSocket, ctx: ConnCtx) {
    let (mut sink, stream) = socket.split();
    let (outbound, queue) = mpsc::channel(OUTBOUND_CAPACITY);
    let (closer, closed) = watch::channel::<Option<CloseReason>>(None);

    // Refuse the connection before any setup if the user or their IP already
    // holds too many sockets (across all instances).
    if let Some(scope) = conn_cap::admit(&state, ctx.conn_id, ctx.user_id, ctx.ip).await {
        tracing::info!(scope, "connection refused: socket cap reached");
        let _ = sink
            .send(close_message((close::TOO_MANY, "too many connections")))
            .await;
        return;
    }

    let rooms = match storage::rooms::room_ids_for_user(&state.pg, ctx.user_id).await {
        Ok(rooms) => rooms,
        Err(err) => {
            tracing::warn!(error = %err, "loading rooms failed");
            conn_cap::release(&state, ctx.conn_id, ctx.user_id, ctx.ip).await;
            let _ = sink.send(close_message((1011, "try again"))).await;
            return;
        }
    };
    let channels = state.hub.register(
        ctx.conn_id,
        ConnHandle {
            user_id: ctx.user_id,
            ip: ctx.ip,
            session_id: ctx.session_id,
            token_hash: ctx.token_hash,
            outbound: outbound.clone(),
            closer: closer.clone(),
        },
        &rooms,
    );
    // Live delivery must be on before `hello`: the client treats `hello` as
    // "anything from now on arrives here".
    if !state.subscriber.ensure(channels).await {
        tracing::warn!("pub/sub unavailable; refusing connection");
        state.subscriber.release(state.hub.unregister(ctx.conn_id));
        conn_cap::release(&state, ctx.conn_id, ctx.user_id, ctx.ip).await;
        let _ = sink.send(close_message((1013, "try again later"))).await;
        return;
    }
    let hello = ServerFrame::Hello {
        user_id: ctx.user_id,
        conn_id: ctx.conn_id,
        heartbeat_ms: HEARTBEAT_MS,
    };
    let _ = outbound.try_send(encode(&hello));
    presence::connected(&state, ctx.user_id, ctx.conn_id).await;
    crate::observe::connection_opened();
    tracing::info!(rooms = rooms.len(), "connected");

    let writer = tokio::spawn(write_loop(sink, queue, closed.clone()));
    let reason = read_loop(&state, &ctx, stream, &outbound, closed).await;
    if let Some(reason) = reason {
        let _ = closer.send(Some(reason));
    }
    // Wait for the writer to send the close frame, but not forever.
    let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;

    state.subscriber.release(state.hub.unregister(ctx.conn_id));
    conn_cap::release(&state, ctx.conn_id, ctx.user_id, ctx.ip).await;
    presence::disconnected(&state, ctx.user_id, ctx.conn_id).await;
    crate::observe::connection_closed();
    tracing::info!("disconnected");
}

fn close_message((code, reason): CloseReason) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: Utf8Bytes::from_static(reason),
    }))
}

async fn write_loop(
    mut sink: SplitSink<WebSocket, Message>,
    mut queue: mpsc::Receiver<Utf8Bytes>,
    mut closed: watch::Receiver<Option<CloseReason>>,
) {
    let ping = encode(&ServerFrame::Ping);
    let mut heartbeat = tokio::time::interval(Duration::from_millis(u64::from(HEARTBEAT_MS)));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            // `biased`: a close request wins over queued frames, so a slow
            // client is cut off instead of drained.
            biased;
            changed = closed.changed() => {
                let reason = if changed.is_ok() { *closed.borrow() } else { None };
                let _ = sink.send(close_message(reason.unwrap_or((close::RESTARTING, "closing")))).await;
                return;
            }
            frame = queue.recv() => {
                let Some(frame) = frame else { return };
                if sink.send(Message::Text(frame)).await.is_err() {
                    return;
                }
            }
            _ = heartbeat.tick() => {
                if sink.send(Message::Text(ping.clone())).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Handles frames until the client goes away, breaks the protocol, goes
/// silent, or the connection is closed from elsewhere. Returns the close
/// reason to send, if the reader decided to close.
async fn read_loop(
    state: &AppState,
    ctx: &ConnCtx,
    mut stream: futures_util::stream::SplitStream<WebSocket>,
    outbound: &mpsc::Sender<Utf8Bytes>,
    mut closed: watch::Receiver<Option<CloseReason>>,
) -> Option<CloseReason> {
    let idle = Duration::from_millis(u64::from(IDLE_TIMEOUT_MS));
    let mut deadline = Instant::now() + idle;
    // A guest's session ends exactly when the account expires.
    let expiry = ctx.expires_at.map(|at| {
        let left = Duration::try_from(at - OffsetDateTime::now_utc()).unwrap_or_default();
        Instant::now() + left
    });
    let mut last_typing: HashMap<Uuid, Instant> = HashMap::new();
    let sender = Sender {
        user_id: ctx.user_id,
        ip: ctx.ip,
        expires_at: ctx.expires_at,
        state,
    };

    loop {
        let frame = tokio::select! {
            _ = closed.changed() => return None,
            () = sleep_until(deadline) => return Some((close::PROTOCOL, "no heartbeat")),
            () = async { match expiry { Some(at) => sleep_until(at).await, None => std::future::pending().await } } => {
                return Some((close::SESSION_ENDED, "guest session expired"));
            }
            frame = stream.next() => frame,
        };
        let text = match frame {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return None,
            Some(Ok(Message::Text(text))) => text,
            Some(Ok(Message::Binary(_))) => return Some((close::PROTOCOL, "text frames only")),
            // Browsers answer WebSocket pings themselves; they count as life.
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {
                deadline = Instant::now() + idle;
                continue;
            }
        };
        deadline = Instant::now() + idle;

        let frame: ClientFrame = match serde_json::from_str(&text) {
            Ok(frame) => frame,
            Err(err) => {
                reply(
                    outbound,
                    &ServerFrame::Error {
                        req_id: None,
                        code: FrameErrorCode::BadFrame,
                        message: format!("unreadable frame: {err}"),
                    },
                );
                continue;
            }
        };
        match frame {
            ClientFrame::Pong => {}
            ClientFrame::MessageSend {
                req_id,
                room_id,
                idempotency_key,
                content,
            } => {
                if !req_id_ok(outbound, &req_id) {
                    continue;
                }
                let outcome =
                    send::send(&sender, &req_id, room_id, idempotency_key, &content).await;
                finish(state, outbound, outcome).await;
            }
            ClientFrame::MessageEdit {
                req_id,
                room_id,
                message_id,
                content,
            } => {
                if !req_id_ok(outbound, &req_id) {
                    continue;
                }
                let outcome = mutate::edit(&sender, &req_id, room_id, message_id, &content).await;
                finish(state, outbound, outcome).await;
            }
            ClientFrame::MessageDelete {
                req_id,
                room_id,
                message_id,
            } => {
                if !req_id_ok(outbound, &req_id) {
                    continue;
                }
                let outcome = mutate::delete(&sender, &req_id, room_id, message_id).await;
                finish(state, outbound, outcome).await;
            }
            ClientFrame::ReactionSet {
                req_id,
                room_id,
                message_id,
                emoji,
                active,
            } => {
                if !req_id_ok(outbound, &req_id) {
                    continue;
                }
                let outcome =
                    mutate::react(&sender, &req_id, room_id, message_id, &emoji, active).await;
                finish(state, outbound, outcome).await;
            }
            ClientFrame::ReadMark {
                room_id,
                message_id,
            } => {
                mutate::mark_read(state, ctx.user_id, room_id, message_id).await;
            }
            ClientFrame::Typing { room_id } => {
                let now = Instant::now();
                if last_typing
                    .get(&room_id)
                    .is_some_and(|t| now - *t < TYPING_INTERVAL)
                {
                    continue;
                }
                last_typing.insert(room_id, now);
                typing(state, ctx.user_id, room_id).await;
            }
            ClientFrame::PresenceSet { state: tab_state } => {
                presence::set(state, ctx.user_id, ctx.conn_id, tab_state).await;
            }
            ClientFrame::PresenceWatch { user_ids } => {
                let watched: HashSet<Uuid> = user_ids.into_iter().take(MAX_WATCHED).collect();
                state.hub.set_watching(ctx.conn_id, watched.clone());
                presence::send_current(state, outbound, &watched).await;
            }
        }
    }
}

/// Request IDs are echoed back, so their size is bounded.
fn req_id_ok(outbound: &mpsc::Sender<Utf8Bytes>, req_id: &str) -> bool {
    if req_id.len() <= MAX_REQ_ID {
        return true;
    }
    reply(
        outbound,
        &ServerFrame::Error {
            req_id: None,
            code: FrameErrorCode::BadFrame,
            message: "req_id is too long".into(),
        },
    );
    false
}

/// The reply goes into this socket's queue before the publish, so the
/// requester always sees its ack or `ok` before the fan-out copy.
async fn finish(state: &AppState, outbound: &mpsc::Sender<Utf8Bytes>, outcome: send::Outcome) {
    reply(outbound, &outcome.reply);
    if let Some(publish) = outcome.publish {
        publish.run(state).await;
    }
}

/// Queues a reply. A full queue is handled like any slow consumer.
fn reply(outbound: &mpsc::Sender<Utf8Bytes>, frame: &ServerFrame) {
    let _ = outbound.try_send(encode(frame));
}

/// Typing indicators: checked like a send, then published to the room.
/// Nothing is stored; a lost one only means a missing "is typing…".
async fn typing(state: &AppState, user_id: Uuid, room_id: Uuid) {
    match storage::rooms::membership(&state.pg, room_id, user_id).await {
        Ok(Some(_)) => {}
        _ => return,
    }
    let frame = ServerFrame::Typing { room_id, user_id };
    events::publish_best_effort(
        &mut state.redis.clone(),
        &events::room_channel(room_id),
        &Event::deliver(&frame),
    )
    .await;
}
