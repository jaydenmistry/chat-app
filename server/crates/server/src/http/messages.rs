//! Message history and resync.

use axum::{
    Json,
    extract::{Path, Query, State},
};
use protocol::{
    api::{HistoryPage, Message, RoomSync, SyncRequest, SyncResponse},
    ids::MessageId,
};
use serde::Deserialize;
use storage::{
    messages::{StoredMessage, page_back, read_forward},
    rooms,
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    auth::session::CurrentSession,
    http::error::{ApiJson, AppError},
    permissions::{self, Action},
    state::AppState,
};

const DEFAULT_PAGE: usize = 50;
const MAX_PAGE: usize = 100;
/// A sync returns at most this many new messages per room; beyond that the
/// client reloads the room instead of patching it.
const SYNC_LIMIT: usize = 500;
const MAX_SYNC_ROOMS: usize = 100;
/// How far before the client's cursor a sync starts. A message can commit
/// up to (1 s clock skew + 2 s write timeout) after a message with a higher
/// ID; 5 s covers that with margin (docs/design.md 16.1).
pub const RESUME_OVERLAP_MS: u64 = 5_000;

pub fn to_api(room_id: Uuid, m: StoredMessage) -> Message {
    let created_at =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(m.id.unix_ms()) * 1_000_000)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    Message {
        id: m.id,
        room_id,
        author_id: m.author_id,
        content: m.content,
        created_at,
        edited_at: m.edited_at,
        deleted: m.deleted,
        version: m.version,
    }
}

fn floor(history_floor: i64) -> MessageId {
    MessageId::from_i64(history_floor).unwrap_or(MessageId::from_parts(0, 0, 0))
}

fn store_error(err: storage::messages::StoreError) -> AppError {
    AppError::Unavailable(err.into())
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    before: Option<MessageId>,
    limit: Option<usize>,
}

pub async fn history(
    State(state): State<AppState>,
    session: CurrentSession,
    Path(room_id): Path<Uuid>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<HistoryPage>, AppError> {
    let membership = rooms::membership(&state.pg, room_id, session.user_id).await?;
    permissions::check(Action::Read, membership.as_ref(), false)?;
    let membership = membership.expect("checked above");
    let limit = query.limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
    let page = page_back(
        &state.messages,
        room_id,
        query.before,
        floor(membership.history_floor),
        limit,
    )
    .await
    .map_err(store_error)?;
    Ok(Json(HistoryPage {
        messages: page
            .messages
            .into_iter()
            .map(|m| to_api(room_id, m))
            .collect(),
        next_before: page.next_before,
    }))
}

/// For each room the client has loaded: the messages after its cursor, from
/// slightly before it. Rooms the caller has left come back with
/// `member: false`.
pub async fn sync(
    State(state): State<AppState>,
    session: CurrentSession,
    ApiJson(req): ApiJson<SyncRequest>,
) -> Result<Json<SyncResponse>, AppError> {
    if req.rooms.len() > MAX_SYNC_ROOMS {
        return Err(AppError::invalid("rooms", "at most 100 rooms per sync"));
    }
    let mut out = Vec::with_capacity(req.rooms.len());
    for cursor in req.rooms {
        let Some(membership) =
            rooms::membership(&state.pg, cursor.room_id, session.user_id).await?
        else {
            out.push(RoomSync {
                room_id: cursor.room_id,
                member: false,
                messages: Vec::new(),
                complete: true,
            });
            continue;
        };
        let forward = read_forward(
            &state.messages,
            cursor.room_id,
            cursor.after.minus_ms(RESUME_OVERLAP_MS),
            floor(membership.history_floor),
            SYNC_LIMIT,
        )
        .await
        .map_err(store_error)?;
        out.push(RoomSync {
            room_id: cursor.room_id,
            member: true,
            messages: forward
                .messages
                .into_iter()
                .map(|m| to_api(cursor.room_id, m))
                .collect(),
            complete: forward.complete,
        });
    }
    Ok(Json(SyncResponse { rooms: out }))
}
