//! Message history, resync, and read receipts.

use axum::{
    Json,
    extract::{Path, Query, State},
};
use protocol::{
    api::{HistoryPage, Message, ReactionCount, Receipts, RoomSync, SyncRequest, SyncResponse},
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
    http::{
        error::{ApiJson, AppError},
        rooms::profile_of_member,
    },
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
/// ID; 5 s covers that with margin (docs/design.md 16.1). The change feed's
/// cursor is backed off the same way, for the same reason.
pub const RESUME_OVERLAP_MS: u64 = 5_000;

/// A stored message as the API shows it. `reactions` are as `viewer` sees
/// them (`me`).
pub fn to_api(room_id: Uuid, m: StoredMessage, reactions: Vec<ReactionCount>) -> Message {
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
        reactions,
    }
}

/// Converts stored messages, loading their reactions in one query per
/// bucket. Deleted messages show no reactions.
pub async fn with_reactions(
    state: &AppState,
    room_id: Uuid,
    viewer: Uuid,
    messages: Vec<StoredMessage>,
) -> Result<Vec<Message>, AppError> {
    let ids: Vec<MessageId> = messages
        .iter()
        .filter(|m| !m.deleted)
        .map(|m| m.id)
        .collect();
    let mut reactions = if ids.is_empty() {
        Default::default()
    } else {
        state
            .messages
            .reactions_for(room_id, &ids, viewer)
            .await
            .map_err(store_error)?
    };
    Ok(messages
        .into_iter()
        .map(|m| {
            let list = reactions
                .remove(&m.id)
                .unwrap_or_default()
                .into_iter()
                .map(|r| ReactionCount {
                    emoji: r.emoji,
                    count: r.count,
                    me: r.me,
                })
                .collect();
            to_api(room_id, m, list)
        })
        .collect())
}

pub fn floor(history_floor: i64) -> MessageId {
    MessageId::from_i64(history_floor).unwrap_or(MessageId::from_parts(0, 0, 0))
}

pub fn store_error(err: storage::messages::StoreError) -> AppError {
    AppError::Unavailable(err.into())
}

/// "Now", as a cursor: the lowest ID that could be issued at this moment.
fn now_cursor(state: &AppState) -> MessageId {
    MessageId::first_at_ms(state.ids.now_ms())
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
    // Taken before reading, so a change made during the read is not skipped
    // by the next sync.
    let change_cursor = now_cursor(&state);
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
        messages: with_reactions(&state, room_id, session.user_id, page.messages).await?,
        next_before: page.next_before,
        change_cursor,
    }))
}

/// For each room the client has loaded: the messages after its cursor (from
/// slightly before it), and, with a change cursor, the current state of
/// older messages that changed since. Rooms the caller has left come back
/// with `member: false`.
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
        let room_id = cursor.room_id;
        let change_cursor = now_cursor(&state);
        let Some(membership) = rooms::membership(&state.pg, room_id, session.user_id).await? else {
            out.push(RoomSync {
                room_id,
                member: false,
                messages: Vec::new(),
                changed: Vec::new(),
                change_cursor,
                complete: true,
            });
            continue;
        };
        let forward = read_forward(
            &state.messages,
            room_id,
            cursor.after.minus_ms(RESUME_OVERLAP_MS),
            floor(membership.history_floor),
            SYNC_LIMIT,
        )
        .await
        .map_err(store_error)?;
        let mut complete = forward.complete;

        let mut changed = Vec::new();
        if let Some(changes_after) = cursor.changes_after {
            let (ids, feed_complete) = state
                .messages
                .changes_since(
                    room_id,
                    changes_after.minus_ms(RESUME_OVERLAP_MS),
                    change_cursor,
                )
                .await
                .map_err(store_error)?;
            complete &= feed_complete;
            // Messages newer than the cursor come back in full above.
            let older: Vec<MessageId> = ids.into_iter().filter(|id| *id <= cursor.after).collect();
            if !older.is_empty() {
                let current = state
                    .messages
                    .get_many(room_id, &older)
                    .await
                    .map_err(store_error)?;
                changed = with_reactions(&state, room_id, session.user_id, current).await?;
            }
        }
        out.push(RoomSync {
            room_id,
            member: true,
            messages: with_reactions(&state, room_id, session.user_id, forward.messages).await?,
            changed,
            change_cursor,
            complete,
        });
    }
    Ok(Json(SyncResponse { rooms: out }))
}

#[derive(Deserialize)]
pub struct ReceiptsQuery {
    message_id: MessageId,
}

/// "Seen by N": fetched on demand, never broadcast in group rooms, because a
/// broadcast would cost every member's read times every member's socket
/// (docs/design.md 9).
pub async fn receipts(
    State(state): State<AppState>,
    session: CurrentSession,
    Path(room_id): Path<Uuid>,
    Query(query): Query<ReceiptsQuery>,
) -> Result<Json<Receipts>, AppError> {
    let membership = rooms::membership(&state.pg, room_id, session.user_id).await?;
    permissions::check(Action::Read, membership.as_ref(), false)?;
    let row = rooms::receipts(
        &state.pg,
        room_id,
        query.message_id.as_i64(),
        session.user_id,
    )
    .await?;
    Ok(Json(Receipts {
        count: u32::try_from(row.count).unwrap_or(u32::MAX),
        readers: row.readers.iter().map(profile_of_member).collect(),
    }))
}
