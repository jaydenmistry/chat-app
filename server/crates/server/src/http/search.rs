//! `GET /api/search`: full-text search across the caller's rooms.

use std::collections::HashMap;

use axum::{
    Json,
    extract::{Query, State},
};
use protocol::{
    api::{Message, SearchResults},
    ids::MessageId,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    auth::session::CurrentSession,
    http::{
        error::AppError,
        messages::{store_error, with_reactions},
    },
    search::by_room,
    state::AppState,
};

const PAGE: usize = 20;
const MAX_QUERY_CHARS: usize = 200;

#[derive(Deserialize)]
pub struct SearchQuery {
    q: String,
    room_id: Option<Uuid>,
    before: Option<MessageId>,
}

/// Candidates come from the Postgres index, joined to the caller's
/// memberships. What they say comes from Scylla: deleted or expired
/// messages are dropped there, so the index lagging can never show deleted
/// content (docs/design.md 10).
pub async fn search(
    State(state): State<AppState>,
    session: CurrentSession,
    Query(query): Query<SearchQuery>,
) -> Result<Json<SearchResults>, AppError> {
    let q = query.q.trim();
    if q.is_empty() {
        return Err(AppError::invalid("q", "enter something to search for"));
    }
    if q.chars().count() > MAX_QUERY_CHARS {
        return Err(AppError::invalid("q", "at most 200 characters"));
    }
    let candidates = storage::search::search(
        &state.pg,
        session.user_id,
        q,
        query.room_id,
        query.before,
        PAGE as i64,
    )
    .await?;

    let mut found: HashMap<MessageId, Message> = HashMap::with_capacity(candidates.len());
    for (room_id, ids) in by_room(&candidates) {
        let current = state
            .messages
            .get_many(room_id, &ids)
            .await
            .map_err(store_error)?;
        let live = current.into_iter().filter(|m| !m.deleted).collect();
        for message in with_reactions(&state, room_id, session.user_id, live).await? {
            found.insert(message.id, message);
        }
    }
    // A full page of candidates means there may be more, even if some were
    // dropped by hydration.
    let next_before = (candidates.len() == PAGE)
        .then(|| candidates.last().map(|(id, _)| *id))
        .flatten();
    Ok(Json(SearchResults {
        messages: candidates
            .iter()
            .filter_map(|(id, _)| found.remove(id))
            .collect(),
        next_before,
    }))
}
