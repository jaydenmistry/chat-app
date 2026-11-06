//! Room endpoints: create, browse, join, leave, delete, members, invites,
//! DMs, and user profiles.
//!
//! Every change commits to Postgres first and then publishes an event, so
//! sockets on every instance follow the membership change (see
//! `realtime::events`).

use std::{collections::HashMap, time::Duration};

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use protocol::{
    api::{
        CreateDmRequest, CreateRoomRequest, Invite, Member, PublicRoom, RoomInfo, RoomKind,
        UserProfile,
    },
    frames::{LeaveReason, ServerFrame},
    ids::MessageId,
    validate,
};
use serde::Deserialize;
use storage::{
    messages::count_unread,
    rooms::{self, InviteOutcome, UserRoom},
    users::{self, User},
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    auth::{session::CurrentSession, token},
    http::{
        error::{ApiJson, AppError},
        messages::floor,
    },
    permissions::{self, Action, Denied},
    rate_limit,
    realtime::events::{self, Event},
    state::AppState,
};

const INVITE_LIFETIME: Duration = Duration::from_secs(7 * 24 * 3600);
const INVITE_MAX_USES: i32 = 25;

/// The membership cap is reached (docs/design.md 11.2).
const ROOM_CAP_FULL: &str = "you've reached the maximum number of rooms; leave one first";

/// How many rooms this user may belong to: a lower cap for guests.
fn room_cap(state: &AppState, user: &User) -> i64 {
    if user.is_guest {
        state.config.max_rooms_per_guest
    } else {
        state.config.max_rooms_per_user
    }
}

/// Fails when the user already belongs to their maximum number of rooms
/// (docs/design.md 11.2). Checked before any path that adds a new membership.
async fn check_room_cap(state: &AppState, user: &User) -> Result<(), AppError> {
    if rooms::membership_count(&state.pg, user.id).await? >= room_cap(state, user) {
        return Err(AppError::Forbidden(ROOM_CAP_FULL));
    }
    Ok(())
}

/// The join / DM-creation limit (docs/design.md 11.1): guests get a lower cap.
async fn enforce_join_limit(state: &AppState, user: &User) -> Result<(), AppError> {
    let bucket = if user.is_guest {
        state.config.rate_limits.join_guest
    } else {
        state.config.rate_limits.join_user
    };
    rate_limit::enforce(
        &mut state.redis.clone(),
        "join",
        "you're joining rooms too fast; try again later",
        &[(format!("rl:join:u:{}", user.id), bucket)],
    )
    .await
}

impl From<Denied> for AppError {
    fn from(denied: Denied) -> Self {
        match denied {
            Denied::NotMember => AppError::NotFound,
            Denied::NotAllowed(reason) => AppError::Forbidden(reason),
        }
    }
}

pub fn profile(user: &User) -> UserProfile {
    UserProfile {
        id: user.id,
        handle: user.handle.clone(),
        display_name: user.display_name.clone(),
        avatar_url: user.avatar_url.clone(),
        is_guest: user.is_guest,
    }
}

/// The caller's account. Needed where the rules depend on being a guest.
pub async fn caller(state: &AppState, session: &CurrentSession) -> Result<User, AppError> {
    users::find_by_id(&state.pg, session.user_id)
        .await?
        .ok_or(AppError::Unauthorized("sign in required"))
}

pub fn profile_of_member(r: &rooms::MemberRow) -> UserProfile {
    UserProfile {
        id: r.user_id,
        handle: r.handle.clone(),
        display_name: r.display_name.clone(),
        avatar_url: r.avatar_url.clone(),
        is_guest: r.is_guest,
    }
}

fn member_api(r: &rooms::MemberRow) -> Member {
    Member {
        user: profile_of_member(r),
        role: r.role,
        joined_at: r.joined_at,
    }
}

/// Unread count for one room: at most 100 messages read, whatever the
/// room's size. A storage failure shows as 0 rather than failing the whole
/// room list.
async fn unread(state: &AppState, room: &UserRoom, viewer: Uuid) -> (u32, bool) {
    let after = MessageId::from_i64(room.last_read_id).unwrap_or(MessageId::from_parts(0, 0, 0));
    match count_unread(
        &state.messages,
        room.id,
        after,
        floor(room.history_floor),
        viewer,
    )
    .await
    {
        Ok(counted) => counted,
        Err(err) => {
            tracing::warn!(error = %err, room_id = %room.id, "unread count failed");
            (0, false)
        }
    }
}

/// Turns rooms into API objects: every DM peer's profile in one query, and
/// unread counts for all rooms concurrently.
async fn room_infos(
    state: &AppState,
    rooms: Vec<UserRoom>,
    viewer: Uuid,
) -> Result<Vec<RoomInfo>, AppError> {
    let peer_ids: Vec<Uuid> = rooms.iter().filter_map(|r| r.dm_peer).collect();
    let peers: HashMap<Uuid, UserProfile> = if peer_ids.is_empty() {
        HashMap::new()
    } else {
        users::find_many(&state.pg, &peer_ids)
            .await?
            .iter()
            .map(|u| (u.id, profile(u)))
            .collect()
    };
    let counts =
        futures_util::future::join_all(rooms.iter().map(|r| unread(state, r, viewer))).await;
    Ok(rooms
        .into_iter()
        .zip(counts)
        .map(|(r, (unread, unread_capped))| RoomInfo {
            id: r.id,
            kind: r.kind,
            name: r.name,
            role: r.role,
            dm_peer: r.dm_peer.and_then(|id| peers.get(&id).cloned()),
            created_at: r.created_at,
            last_read_id: MessageId::from_i64(r.last_read_id)
                .unwrap_or(MessageId::from_parts(0, 0, 0)),
            unread,
            unread_capped,
        })
        .collect())
}

async fn room_info_for(
    state: &AppState,
    room_id: Uuid,
    user_id: Uuid,
) -> Result<RoomInfo, AppError> {
    let room = rooms::room_for_user(&state.pg, room_id, user_id)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(room_infos(state, vec![room], user_id).await?.remove(0))
}

/// Tells the room's other members someone joined.
async fn announce_member_joined(state: &AppState, room_id: Uuid, user_id: Uuid) {
    let member = match rooms::member(&state.pg, room_id, user_id).await {
        Ok(Some(row)) => member_api(&row),
        _ => return,
    };
    events::publish_best_effort(
        &mut state.redis.clone(),
        &events::room_channel(room_id),
        &Event::deliver(&ServerFrame::MemberJoined { room_id, member }),
    )
    .await;
}

/// Tells every socket of `user_id` that they are now in `room`.
async fn announce_joined(state: &AppState, user_id: Uuid, room: RoomInfo) {
    events::publish_best_effort(
        &mut state.redis.clone(),
        &events::user_channel(user_id),
        &Event::Joined { room },
    )
    .await;
}

pub async fn list(
    State(state): State<AppState>,
    session: CurrentSession,
) -> Result<Json<Vec<RoomInfo>>, AppError> {
    let rooms = rooms::rooms_for_user(&state.pg, session.user_id).await?;
    Ok(Json(room_infos(&state, rooms, session.user_id).await?))
}

pub async fn directory(
    State(state): State<AppState>,
    session: CurrentSession,
) -> Result<Json<Vec<PublicRoom>>, AppError> {
    let rows = rooms::public_rooms(&state.pg, session.user_id).await?;
    Ok(Json(
        rows.into_iter()
            .map(|r| PublicRoom {
                id: r.id,
                name: r.name,
                member_count: r.member_count,
                joined: r.joined,
            })
            .collect(),
    ))
}

pub async fn create(
    State(state): State<AppState>,
    session: CurrentSession,
    ApiJson(req): ApiJson<CreateRoomRequest>,
) -> Result<Response, AppError> {
    let user = caller(&state, &session).await?;
    permissions::can_create_room(user.is_guest)?;
    rate_limit::enforce(
        &mut state.redis.clone(),
        "room_create",
        "you're creating rooms too fast; try again later",
        &[(
            format!("rl:roomcreate:u:{}", user.id),
            state.config.rate_limits.room_create_user,
        )],
    )
    .await?;
    check_room_cap(&state, &user).await?;
    if req.kind == RoomKind::Dm {
        return Err(AppError::invalid("kind", "create DMs with POST /api/dms"));
    }
    let name = validate::room_name(&req.name)
        .map_err(|e| AppError::invalid("name", format!("room name {}", e.message())))?;
    let room = rooms::create_room(&state.pg, req.kind, name, user.id).await?;
    let info = room_infos(&state, vec![room], user.id).await?.remove(0);
    announce_joined(&state, user.id, info.clone()).await;
    Ok((StatusCode::CREATED, Json(info)).into_response())
}

pub async fn join(
    State(state): State<AppState>,
    session: CurrentSession,
    Path(room_id): Path<Uuid>,
) -> Result<Json<RoomInfo>, AppError> {
    // Only public rooms can be joined directly. A private room answers the
    // same 404 as a missing one.
    if rooms::room_kind(&state.pg, room_id).await? != Some(RoomKind::Public) {
        return Err(AppError::NotFound);
    }
    let user = caller(&state, &session).await?;
    enforce_join_limit(&state, &user).await?;
    // The cap guards new memberships only; re-joining a room you're already in
    // stays a no-op even at the cap.
    if rooms::membership(&state.pg, room_id, user.id)
        .await?
        .is_none()
    {
        check_room_cap(&state, &user).await?;
    }
    let added = rooms::add_member(&state.pg, room_id, user.id).await?;
    let info = room_info_for(&state, room_id, user.id).await?;
    if added {
        announce_joined(&state, user.id, info.clone()).await;
        announce_member_joined(&state, room_id, user.id).await;
    }
    Ok(Json(info))
}

pub async fn leave(
    State(state): State<AppState>,
    session: CurrentSession,
    Path(room_id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let user = caller(&state, &session).await?;
    let membership = rooms::membership(&state.pg, room_id, user.id).await?;
    permissions::check(Action::Leave, membership.as_ref(), user.is_guest)?;
    if rooms::remove_member(&state.pg, room_id, user.id).await? {
        member_left(&state, room_id, user.id, LeaveReason::Left).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Published on the room channel, so it is ordered with the room's messages:
/// every instance stops routing the room to that user at the same point in
/// the stream (docs/design.md 16.4). Then the remaining members hear
/// `member.left`; the user is no longer routed, so they don't.
async fn member_left(state: &AppState, room_id: Uuid, user_id: Uuid, reason: LeaveReason) {
    let mut redis = state.redis.clone();
    let channel = events::room_channel(room_id);
    events::publish_best_effort(
        &mut redis,
        &channel,
        &Event::MemberLeft {
            room_id,
            user_id,
            reason,
        },
    )
    .await;
    events::publish_best_effort(
        &mut redis,
        &channel,
        &Event::deliver(&ServerFrame::MemberLeft { room_id, user_id }),
    )
    .await;
}

pub async fn delete(
    State(state): State<AppState>,
    session: CurrentSession,
    Path(room_id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let user = caller(&state, &session).await?;
    let membership = rooms::membership(&state.pg, room_id, user.id).await?;
    permissions::check(Action::DeleteRoom, membership.as_ref(), user.is_guest)?;
    rooms::delete_room(&state.pg, room_id).await?;
    // Scylla rows of a deleted room become unreachable: every read checks
    // membership first. Reclaiming the space is a cleanup job (Phase 4).
    events::publish_best_effort(
        &mut state.redis.clone(),
        &events::room_channel(room_id),
        &Event::RoomDeleted { room_id },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn members(
    State(state): State<AppState>,
    session: CurrentSession,
    Path(room_id): Path<Uuid>,
) -> Result<Json<Vec<Member>>, AppError> {
    let membership = rooms::membership(&state.pg, room_id, session.user_id).await?;
    permissions::check(Action::Read, membership.as_ref(), false)?;
    let rows = rooms::members(&state.pg, room_id).await?;
    Ok(Json(rows.iter().map(member_api).collect()))
}

pub async fn remove_member(
    State(state): State<AppState>,
    session: CurrentSession,
    Path((room_id, user_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, AppError> {
    let user = caller(&state, &session).await?;
    let membership = rooms::membership(&state.pg, room_id, user.id).await?;
    permissions::check(Action::RemoveMember, membership.as_ref(), user.is_guest)?;
    if user_id == user.id {
        return Err(AppError::Forbidden("the owner can't remove themselves"));
    }
    if !rooms::remove_member(&state.pg, room_id, user_id).await? {
        return Err(AppError::NotFound);
    }
    member_left(&state, room_id, user_id, LeaveReason::Removed).await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn create_invite(
    State(state): State<AppState>,
    session: CurrentSession,
    Path(room_id): Path<Uuid>,
) -> Result<Response, AppError> {
    let user = caller(&state, &session).await?;
    let membership = rooms::membership(&state.pg, room_id, user.id).await?;
    permissions::check(Action::Invite, membership.as_ref(), user.is_guest)?;
    // 128 random bits: not guessable, and short enough for a link.
    let code = token::random_string(16);
    let expires_at = OffsetDateTime::now_utc() + INVITE_LIFETIME;
    rooms::create_invite(
        &state.pg,
        room_id,
        user.id,
        &code,
        expires_at,
        INVITE_MAX_USES,
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(Invite {
            code,
            expires_at,
            max_uses: INVITE_MAX_USES,
        }),
    )
        .into_response())
}

pub async fn accept_invite(
    State(state): State<AppState>,
    session: CurrentSession,
    Path(code): Path<String>,
) -> Result<Json<RoomInfo>, AppError> {
    let user = caller(&state, &session).await?;
    permissions::can_accept_invite(user.is_guest)?;
    enforce_join_limit(&state, &user).await?;
    // The cap can't be checked before accepting (the invite's room isn't known
    // until then), and only a genuine new join should count, so check after
    // and undo an over-cap join.
    let cap = room_cap(&state, &user);
    let count_before = rooms::membership_count(&state.pg, user.id).await?;
    let room_id = match rooms::accept_invite(&state.pg, &code, user.id).await? {
        InviteOutcome::Joined(room_id) => {
            if count_before >= cap {
                rooms::remove_member(&state.pg, room_id, user.id).await?;
                return Err(AppError::Forbidden(ROOM_CAP_FULL));
            }
            let info = room_info_for(&state, room_id, user.id).await?;
            announce_joined(&state, user.id, info.clone()).await;
            announce_member_joined(&state, room_id, user.id).await;
            return Ok(Json(info));
        }
        InviteOutcome::AlreadyMember(room_id) => room_id,
        InviteOutcome::Invalid => return Err(AppError::NotFound),
    };
    Ok(Json(room_info_for(&state, room_id, user.id).await?))
}

pub async fn open_dm(
    State(state): State<AppState>,
    session: CurrentSession,
    ApiJson(req): ApiJson<CreateDmRequest>,
) -> Result<Json<RoomInfo>, AppError> {
    let user = caller(&state, &session).await?;
    enforce_join_limit(&state, &user).await?;
    let handle = req.handle.trim().trim_start_matches('@').to_lowercase();
    let peer = users::find_by_handle(&state.pg, &handle)
        .await?
        .ok_or(AppError::NotFound)?;
    if peer.id == user.id {
        return Err(AppError::invalid("handle", "you can't message yourself"));
    }
    // A DM with a guest expires with the guest (docs/design.md 11.3).
    let expiry = match (user.expires_at, peer.expires_at) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let (room_id, created) = rooms::find_or_create_dm(&state.pg, user.id, peer.id, expiry).await?;
    let info = room_info_for(&state, room_id, user.id).await?;
    if created {
        announce_joined(&state, user.id, info.clone()).await;
        let peer_info = room_info_for(&state, room_id, peer.id).await?;
        announce_joined(&state, peer.id, peer_info).await;
    }
    Ok(Json(info))
}

#[derive(Deserialize)]
pub struct ProfilesQuery {
    /// Comma-separated user IDs.
    ids: String,
}

/// Profiles by ID, for showing message authors. Handles, names and avatars
/// are public within the app, so any signed-in user may look them up.
pub async fn profiles(
    State(state): State<AppState>,
    _session: CurrentSession,
    Query(query): Query<ProfilesQuery>,
) -> Result<Json<Vec<UserProfile>>, AppError> {
    let ids: Vec<Uuid> = query
        .ids
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::parse)
        .collect::<Result<_, _>>()
        .map_err(|_| AppError::invalid("ids", "comma-separated user IDs"))?;
    if ids.len() > 200 {
        return Err(AppError::invalid("ids", "at most 200 at a time"));
    }
    let users = users::find_many(&state.pg, &ids).await?;
    Ok(Json(users.iter().map(profile).collect()))
}
