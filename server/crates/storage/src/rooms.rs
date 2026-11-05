//! Rooms, memberships and invites (Postgres).
//!
//! These functions do what they're asked; whether the caller may ask is
//! decided by the server's permission rules first. Where a rule must hold
//! even under concurrent requests (one DM per pair, one owner per room,
//! invite use limits), it is enforced here, in the same statement or
//! transaction.

use protocol::api::{MemberRole, RoomKind};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

pub fn kind_from_db(kind: &str) -> RoomKind {
    match kind {
        "public" => RoomKind::Public,
        "private" => RoomKind::Private,
        "dm" => RoomKind::Dm,
        // The CHECK constraint makes anything else impossible.
        other => unreachable!("rooms.kind = {other:?} violates rooms_kind_valid"),
    }
}

pub fn kind_to_db(kind: RoomKind) -> &'static str {
    match kind {
        RoomKind::Public => "public",
        RoomKind::Private => "private",
        RoomKind::Dm => "dm",
    }
}

pub fn role_from_db(role: &str) -> MemberRole {
    match role {
        "owner" => MemberRole::Owner,
        "member" => MemberRole::Member,
        other => unreachable!("memberships.role = {other:?} violates memberships_role_valid"),
    }
}

fn role_to_db(role: MemberRole) -> &'static str {
    match role {
        MemberRole::Owner => "owner",
        MemberRole::Member => "member",
    }
}

/// What the server needs to authorize a request in a room, from one
/// primary-key lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    pub role: MemberRole,
    pub kind: RoomKind,
    pub message_expiry_at: Option<OffsetDateTime>,
    pub history_floor: i64,
}

/// `None` if the user isn't a member (or the room doesn't exist).
pub async fn membership(
    pool: &PgPool,
    room_id: Uuid,
    user_id: Uuid,
) -> Result<Option<Membership>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT m.role, r.kind, r.message_expiry_at, r.history_floor
        FROM memberships m JOIN rooms r ON r.id = m.room_id
        WHERE m.room_id = $1 AND m.user_id = $2
        "#,
        room_id,
        user_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| Membership {
        role: role_from_db(&r.role),
        kind: kind_from_db(&r.kind),
        message_expiry_at: r.message_expiry_at,
        history_floor: r.history_floor,
    }))
}

/// A room as one of its members sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserRoom {
    pub id: Uuid,
    pub kind: RoomKind,
    pub name: Option<String>,
    pub role: MemberRole,
    pub created_at: OffsetDateTime,
    /// For DMs, the other member's ID.
    pub dm_peer: Option<Uuid>,
    /// The member's read position (a message ID; 0 for none).
    pub last_read_id: i64,
    pub history_floor: i64,
}

pub async fn rooms_for_user(pool: &PgPool, user_id: Uuid) -> Result<Vec<UserRoom>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT r.id, r.kind, r.name, r.created_at, m.role, m.last_read_id, r.history_floor,
               (SELECT other.user_id FROM memberships other
                WHERE other.room_id = r.id AND other.user_id <> $1 AND r.kind = 'dm'
                LIMIT 1) AS dm_peer
        FROM memberships m JOIN rooms r ON r.id = m.room_id
        WHERE m.user_id = $1
        ORDER BY r.created_at
        "#,
        user_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| UserRoom {
            id: r.id,
            kind: kind_from_db(&r.kind),
            name: r.name,
            role: role_from_db(&r.role),
            created_at: r.created_at,
            dm_peer: r.dm_peer,
            last_read_id: r.last_read_id,
            history_floor: r.history_floor,
        })
        .collect())
}

/// One room, as `user_id` sees it. `None` if they aren't a member.
pub async fn room_for_user(
    pool: &PgPool,
    room_id: Uuid,
    user_id: Uuid,
) -> Result<Option<UserRoom>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT r.id, r.kind, r.name, r.created_at, m.role, m.last_read_id, r.history_floor,
               (SELECT other.user_id FROM memberships other
                WHERE other.room_id = r.id AND other.user_id <> $2 AND r.kind = 'dm'
                LIMIT 1) AS dm_peer
        FROM memberships m JOIN rooms r ON r.id = m.room_id
        WHERE m.room_id = $1 AND m.user_id = $2
        "#,
        room_id,
        user_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| UserRoom {
        id: r.id,
        kind: kind_from_db(&r.kind),
        name: r.name,
        role: role_from_db(&r.role),
        created_at: r.created_at,
        dm_peer: r.dm_peer,
        last_read_id: r.last_read_id,
        history_floor: r.history_floor,
    }))
}

/// The IDs of the rooms a user belongs to: what their sockets subscribe to.
pub async fn room_ids_for_user(pool: &PgPool, user_id: Uuid) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar!(
        "SELECT room_id FROM memberships WHERE user_id = $1",
        user_id
    )
    .fetch_all(pool)
    .await
}

pub async fn member_ids(pool: &PgPool, room_id: Uuid) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar!(
        "SELECT user_id FROM memberships WHERE room_id = $1",
        room_id
    )
    .fetch_all(pool)
    .await
}

/// How many rooms the user belongs to, for the membership cap.
pub async fn membership_count(pool: &PgPool, user_id: Uuid) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar!(
        r#"SELECT count(*) AS "count!" FROM memberships WHERE user_id = $1"#,
        user_id
    )
    .fetch_one(pool)
    .await
}

/// Creates a public or private room with `owner_id` as its owner, in one
/// transaction: a room never exists without its owner.
pub async fn create_room(
    pool: &PgPool,
    kind: RoomKind,
    name: &str,
    owner_id: Uuid,
) -> Result<UserRoom, sqlx::Error> {
    debug_assert!(
        kind != RoomKind::Dm,
        "DMs are created with find_or_create_dm"
    );
    let id = Uuid::now_v7();
    let mut tx = pool.begin().await?;
    let created_at = sqlx::query_scalar!(
        "INSERT INTO rooms (id, kind, name, created_by) VALUES ($1, $2, $3, $4) RETURNING created_at",
        id,
        kind_to_db(kind),
        name,
        owner_id,
    )
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query!(
        "INSERT INTO memberships (room_id, user_id, role) VALUES ($1, $2, $3)",
        id,
        owner_id,
        role_to_db(MemberRole::Owner),
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(UserRoom {
        id,
        kind,
        name: Some(name.to_string()),
        role: MemberRole::Owner,
        created_at,
        dm_peer: None,
        last_read_id: 0,
        history_floor: 0,
    })
}

/// Returns the DM between two users, creating it if it doesn't exist.
/// `created` is true when this call created it. Concurrent calls for the same
/// pair can't create two: the second insert hits the unique `dm_key`.
///
/// `message_expiry_at` applies only to a new DM: when one member is a guest,
/// the DM's messages expire with the guest (docs/design.md 11.3).
pub async fn find_or_create_dm(
    pool: &PgPool,
    a: Uuid,
    b: Uuid,
    message_expiry_at: Option<OffsetDateTime>,
) -> Result<(Uuid, bool), sqlx::Error> {
    debug_assert_ne!(a, b);
    let (low, high) = if a < b { (a, b) } else { (b, a) };
    let dm_key = format!("{low}:{high}");
    let mut tx = pool.begin().await?;
    let inserted = sqlx::query_scalar!(
        r#"
        INSERT INTO rooms (id, kind, dm_key, created_by, message_expiry_at)
        VALUES ($1, 'dm', $2, $3, $4)
        ON CONFLICT (dm_key) DO NOTHING
        RETURNING id
        "#,
        Uuid::now_v7(),
        dm_key,
        a,
        message_expiry_at,
    )
    .fetch_optional(&mut *tx)
    .await?;
    let (room_id, created) = match inserted {
        Some(id) => (id, true),
        None => (
            sqlx::query_scalar!("SELECT id FROM rooms WHERE dm_key = $1", dm_key)
                .fetch_one(&mut *tx)
                .await?,
            false,
        ),
    };
    // Both members, whether the room is new or not: idempotent.
    sqlx::query!(
        r#"
        INSERT INTO memberships (room_id, user_id, role)
        SELECT $1, unnest($2::uuid[]), 'member'
        ON CONFLICT DO NOTHING
        "#,
        room_id,
        &[a, b][..],
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok((room_id, created))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicRoomRow {
    pub id: Uuid,
    pub name: String,
    pub member_count: i64,
    pub joined: bool,
}

/// The public room directory, largest first. Bounded: this is a demo, not a
/// search engine.
pub async fn public_rooms(pool: &PgPool, user_id: Uuid) -> Result<Vec<PublicRoomRow>, sqlx::Error> {
    sqlx::query_as!(
        PublicRoomRow,
        r#"
        SELECT r.id, r.name AS "name!",
               count(m.user_id) AS "member_count!",
               bool_or(m.user_id = $1) IS TRUE AS "joined!"
        FROM rooms r LEFT JOIN memberships m ON m.room_id = r.id
        WHERE r.kind = 'public'
        GROUP BY r.id
        ORDER BY count(m.user_id) DESC, r.created_at
        LIMIT 100
        "#,
        user_id,
    )
    .fetch_all(pool)
    .await
}

/// The room's kind, if it exists.
pub async fn room_kind(pool: &PgPool, room_id: Uuid) -> Result<Option<RoomKind>, sqlx::Error> {
    let kind = sqlx::query_scalar!("SELECT kind FROM rooms WHERE id = $1", room_id)
        .fetch_optional(pool)
        .await?;
    Ok(kind.as_deref().map(kind_from_db))
}

/// Adds a member. Returns false if they already were one.
pub async fn add_member(pool: &PgPool, room_id: Uuid, user_id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query!(
        "INSERT INTO memberships (room_id, user_id, role) VALUES ($1, $2, 'member') ON CONFLICT DO NOTHING",
        room_id,
        user_id,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Removes a non-owner member. Returns false if there was no such member.
/// Owners are never removed this way (they delete the room instead), and
/// the `role = 'member'` condition makes that hold in the statement itself.
pub async fn remove_member(
    pool: &PgPool,
    room_id: Uuid,
    user_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query!(
        "DELETE FROM memberships WHERE room_id = $1 AND user_id = $2 AND role = 'member'",
        room_id,
        user_id,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Deletes a room and returns who was in it, so they can be told.
/// Memberships and invites go with it (ON DELETE CASCADE).
pub async fn delete_room(pool: &PgPool, room_id: Uuid) -> Result<Vec<Uuid>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let members = sqlx::query_scalar!(
        "SELECT user_id FROM memberships WHERE room_id = $1 FOR UPDATE",
        room_id
    )
    .fetch_all(&mut *tx)
    .await?;
    sqlx::query!("DELETE FROM rooms WHERE id = $1", room_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query!("DELETE FROM message_search WHERE room_id = $1", room_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(members)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRow {
    pub user_id: Uuid,
    pub handle: String,
    pub display_name: String,
    pub avatar_url: Option<String>,
    pub is_guest: bool,
    pub role: MemberRole,
    pub joined_at: OffsetDateTime,
}

/// Members, owner first, then by join time. Expired guests that the cleanup
/// job hasn't removed yet are left out (docs/design.md 11.3).
pub async fn members(pool: &PgPool, room_id: Uuid) -> Result<Vec<MemberRow>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT u.id, u.handle, u.display_name, u.avatar_url, u.is_guest, m.role, m.joined_at
        FROM memberships m JOIN users u ON u.id = m.user_id
        WHERE m.room_id = $1 AND (u.expires_at IS NULL OR u.expires_at > now())
        ORDER BY m.role = 'owner' DESC, m.joined_at
        LIMIT 1000
        "#,
        room_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| MemberRow {
            user_id: r.id,
            handle: r.handle,
            display_name: r.display_name,
            avatar_url: r.avatar_url,
            is_guest: r.is_guest,
            role: role_from_db(&r.role),
            joined_at: r.joined_at,
        })
        .collect())
}

pub async fn create_invite(
    pool: &PgPool,
    room_id: Uuid,
    created_by: Uuid,
    code: &str,
    expires_at: OffsetDateTime,
    max_uses: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "INSERT INTO invites (code, room_id, created_by, expires_at, max_uses) VALUES ($1, $2, $3, $4, $5)",
        code,
        room_id,
        created_by,
        expires_at,
        max_uses,
    )
    .execute(pool)
    .await
    .map(|_| ())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InviteOutcome {
    /// The user joined the room.
    Joined(Uuid),
    /// They were already a member; the invite wasn't used up.
    AlreadyMember(Uuid),
    /// Unknown, expired or used up. One answer for all three, so codes can't
    /// be probed for which state they're in.
    Invalid,
}

/// Uses an invite. The row is locked for the transaction, so two people
/// accepting the last use at once can't both get in.
pub async fn accept_invite(
    pool: &PgPool,
    code: &str,
    user_id: Uuid,
) -> Result<InviteOutcome, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let invite = sqlx::query!(
        r#"
        SELECT room_id, (expires_at > now() AND uses < max_uses) AS "usable!"
        FROM invites WHERE code = $1 FOR UPDATE
        "#,
        code,
    )
    .fetch_optional(&mut *tx)
    .await?;
    let Some(invite) = invite else {
        return Ok(InviteOutcome::Invalid);
    };
    if !invite.usable {
        return Ok(InviteOutcome::Invalid);
    }
    let inserted = sqlx::query!(
        "INSERT INTO memberships (room_id, user_id, role) VALUES ($1, $2, 'member') ON CONFLICT DO NOTHING",
        invite.room_id,
        user_id,
    )
    .execute(&mut *tx)
    .await?
    .rows_affected()
        == 1;
    if !inserted {
        tx.rollback().await?;
        return Ok(InviteOutcome::AlreadyMember(invite.room_id));
    }
    sqlx::query!("UPDATE invites SET uses = uses + 1 WHERE code = $1", code)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(InviteOutcome::Joined(invite.room_id))
}

/// Moves a read position forward. Returns false if it was already at or
/// past `message_id` (or the user isn't a member): read marks can arrive out
/// of order, and a position never moves back.
pub async fn mark_read(
    pool: &PgPool,
    room_id: Uuid,
    user_id: Uuid,
    message_id: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query!(
        r#"
        UPDATE memberships SET last_read_id = $3
        WHERE room_id = $1 AND user_id = $2 AND last_read_id < $3
        "#,
        room_id,
        user_id,
        message_id,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub struct ReceiptRow {
    pub count: i64,
    pub readers: Vec<MemberRow>,
}

/// Members other than `except` who have read at least up to `message_id`:
/// how many, and the 20 most recent. Both use the
/// `(room_id, last_read_id)` index, so the cost is bounded by the room's
/// member count whatever its message count.
pub async fn receipts(
    pool: &PgPool,
    room_id: Uuid,
    message_id: i64,
    except: Uuid,
) -> Result<ReceiptRow, sqlx::Error> {
    let count = sqlx::query_scalar!(
        r#"
        SELECT count(*) AS "count!" FROM memberships
        WHERE room_id = $1 AND last_read_id >= $2 AND user_id <> $3
        "#,
        room_id,
        message_id,
        except,
    )
    .fetch_one(pool)
    .await?;
    let rows = sqlx::query!(
        r#"
        SELECT u.id, u.handle, u.display_name, u.avatar_url, u.is_guest, m.role, m.joined_at
        FROM memberships m JOIN users u ON u.id = m.user_id
        WHERE m.room_id = $1 AND m.last_read_id >= $2 AND m.user_id <> $3
        ORDER BY m.last_read_id DESC
        LIMIT 20
        "#,
        room_id,
        message_id,
        except,
    )
    .fetch_all(pool)
    .await?;
    Ok(ReceiptRow {
        count,
        readers: rows
            .into_iter()
            .map(|r| MemberRow {
                user_id: r.id,
                handle: r.handle,
                display_name: r.display_name,
                avatar_url: r.avatar_url,
                is_guest: r.is_guest,
                role: role_from_db(&r.role),
                joined_at: r.joined_at,
            })
            .collect(),
    })
}

/// One member, as `member.joined` announces them.
pub async fn member(
    pool: &PgPool,
    room_id: Uuid,
    user_id: Uuid,
) -> Result<Option<MemberRow>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT u.id, u.handle, u.display_name, u.avatar_url, u.is_guest, m.role, m.joined_at
        FROM memberships m JOIN users u ON u.id = m.user_id
        WHERE m.room_id = $1 AND m.user_id = $2
        "#,
        room_id,
        user_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| MemberRow {
        user_id: r.id,
        handle: r.handle,
        display_name: r.display_name,
        avatar_url: r.avatar_url,
        is_guest: r.is_guest,
        role: role_from_db(&r.role),
        joined_at: r.joined_at,
    }))
}
