//! Accounts (Postgres `users`).
//!
//! Callers pass already-validated, normalized values (lowercase handle and
//! email). The table's CHECK constraints are the last line of defence, not the
//! validation layer.

use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

/// The system account that owns the demo rooms and authors their seed
/// messages (docs/design.md 11.4). Seeded by the `demo_rooms` migration; this
/// constant must match the ID there.
pub const SYSTEM_USER_ID: Uuid = Uuid::from_u128(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub id: Uuid,
    pub handle: String,
    pub display_name: String,
    pub avatar_url: Option<String>,
    pub is_guest: bool,
    pub expires_at: Option<OffsetDateTime>,
    pub banned_at: Option<OffsetDateTime>,
}

#[derive(Debug, thiserror::Error)]
pub enum CreateUserError {
    #[error("email already registered")]
    EmailTaken,
    #[error("handle already taken")]
    HandleTaken,
    #[error("github account already linked")]
    GithubIdTaken,
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

/// Turns a unique-index violation into the matching variant. The index names
/// come from the migration.
fn classify(err: sqlx::Error) -> CreateUserError {
    let constraint = err.as_database_error().and_then(|e| e.constraint());
    match constraint {
        Some("users_email_key") => CreateUserError::EmailTaken,
        Some("users_handle_key") => CreateUserError::HandleTaken,
        Some("users_github_id_key") => CreateUserError::GithubIdTaken,
        _ => CreateUserError::Db(err),
    }
}

pub struct NewPasswordUser<'a> {
    pub handle: &'a str,
    pub display_name: &'a str,
    pub email: &'a str,
    pub password_hash: &'a str,
}

pub async fn create_password_user(
    pool: &PgPool,
    new: &NewPasswordUser<'_>,
) -> Result<User, CreateUserError> {
    sqlx::query_as!(
        User,
        r#"
        INSERT INTO users (id, handle, display_name, email, password_hash)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, handle, display_name, avatar_url, is_guest, expires_at, banned_at
        "#,
        Uuid::now_v7(),
        new.handle,
        new.display_name,
        new.email,
        new.password_hash,
    )
    .fetch_one(pool)
    .await
    .map_err(classify)
}

pub async fn create_guest(
    pool: &PgPool,
    handle: &str,
    display_name: &str,
    expires_at: OffsetDateTime,
) -> Result<User, CreateUserError> {
    sqlx::query_as!(
        User,
        r#"
        INSERT INTO users (id, handle, display_name, is_guest, expires_at)
        VALUES ($1, $2, $3, true, $4)
        RETURNING id, handle, display_name, avatar_url, is_guest, expires_at, banned_at
        "#,
        Uuid::now_v7(),
        handle,
        display_name,
        expires_at,
    )
    .fetch_one(pool)
    .await
    .map_err(classify)
}

pub struct GithubUser<'a> {
    pub github_id: i64,
    pub handle: &'a str,
    pub display_name: &'a str,
    pub avatar_url: Option<&'a str>,
}

pub async fn create_github_user(
    pool: &PgPool,
    new: &GithubUser<'_>,
) -> Result<User, CreateUserError> {
    sqlx::query_as!(
        User,
        r#"
        INSERT INTO users (id, handle, display_name, github_id, avatar_url)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, handle, display_name, avatar_url, is_guest, expires_at, banned_at
        "#,
        Uuid::now_v7(),
        new.handle,
        new.display_name,
        new.github_id,
        new.avatar_url,
    )
    .fetch_one(pool)
    .await
    .map_err(classify)
}

/// Finds the account linked to a GitHub user and refreshes its avatar, which
/// the user may have changed on GitHub.
pub async fn refresh_github_user(
    pool: &PgPool,
    github_id: i64,
    avatar_url: Option<&str>,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as!(
        User,
        r#"
        UPDATE users SET avatar_url = $2
        WHERE github_id = $1
        RETURNING id, handle, display_name, avatar_url, is_guest, expires_at, banned_at
        "#,
        github_id,
        avatar_url,
    )
    .fetch_optional(pool)
    .await
}

pub struct LoginRecord {
    pub user: User,
    pub password_hash: Option<String>,
}

pub async fn find_for_login(
    pool: &PgPool,
    email: &str,
) -> Result<Option<LoginRecord>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT id, handle, display_name, avatar_url, is_guest, expires_at, banned_at, password_hash
        FROM users WHERE email = $1
        "#,
        email,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| LoginRecord {
        user: User {
            id: r.id,
            handle: r.handle,
            display_name: r.display_name,
            avatar_url: r.avatar_url,
            is_guest: r.is_guest,
            expires_at: r.expires_at,
            banned_at: r.banned_at,
        },
        password_hash: r.password_hash,
    }))
}

pub async fn find_by_id(pool: &PgPool, id: Uuid) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as!(
        User,
        r#"
        SELECT id, handle, display_name, avatar_url, is_guest, expires_at, banned_at
        FROM users WHERE id = $1
        "#,
        id,
    )
    .fetch_optional(pool)
    .await
}

/// Live users only: an expired guest the cleanup job hasn't deleted yet is
/// treated as gone.
pub async fn find_by_handle(pool: &PgPool, handle: &str) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as!(
        User,
        r#"
        SELECT id, handle, display_name, avatar_url, is_guest, expires_at, banned_at
        FROM users WHERE handle = $1 AND (expires_at IS NULL OR expires_at > now())
        "#,
        handle,
    )
    .fetch_optional(pool)
    .await
}

/// Suspends an account: it can no longer sign in (checked at login) and its
/// sessions are ended separately (docs/design.md 11.4). Idempotent; re-banning
/// just moves the timestamp. Returns false if there is no such user.
pub async fn ban(pool: &PgPool, user_id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query!("UPDATE users SET banned_at = now() WHERE id = $1", user_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

/// An expired guest the cleanup job removed, with the rooms they were in at
/// the time (collected before the delete, so clients can be told).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiredGuest {
    pub user_id: Uuid,
    pub rooms: Vec<Uuid>,
}

/// Deletes up to `limit` expired guests and their DM rooms, returning each
/// guest and the rooms they were in (docs/design.md 11.3). Memberships and
/// sessions go by cascade; a DM the guest was in is deleted outright, since a
/// one-sided DM is meaningless. Runs in one transaction, so a guest is either
/// fully gone or untouched.
pub async fn delete_expired_guests(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ExpiredGuest>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let ids: Vec<Uuid> = sqlx::query_scalar!(
        r#"
        SELECT id FROM users
        WHERE is_guest AND expires_at <= now()
        ORDER BY expires_at
        LIMIT $1
        "#,
        limit,
    )
    .fetch_all(&mut *tx)
    .await?;

    let mut expired = Vec::with_capacity(ids.len());
    for user_id in ids {
        let rooms: Vec<Uuid> = sqlx::query_scalar!(
            "SELECT room_id FROM memberships WHERE user_id = $1",
            user_id,
        )
        .fetch_all(&mut *tx)
        .await?;
        // Drop the DM rooms the guest was in (cascading their memberships);
        // their public and private memberships go when the user is deleted.
        sqlx::query!(
            r#"
            DELETE FROM rooms
            WHERE kind = 'dm'
              AND id IN (SELECT room_id FROM memberships WHERE user_id = $1)
            "#,
            user_id,
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!("DELETE FROM users WHERE id = $1", user_id)
            .execute(&mut *tx)
            .await?;
        expired.push(ExpiredGuest { user_id, rooms });
    }

    tx.commit().await?;
    Ok(expired)
}

/// Profiles for message authors and DM peers, at most 200 per call.
pub async fn find_many(pool: &PgPool, ids: &[Uuid]) -> Result<Vec<User>, sqlx::Error> {
    sqlx::query_as!(
        User,
        r#"
        SELECT id, handle, display_name, avatar_url, is_guest, expires_at, banned_at
        FROM users WHERE id = ANY($1)
        LIMIT 200
        "#,
        ids,
    )
    .fetch_all(pool)
    .await
}
