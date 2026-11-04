//! Account endpoints under `/api/auth`, `/api/me` and `/api/sessions`.

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{LOCATION, SET_COOKIE, USER_AGENT},
    },
    response::{AppendHeaders, IntoResponse, Response},
};
use protocol::api::{AuthConfig, LoginRequest, Me, SessionInfo, SignupRequest};
use serde::Deserialize;
use std::time::Duration;
use storage::{
    sessions,
    users::{self, CreateUserError, NewPasswordUser, User},
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    auth::{
        github,
        session::{self, CurrentSession},
        validate,
    },
    http::{
        client_ip::ClientIp,
        cookies,
        error::{ApiJson, AppError},
    },
    state::AppState,
};

const WRONG_CREDENTIALS: AppError = AppError::Unauthorized("wrong email or password");

pub fn me(user: &User) -> Me {
    Me {
        id: user.id,
        handle: user.handle.clone(),
        display_name: user.display_name.clone(),
        avatar_url: user.avatar_url.clone(),
        is_guest: user.is_guest,
        expires_at: user.expires_at,
    }
}

fn user_agent(headers: &HeaderMap) -> Option<&str> {
    headers.get(USER_AGENT).and_then(|v| v.to_str().ok())
}

/// The response for every successful sign-in: the user, plus the cookie.
fn signed_in(status: StatusCode, cookie: HeaderValue, user: &User) -> Response {
    (status, [(SET_COOKIE, cookie)], Json(me(user))).into_response()
}

pub async fn config(State(state): State<AppState>) -> Json<AuthConfig> {
    Json(AuthConfig {
        github: state.config.github.is_some(),
    })
}

pub async fn signup(
    State(state): State<AppState>,
    ClientIp(ip): ClientIp,
    headers: HeaderMap,
    ApiJson(req): ApiJson<SignupRequest>,
) -> Result<Response, AppError> {
    let email = validate::email(&req.email)?;
    validate::password(&req.password)?;
    let handle = validate::handle(&req.handle)?;
    let display_name = validate::display_name(&req.display_name)?;

    let password_hash = state.passwords.hash(req.password).await?;
    let user = users::create_password_user(
        &state.pg,
        &NewPasswordUser {
            handle: &handle,
            display_name: &display_name,
            email: &email,
            password_hash: &password_hash,
        },
    )
    .await
    .map_err(|err| match err {
        // Without email verification there is no way to hide that an email
        // is registered at sign-up. Rate limits (Phase 4) bound how fast
        // anyone can probe.
        CreateUserError::EmailTaken => AppError::Conflict {
            field: "email",
            message: "an account with this email already exists",
        },
        CreateUserError::HandleTaken => AppError::Conflict {
            field: "handle",
            message: "this handle is taken",
        },
        CreateUserError::GithubIdTaken => AppError::internal(err),
        CreateUserError::Db(err) => err.into(),
    })?;

    let cookie = session::start(&state, &user, user_agent(&headers), ip).await?;
    tracing::info!(user_id = %user.id, "account created");
    Ok(signed_in(StatusCode::CREATED, cookie, &user))
}

pub async fn login(
    State(state): State<AppState>,
    ClientIp(ip): ClientIp,
    headers: HeaderMap,
    ApiJson(req): ApiJson<LoginRequest>,
) -> Result<Response, AppError> {
    // A malformed email can't match an account; answer exactly as for a
    // wrong password.
    let Ok(email) = validate::email(&req.email) else {
        return Err(WRONG_CREDENTIALS);
    };
    let record = users::find_for_login(&state.pg, &email).await?;
    let stored_hash = record.as_ref().and_then(|r| r.password_hash.clone());
    // Always runs one Argon2 verification, so the response time doesn't
    // reveal whether the email exists.
    let password_ok = state.passwords.verify(req.password, stored_hash).await?;
    let user = match record {
        Some(record) if password_ok => record.user,
        _ => return Err(WRONG_CREDENTIALS),
    };
    if user.banned_at.is_some() {
        return Err(AppError::Forbidden("this account is suspended"));
    }
    let cookie = session::start(&state, &user, user_agent(&headers), ip).await?;
    Ok(signed_in(StatusCode::OK, cookie, &user))
}

pub async fn guest(
    State(state): State<AppState>,
    ClientIp(ip): ClientIp,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let expires_at = OffsetDateTime::now_utc() + state.config.guest_ttl;
    // 36^6 (about 2 billion) suffixes make a collision rare; retry if one
    // happens anyway.
    let mut created = None;
    for _ in 0..3 {
        let suffix = github::random_alphanumeric(6);
        let handle = format!("{}{suffix}", validate::GUEST_PREFIX);
        let display_name = format!("Guest {suffix}");
        match users::create_guest(&state.pg, &handle, &display_name, expires_at).await {
            Ok(user) => {
                created = Some(user);
                break;
            }
            Err(CreateUserError::HandleTaken) => continue,
            Err(CreateUserError::Db(err)) => return Err(err.into()),
            Err(other) => return Err(AppError::internal(other)),
        }
    }
    let user =
        created.ok_or_else(|| AppError::internal(anyhow::anyhow!("no free guest handle")))?;
    let cookie = session::start(&state, &user, user_agent(&headers), ip).await?;
    Ok(signed_in(StatusCode::CREATED, cookie, &user))
}

/// Always clears the cookie. A request without a valid session is still a
/// successful logout; a dependency failure is not.
pub async fn logout(
    State(state): State<AppState>,
    session: Result<CurrentSession, AppError>,
) -> Result<Response, AppError> {
    match session {
        Ok(session) => session::end(&state, &session).await?,
        Err(AppError::Unauthorized(_)) => {}
        Err(other) => return Err(other),
    }
    Ok((
        StatusCode::NO_CONTENT,
        [(SET_COOKIE, cookies::clear(cookies::SESSION))],
    )
        .into_response())
}

pub async fn current_user(
    State(state): State<AppState>,
    session: CurrentSession,
) -> Result<Json<Me>, AppError> {
    let user = users::find_by_id(&state.pg, session.user_id)
        .await?
        .ok_or(AppError::Unauthorized("sign in required"))?;
    Ok(Json(me(&user)))
}

pub async fn list_sessions(
    State(state): State<AppState>,
    session: CurrentSession,
) -> Result<Json<Vec<SessionInfo>>, AppError> {
    let rows = sessions::list_active(&state.pg, session.user_id).await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| SessionInfo {
                current: row.id == session.session_id,
                id: row.id,
                created_at: row.created_at,
                last_seen_at: row.last_seen_at,
                user_agent: row.user_agent,
                ip: row.ip.map(|ip| ip.to_string()),
            })
            .collect(),
    ))
}

/// Revokes one of the caller's sessions. Another user's session ID gives the
/// same 404 as a nonexistent one.
pub async fn revoke_session(
    State(state): State<AppState>,
    session: CurrentSession,
    Path(session_id): Path<Uuid>,
) -> Result<Response, AppError> {
    let hash = sessions::revoke(&state.pg, session.user_id, session_id)
        .await?
        .ok_or(AppError::NotFound)?;
    sessions::delete_live(&mut state.redis.clone(), &hash).await?;
    if session_id == session.session_id {
        Ok((
            StatusCode::NO_CONTENT,
            [(SET_COOKIE, cookies::clear(cookies::SESSION))],
        )
            .into_response())
    } else {
        Ok(StatusCode::NO_CONTENT.into_response())
    }
}

pub async fn github_start(State(state): State<AppState>) -> Result<Response, AppError> {
    let github = state.config.github.as_ref().ok_or(AppError::NotFound)?;
    let started = github::start(&state, github).await?;
    let nonce_cookie = cookies::set(
        cookies::OAUTH_NONCE,
        &started.nonce,
        Duration::from_secs(600),
    );
    let location = HeaderValue::from_str(&started.authorize_url).map_err(AppError::internal)?;
    Ok((
        StatusCode::SEE_OTHER,
        AppendHeaders([(LOCATION, location), (SET_COOKIE, nonce_cookie)]),
    )
        .into_response())
}

#[derive(Deserialize)]
pub struct CallbackParams {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

/// GitHub redirects the browser here, so failures redirect to the app with a
/// flag instead of returning JSON. The reason is logged, not shown.
pub async fn github_callback(
    State(state): State<AppState>,
    ClientIp(ip): ClientIp,
    headers: HeaderMap,
    Query(params): Query<CallbackParams>,
) -> Result<Response, AppError> {
    let github = state.config.github.as_ref().ok_or(AppError::NotFound)?;
    let clear_nonce = (SET_COOKIE, cookies::clear(cookies::OAUTH_NONCE));
    let failed = |reason: &str| {
        tracing::warn!(reason, "github sign-in failed");
        (
            StatusCode::SEE_OTHER,
            AppendHeaders([
                (LOCATION, HeaderValue::from_static("/?auth_error=github")),
                clear_nonce.clone(),
            ]),
        )
            .into_response()
    };

    let (Some(code), Some(oauth_state)) = (params.code, params.state) else {
        // The user pressed "Cancel" on GitHub, or the URL was tampered with.
        return Ok(failed(
            params.error.as_deref().unwrap_or("missing code or state"),
        ));
    };
    let cookie_nonce = cookies::get(&headers, cookies::OAUTH_NONCE);
    let user = match github::finish(&state, github, &code, &oauth_state, cookie_nonce).await {
        Ok(user) => user,
        Err(github::GithubError::App(err @ AppError::Unavailable(_))) => return Err(err),
        Err(err) => return Ok(failed(&err.to_string())),
    };
    if user.banned_at.is_some() {
        return Ok(failed("account suspended"));
    }

    let session_cookie = session::start(&state, &user, user_agent(&headers), ip).await?;
    Ok((
        StatusCode::SEE_OTHER,
        AppendHeaders([
            (LOCATION, HeaderValue::from_static("/")),
            (SET_COOKIE, session_cookie),
            clear_nonce,
        ]),
    )
        .into_response())
}
