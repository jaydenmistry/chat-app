//! Creating sessions and recognising them on later requests.

use std::{net::IpAddr, time::Duration};

use axum::{
    extract::FromRequestParts,
    http::{HeaderValue, request::Parts},
};
use storage::{
    sessions::{self, LiveSession, NewSession, TokenHash},
    users::User,
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    auth::token,
    http::{cookies::Cookie, error::AppError},
    state::AppState,
};

/// Sliding sessions are extended at most this often, so a busy user doesn't
/// cost a Redis and a Postgres write per request.
const REFRESH_INTERVAL_SECS: i64 = 3600;
const USER_AGENT_MAX: usize = 256;

/// Signs `user` in: records the session in Postgres, stores the live copy in
/// Redis, and returns the `Set-Cookie` value.
pub async fn start(
    state: &AppState,
    user: &User,
    user_agent: Option<&str>,
    ip: IpAddr,
) -> Result<HeaderValue, AppError> {
    let now = OffsetDateTime::now_utc();
    // A guest session ends exactly when the guest account expires.
    let (expires_at, sliding) = match user.expires_at {
        Some(expires_at) => (expires_at, false),
        None => (now + state.config.session_ttl, true),
    };
    if expires_at <= now {
        return Err(AppError::Unauthorized("account expired"));
    }

    let (token, hash) = token::new_session_token();
    let user_agent = user_agent.map(|ua| truncate(ua, USER_AGENT_MAX));
    let session_id = sessions::insert(
        &state.pg,
        &NewSession {
            user_id: user.id,
            token_hash: &hash,
            expires_at,
            user_agent: user_agent.as_deref(),
            ip: Some(ip),
        },
    )
    .await?;

    let live = LiveSession {
        session_id,
        user_id: user.id,
        expires_at: expires_at.unix_timestamp(),
        sliding,
        refreshed_at: now.unix_timestamp(),
    };
    if let Err(err) = sessions::store_live(&mut state.redis.clone(), &hash, &live).await {
        // Don't leave a Postgres session that can never be used listed as
        // active. Best effort: the original error is what the caller sees.
        let _ = sessions::revoke(&state.pg, user.id, session_id).await;
        return Err(err.into());
    }

    let max_age = Duration::try_from(expires_at - now).unwrap_or_default();
    Ok(state.cookies.set(Cookie::Session, &token, max_age))
}

/// Ends a session in both stores. Deletes the Redis key first: that is what
/// stops the session working, and the Postgres row only records it.
pub async fn end(state: &AppState, session: &CurrentSession) -> Result<(), AppError> {
    sessions::delete_live(&mut state.redis.clone(), &session.token_hash).await?;
    sessions::revoke(&state.pg, session.user_id, session.session_id).await?;
    Ok(())
}

fn truncate(s: &str, max_chars: usize) -> String {
    s.chars().take(max_chars).collect()
}

/// The caller's session. Using this as a handler argument makes the route
/// require a signed-in user: a missing or unknown cookie is a 401.
#[derive(Debug, Clone)]
pub struct CurrentSession {
    pub session_id: Uuid,
    pub user_id: Uuid,
    pub token_hash: TokenHash,
}

impl FromRequestParts<AppState> for CurrentSession {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        const SIGN_IN: AppError = AppError::Unauthorized("sign in required");

        let token = state
            .cookies
            .get(&parts.headers, Cookie::Session)
            .ok_or(SIGN_IN)?;
        let hash = token::hash_session_token(token).ok_or(SIGN_IN)?;
        let live = sessions::load_live(&mut state.redis.clone(), &hash)
            .await?
            .ok_or(SIGN_IN)?;

        let now = OffsetDateTime::now_utc().unix_timestamp();
        // Redis expires the key at `expires_at`; this covers the second or so
        // between the deadline and Redis acting on it.
        if live.expires_at <= now {
            return Err(SIGN_IN);
        }
        if live.sliding && now - live.refreshed_at >= REFRESH_INTERVAL_SECS {
            // Off the request path: the request doesn't wait for, or fail
            // because of, the refresh.
            tokio::spawn(refresh(state.clone(), hash, live.session_id));
        }
        Ok(Self {
            session_id: live.session_id,
            user_id: live.user_id,
            token_hash: hash,
        })
    }
}

async fn refresh(state: AppState, hash: TokenHash, session_id: Uuid) {
    let now = OffsetDateTime::now_utc();
    let expires_at = now + state.config.session_ttl;
    let result = async {
        let extended = sessions::extend_live(
            &mut state.redis.clone(),
            &hash,
            expires_at.unix_timestamp(),
            now.unix_timestamp(),
        )
        .await?;
        if extended {
            sessions::touch(&state.pg, session_id, expires_at).await?;
        }
        anyhow::Ok(())
    }
    .await;
    if let Err(err) = result {
        tracing::warn!(error = format!("{err:#}"), %session_id, "session refresh failed");
    }
}
