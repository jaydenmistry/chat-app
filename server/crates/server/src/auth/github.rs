//! Sign in with GitHub: OAuth 2.0 authorization code flow with PKCE and state.
//!
//! 1. `start` creates a random `state`, a PKCE verifier and a browser nonce.
//!    It stores `oauth:<state> = <verifier>.<nonce>` in Redis for 10 minutes,
//!    and the caller sets the nonce in a cookie and redirects to GitHub.
//! 2. GitHub redirects back with `code` and `state`. `finish` takes the
//!    pending login out of Redis (single use), checks the cookie nonce,
//!    exchanges the code with the client secret and the verifier, and fetches
//!    the profile.
//!
//! The nonce check stops login CSRF: without it, an attacker could make your
//! browser complete *their* GitHub sign-in and use the app as them.

use rand::RngExt;
use serde::Deserialize;
use storage::users::{self, CreateUserError, GithubUser, User};

use crate::{
    auth::{token, validate},
    config::GithubConfig,
    http::error::AppError,
    state::AppState,
};

const PENDING_TTL_SECS: u64 = 600;

pub struct Started {
    pub authorize_url: String,
    pub nonce: String,
}

#[derive(Debug, thiserror::Error)]
pub enum GithubError {
    #[error("unknown or expired state")]
    UnknownState,
    #[error("nonce cookie missing or mismatched")]
    NonceMismatch,
    #[error("github returned an error: {0}")]
    Github(String),
    #[error("talking to github failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    App(#[from] AppError),
}

impl From<redis::RedisError> for GithubError {
    fn from(err: redis::RedisError) -> Self {
        Self::App(err.into())
    }
}

impl From<sqlx::Error> for GithubError {
    fn from(err: sqlx::Error) -> Self {
        Self::App(err.into())
    }
}

fn pending_key(oauth_state: &str) -> String {
    format!("oauth:{oauth_state}")
}

pub async fn start(state: &AppState, github: &GithubConfig) -> Result<Started, AppError> {
    let oauth_state = token::random_string(32);
    // 32 random bytes give a 43-character verifier, within PKCE's 43 to 128.
    let verifier = token::random_string(32);
    let nonce = token::random_string(32);
    let challenge = pkce_challenge(&verifier);

    storage::metered::redis(
        "github.store_state",
        redis::cmd("SET")
            .arg(pending_key(&oauth_state))
            // base64url never contains '.', so it is a safe separator.
            .arg(format!("{verifier}.{nonce}"))
            .arg("EX")
            .arg(PENDING_TTL_SECS)
            .query_async::<()>(&mut state.redis.clone()),
    )
    .await?;

    let authorize_url = reqwest::Url::parse_with_params(
        &format!("{}/login/oauth/authorize", github.oauth_base_url),
        &[
            ("client_id", github.client_id.as_str()),
            ("redirect_uri", &state.config.github_redirect_url()),
            ("state", &oauth_state),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
            // No scopes: the public profile is all we need.
            ("scope", ""),
        ],
    )
    .map_err(AppError::internal)?
    .to_string();

    Ok(Started {
        authorize_url,
        nonce,
    })
}

/// PKCE S256: base64url(SHA-256(verifier)).
pub fn pkce_challenge(verifier: &str) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD.encode(token::sha256(verifier.as_bytes()))
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct Profile {
    id: i64,
    login: String,
    name: Option<String>,
    avatar_url: Option<String>,
}

pub async fn finish(
    state: &AppState,
    github: &GithubConfig,
    code: &str,
    oauth_state: &str,
    cookie_nonce: Option<&str>,
) -> Result<User, GithubError> {
    // GETDEL makes each state usable exactly once.
    let pending: Option<String> = storage::metered::redis(
        "github.take_state",
        redis::cmd("GETDEL")
            .arg(pending_key(oauth_state))
            .query_async(&mut state.redis.clone()),
    )
    .await?;
    let (verifier, nonce) = pending
        .as_deref()
        .and_then(|value| value.split_once('.'))
        .ok_or(GithubError::UnknownState)?;
    let nonce_matches = cookie_nonce
        .is_some_and(|cookie| token::constant_time_eq(cookie.as_bytes(), nonce.as_bytes()));
    if !nonce_matches {
        return Err(GithubError::NonceMismatch);
    }

    let redirect_uri = state.config.github_redirect_url();
    let token: TokenResponse = state
        .http
        .post(format!(
            "{}/login/oauth/access_token",
            github.oauth_base_url
        ))
        .header(reqwest::header::ACCEPT, "application/json")
        .form(&[
            ("client_id", github.client_id.as_str()),
            ("client_secret", github.client_secret.expose()),
            ("code", code),
            ("redirect_uri", &redirect_uri),
            ("code_verifier", verifier),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let access_token = match token {
        TokenResponse {
            access_token: Some(access_token),
            ..
        } => access_token,
        TokenResponse { error, .. } => {
            return Err(GithubError::Github(
                error.unwrap_or_else(|| "no access token".into()),
            ));
        }
    };

    let profile: Profile = state
        .http
        .get(format!("{}/user", github.api_base_url))
        .bearer_auth(&access_token)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    find_or_create_user(state, &profile).await
}

async fn find_or_create_user(state: &AppState, profile: &Profile) -> Result<User, GithubError> {
    let avatar = profile.avatar_url.as_deref();
    if let Some(user) = users::refresh_github_user(&state.pg, profile.id, avatar).await? {
        return Ok(user);
    }

    let base = handle_from_login(&profile.login);
    let display_name = display_name_from_profile(profile);
    for attempt in 0..5 {
        let handle = if attempt == 0 {
            base.clone()
        } else {
            with_random_suffix(&base)
        };
        let new = GithubUser {
            github_id: profile.id,
            handle: &handle,
            display_name: &display_name,
            avatar_url: avatar,
        };
        match users::create_github_user(&state.pg, &new).await {
            Ok(user) => return Ok(user),
            Err(CreateUserError::HandleTaken) => continue,
            // A concurrent first sign-in with the same GitHub account won the
            // race; use the account it created.
            Err(CreateUserError::GithubIdTaken) => {
                if let Some(user) =
                    users::refresh_github_user(&state.pg, profile.id, avatar).await?
                {
                    return Ok(user);
                }
            }
            Err(CreateUserError::EmailTaken) => {
                unreachable!("GitHub accounts are created without an email")
            }
            Err(CreateUserError::Db(err)) => return Err(err.into()),
        }
    }
    Err(AppError::internal(anyhow::anyhow!(
        "no free handle for github login {}",
        profile.login
    ))
    .into())
}

/// Turns a GitHub login (letters, digits, single hyphens, up to 39 chars) into
/// a valid handle: lowercase, hyphens to underscores, at most 32 characters,
/// at least 3, never in the reserved guest namespace.
pub fn handle_from_login(login: &str) -> String {
    let mut handle: String = login
        .to_lowercase()
        .chars()
        .map(|c| if c == '-' { '_' } else { c })
        .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_')
        .collect();
    if handle.starts_with(validate::GUEST_PREFIX) {
        handle.insert_str(0, "gh_");
    }
    handle.truncate(validate::HANDLE_MAX);
    if handle.len() < validate::HANDLE_MIN {
        handle.push_str("_gh");
    }
    debug_assert!(validate::is_valid_handle(&handle));
    handle
}

/// `base` shortened to leave room for `_` and four random characters.
fn with_random_suffix(base: &str) -> String {
    let keep = base.len().min(validate::HANDLE_MAX - 5);
    format!("{}_{}", &base[..keep], random_alphanumeric(4))
}

/// Lowercase letters and digits. The modulo bias (256 is not a multiple of
/// 36) doesn't matter for collision-avoidance suffixes.
pub fn random_alphanumeric(len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..len)
        .map(|_| ALPHABET[usize::from(rng.random::<u8>()) % ALPHABET.len()] as char)
        .collect()
}

fn display_name_from_profile(profile: &Profile) -> String {
    profile
        .name
        .as_deref()
        .and_then(|name| validate::display_name(name).ok())
        .unwrap_or_else(|| profile.login.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_from_github_logins() {
        assert_eq!(handle_from_login("Octo-Cat"), "octo_cat");
        assert_eq!(handle_from_login("ab"), "ab_gh");
        assert_eq!(handle_from_login("guest_star"), "gh_guest_star");
        let long = "a".repeat(39);
        assert_eq!(handle_from_login(&long), "a".repeat(32));
        for login in ["x", "Octo-Cat", &long, "guest_star", "UPPER-lower-123"] {
            assert!(
                validate::is_valid_handle(&handle_from_login(login)),
                "{login}"
            );
        }
    }

    #[test]
    fn suffixed_handles_stay_valid() {
        let base = "a".repeat(32);
        let suffixed = with_random_suffix(&base);
        assert_eq!(suffixed.len(), 32);
        assert!(validate::is_valid_handle(&suffixed));
        assert!(validate::is_valid_handle(&with_random_suffix("abc")));
    }

    /// The example from RFC 7636, appendix B.
    #[test]
    fn pkce_challenge_matches_rfc_example() {
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }
}
