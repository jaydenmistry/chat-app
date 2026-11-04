//! GitHub sign-in against a fake GitHub that enforces PKCE: its token endpoint
//! accepts a code only with the verifier whose challenge was sent to the
//! authorize URL.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{
    Form, Json, Router,
    extract::State,
    http::HeaderMap,
    routing::{get, post},
};
use protocol::api::{AuthConfig, Me};
use reqwest::{StatusCode, Url, header};
use serde_json::{Value, json};
use server::config::{GithubConfig, Secret};

use crate::{
    app::{TestApp, session_cookie, set_cookie},
    infra::Infra,
};

const CLIENT_ID: &str = "test-client";
const CLIENT_SECRET: &str = "test-secret";
const ACCESS_TOKEN: &str = "gho_test_token";

#[derive(Default)]
struct FakeGithub {
    /// code -> the PKCE challenge it was issued for
    codes: Mutex<HashMap<String, String>>,
    profile: Mutex<Value>,
}

async fn token(
    State(fake): State<Arc<FakeGithub>>,
    Form(form): Form<HashMap<String, String>>,
) -> Json<Value> {
    let get = |key: &str| form.get(key).map(String::as_str).unwrap_or_default();
    let challenge = fake.codes.lock().unwrap().remove(get("code"));
    let valid = get("client_id") == CLIENT_ID
        && get("client_secret") == CLIENT_SECRET
        && get("redirect_uri") == "https://app.test/api/auth/github/callback"
        && challenge
            .is_some_and(|c| c == server::auth::github::pkce_challenge(get("code_verifier")));
    Json(if valid {
        json!({"access_token": ACCESS_TOKEN, "token_type": "bearer", "scope": ""})
    } else {
        // GitHub reports token errors with a 200 and an `error` field.
        json!({"error": "bad_verification_code"})
    })
}

async fn user(
    State(fake): State<Arc<FakeGithub>>,
    headers: HeaderMap,
) -> Result<Json<Value>, StatusCode> {
    let expected = format!("Bearer {ACCESS_TOKEN}");
    if headers
        .get(header::AUTHORIZATION)
        .is_some_and(|v| v == expected.as_str())
    {
        Ok(Json(fake.profile.lock().unwrap().clone()))
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// Starts the fake and a server configured to use it.
async fn setup(infra: &Infra) -> (TestApp, Arc<FakeGithub>) {
    let fake = Arc::new(FakeGithub::default());
    *fake.profile.lock().unwrap() = json!({
        "id": 4242, "login": "Octo-Cat", "name": "Octo Cat", "avatar_url": "https://example.com/a.png",
    });
    let router = Router::new()
        .route("/login/oauth/access_token", post(token))
        .route("/user", get(user))
        .with_state(Arc::clone(&fake));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await });

    let app = TestApp::spawn_with(infra, |config| {
        config.github = Some(GithubConfig {
            client_id: CLIENT_ID.into(),
            client_secret: Secret::new(CLIENT_SECRET),
            oauth_base_url: base.clone(),
            api_base_url: base.clone(),
        });
    })
    .await;
    (app, fake)
}

struct Started {
    state: String,
    challenge: String,
    nonce_cookie: String,
}

/// Plays the browser's first step: GET /start, follow nothing, read what we
/// would send to GitHub.
async fn start(app: &TestApp) -> Started {
    let resp = app.get("/api/auth/github/start").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    let nonce = set_cookie(&resp, "__Host-oauth").expect("nonce cookie");
    assert!(
        nonce.contains("HttpOnly") && nonce.contains("Secure") && nonce.contains("SameSite=Lax")
    );
    let location = Url::parse(resp.headers()[header::LOCATION].to_str().unwrap()).unwrap();
    assert_eq!(location.path(), "/login/oauth/authorize");
    let query: HashMap<_, _> = location.query_pairs().into_owned().collect();
    assert_eq!(query["client_id"], CLIENT_ID);
    assert_eq!(query["code_challenge_method"], "S256");
    assert_eq!(
        query["redirect_uri"],
        "https://app.test/api/auth/github/callback"
    );
    Started {
        state: query["state"].clone(),
        challenge: query["code_challenge"].clone(),
        nonce_cookie: nonce.split(';').next().unwrap().to_string(),
    }
}

async fn callback(
    app: &TestApp,
    code: &str,
    state: &str,
    cookie: Option<&str>,
) -> reqwest::Response {
    let url = Url::parse_with_params(
        &app.url("/api/auth/github/callback"),
        &[("code", code), ("state", state)],
    )
    .unwrap();
    let mut request = app.client.get(url);
    if let Some(cookie) = cookie {
        request = request.header(header::COOKIE, cookie);
    }
    request.send().await.unwrap()
}

fn assert_failed(resp: &reqwest::Response) {
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert_eq!(resp.headers()[header::LOCATION], "/?auth_error=github");
    assert!(
        session_cookie(resp).is_none(),
        "a failed sign-in must not create a session"
    );
}

pub async fn github_sign_in_with_pkce(infra: Arc<Infra>) {
    let (app, fake) = setup(&infra).await;

    let started = start(&app).await;
    // GitHub's authorize page: the user approves, GitHub issues a code bound
    // to the challenge.
    fake.codes
        .lock()
        .unwrap()
        .insert("code-1".into(), started.challenge.clone());
    let resp = callback(&app, "code-1", &started.state, Some(&started.nonce_cookie)).await;
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert_eq!(resp.headers()[header::LOCATION], "/");
    let cleared = set_cookie(&resp, "__Host-oauth").unwrap();
    assert!(cleared.contains("Max-Age=0"));
    let cookie = session_cookie(&resp).expect("session cookie");

    let me: Me = app.me(&cookie).await.unwrap();
    assert_eq!(me.handle, "octo_cat");
    assert_eq!(me.display_name, "Octo Cat");
    assert_eq!(me.avatar_url.as_deref(), Some("https://example.com/a.png"));

    // The state is single use: replaying the callback fails.
    fake.codes
        .lock()
        .unwrap()
        .insert("code-2".into(), started.challenge);
    let resp = callback(&app, "code-2", &started.state, Some(&started.nonce_cookie)).await;
    assert_failed(&resp);

    // Signing in again finds the same account and picks up a new avatar.
    fake.profile.lock().unwrap()["avatar_url"] = json!("https://example.com/b.png");
    let again = start(&app).await;
    fake.codes
        .lock()
        .unwrap()
        .insert("code-3".into(), again.challenge);
    let resp = callback(&app, "code-3", &again.state, Some(&again.nonce_cookie)).await;
    let cookie = session_cookie(&resp).unwrap();
    let me_again = app.me(&cookie).await.unwrap();
    assert_eq!(me_again.id, me.id);
    assert_eq!(
        me_again.avatar_url.as_deref(),
        Some("https://example.com/b.png")
    );
}

pub async fn github_callback_rejects_bad_state_and_nonce(infra: Arc<Infra>) {
    let (app, fake) = setup(&infra).await;

    // A state we never issued.
    let started = start(&app).await;
    fake.codes
        .lock()
        .unwrap()
        .insert("code-a".into(), started.challenge.clone());
    let resp = callback(&app, "code-a", "made-up-state", Some(&started.nonce_cookie)).await;
    assert_failed(&resp);

    // The right state but no nonce cookie: an attacker's link opened in the
    // victim's browser (login CSRF).
    let resp = callback(&app, "code-a", &started.state, None).await;
    assert_failed(&resp);

    // A nonce cookie from a different sign-in.
    let first = start(&app).await;
    let second = start(&app).await;
    fake.codes
        .lock()
        .unwrap()
        .insert("code-b".into(), first.challenge.clone());
    let resp = callback(&app, "code-b", &first.state, Some(&second.nonce_cookie)).await;
    assert_failed(&resp);

    // A code GitHub issued for a different PKCE challenge (an intercepted
    // code used with someone else's flow).
    let flow = start(&app).await;
    fake.codes
        .lock()
        .unwrap()
        .insert("code-c".into(), second.challenge);
    let resp = callback(&app, "code-c", &flow.state, Some(&flow.nonce_cookie)).await;
    assert_failed(&resp);

    // The user pressed Cancel on GitHub.
    let url = Url::parse_with_params(
        &app.url("/api/auth/github/callback"),
        &[("error", "access_denied"), ("state", flow.state.as_str())],
    )
    .unwrap();
    let resp = app.client.get(url).send().await.unwrap();
    assert_failed(&resp);
}

pub async fn github_disabled_without_config(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let config: AuthConfig = app
        .get("/api/auth/config")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!config.github);
    let resp = app.get("/api/auth/github/start").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
