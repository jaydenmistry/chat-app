use std::sync::Arc;

use protocol::api::{ErrorCode, Me, SessionInfo};
use reqwest::{StatusCode, header};
use serde_json::json;
use time::OffsetDateTime;

use crate::{
    app::{TestApp, expect_error, session_cookie, set_cookie},
    infra::Infra,
};

const PASSWORD: &str = "correct horse battery";

pub async fn signup_creates_account_and_session(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let resp = app
        .post("/api/auth/signup")
        .json(&json!({
            "email": "  Ada@Example.com ",
            "password": PASSWORD,
            "handle": "Ada",
            "display_name": " Ada Lovelace ",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let cookie = set_cookie(&resp, "__Host-session").expect("session cookie");
    for attribute in ["Path=/", "HttpOnly", "Secure", "SameSite=Lax"] {
        assert!(
            cookie.contains(attribute),
            "{attribute} missing from {cookie}"
        );
    }
    let max_age: u64 = cookie
        .split("Max-Age=")
        .nth(1)
        .and_then(|rest| rest.split(';').next())
        .and_then(|v| v.parse().ok())
        .expect("Max-Age");
    let thirty_days = 30 * 24 * 3600;
    assert!(
        (thirty_days - 5..=thirty_days).contains(&max_age),
        "{max_age}"
    );

    let session = session_cookie(&resp).unwrap();
    let created: Me = resp.json().await.unwrap();
    assert_eq!(created.handle, "ada");
    assert_eq!(created.display_name, "Ada Lovelace");
    assert!(!created.is_guest);
    assert_eq!(created.expires_at, None);

    assert_eq!(app.me(&session).await, Ok(created.clone()));
    // The normalized email signs in.
    let resp = app.login("ada@example.com", PASSWORD).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

pub async fn signup_rejects_taken_email_and_handle(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    app.signup("taken@example.com", "taken").await;

    let attempt = |email: &'static str, handle: &'static str| {
        app.post("/api/auth/signup")
            .json(&json!({
                "email": email, "password": PASSWORD, "handle": handle, "display_name": "X",
            }))
            .send()
    };
    // Uniqueness ignores case on both fields.
    let resp = attempt("TAKEN@example.com", "fresh").await.unwrap();
    let error = expect_error(resp, StatusCode::CONFLICT, ErrorCode::Conflict).await;
    assert_eq!(error.field.as_deref(), Some("email"));

    let resp = attempt("fresh@example.com", "TAKEN").await.unwrap();
    let error = expect_error(resp, StatusCode::CONFLICT, ErrorCode::Conflict).await;
    assert_eq!(error.field.as_deref(), Some("handle"));
}

pub async fn signup_validates_fields(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let cases = [
        ("not-an-email", PASSWORD, "valid_handle", "Name", "email"),
        ("a@example.com", "short", "valid_handle", "Name", "password"),
        ("a@example.com", PASSWORD, "no spaces", "Name", "handle"),
        (
            "a@example.com",
            PASSWORD,
            "guest_imposter",
            "Name",
            "handle",
        ),
        (
            "a@example.com",
            PASSWORD,
            "valid_handle",
            "   ",
            "display_name",
        ),
    ];
    for (email, password, handle, display_name, field) in cases {
        let resp = app
            .post("/api/auth/signup")
            .json(&json!({
                "email": email, "password": password, "handle": handle, "display_name": display_name,
            }))
            .send()
            .await
            .unwrap();
        let error = expect_error(
            resp,
            StatusCode::UNPROCESSABLE_ENTITY,
            ErrorCode::InvalidInput,
        )
        .await;
        assert_eq!(error.field.as_deref(), Some(field), "case {email} {handle}");
    }
}

/// Wrong password, unknown email and malformed email all get the same answer,
/// so the response doesn't reveal which emails are registered.
pub async fn login_checks_password(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let (user, _) = app.signup("login@example.com", "login_user").await;

    let resp = app.login("login@example.com", PASSWORD).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cookie = session_cookie(&resp).unwrap();
    assert_eq!(app.me(&cookie).await.unwrap().id, user.id);

    let mut messages = Vec::new();
    for (email, password) in [
        ("login@example.com", "wrong password"),
        ("nobody@example.com", PASSWORD),
        ("not an email", PASSWORD),
    ] {
        let resp = app.login(email, password).await;
        assert!(session_cookie(&resp).is_none());
        let error = expect_error(resp, StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized).await;
        messages.push(error.message);
    }
    assert!(
        messages.windows(2).all(|pair| pair[0] == pair[1]),
        "{messages:?}"
    );
}

pub async fn logout_ends_the_session(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let (_, cookie) = app.signup("logout@example.com", "logout_user").await;
    assert!(app.me(&cookie).await.is_ok());

    let resp = app
        .post("/api/auth/logout")
        .header(header::COOKIE, &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let cleared = set_cookie(&resp, "__Host-session").unwrap();
    assert!(cleared.starts_with("__Host-session=;") && cleared.contains("Max-Age=0"));

    // The old cookie no longer works, even if the browser kept it.
    assert_eq!(app.me(&cookie).await, Err(StatusCode::UNAUTHORIZED));
    // Logging out again is harmless.
    let resp = app
        .post("/api/auth/logout")
        .header(header::COOKIE, &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

pub async fn sessions_can_be_listed_and_revoked(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let (_, laptop) = app.signup("multi@example.com", "multi_user").await;
    let phone = session_cookie(&app.login("multi@example.com", PASSWORD).await).unwrap();
    let (_, stranger) = app.signup("stranger@example.com", "stranger").await;

    let list = |cookie: String| {
        let request = app.get("/api/sessions").header(header::COOKIE, cookie);
        async move {
            request
                .send()
                .await
                .unwrap()
                .json::<Vec<SessionInfo>>()
                .await
                .unwrap()
        }
    };
    let sessions = list(laptop.clone()).await;
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions.iter().filter(|s| s.current).count(), 1);
    let phone_session = sessions.iter().find(|s| !s.current).unwrap();
    assert_eq!(phone_session.ip.as_deref(), Some("127.0.0.1"));

    // Another user can't revoke it, and can't tell that it exists.
    let resp = app
        .delete(&format!("/api/sessions/{}", phone_session.id))
        .header(header::COOKIE, &stranger)
        .send()
        .await
        .unwrap();
    expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;
    assert!(app.me(&phone).await.is_ok());

    // The owner can, from the other session.
    let resp = app
        .delete(&format!("/api/sessions/{}", phone_session.id))
        .header(header::COOKIE, &laptop)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(app.me(&phone).await, Err(StatusCode::UNAUTHORIZED));
    assert!(app.me(&laptop).await.is_ok());
    assert_eq!(list(laptop).await.len(), 1);
}

pub async fn guest_session_ends_with_the_account(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let resp = app.post("/api/auth/guest").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let cookie_header = set_cookie(&resp, "__Host-session").unwrap();
    let cookie = session_cookie(&resp).unwrap();
    let guest: Me = resp.json().await.unwrap();

    assert!(guest.is_guest);
    assert!(guest.handle.starts_with("guest_"), "{}", guest.handle);
    let expires_at = guest.expires_at.expect("guests expire");
    let remaining = expires_at - OffsetDateTime::now_utc();
    assert!(
        remaining.whole_hours() == 23 || remaining.whole_hours() == 24,
        "{remaining}"
    );

    // The cookie lives exactly as long as the account, not the 30-day default.
    let max_age: i64 = cookie_header
        .split("Max-Age=")
        .nth(1)
        .and_then(|rest| rest.split(';').next())
        .and_then(|v| v.parse().ok())
        .unwrap();
    assert!(
        (max_age - remaining.whole_seconds()).abs() <= 5,
        "{max_age}"
    );

    assert_eq!(app.me(&cookie).await.unwrap(), guest);
}

/// The CSRF defence: a POST without our Origin is refused before it does
/// anything. GETs don't need it.
pub async fn state_changing_requests_need_an_allowed_origin(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let (_, cookie) = app.signup("origin@example.com", "origin_user").await;

    let body = json!({"email": "csrf@example.com", "password": PASSWORD, "handle": "csrf", "display_name": "X"});
    for origin in [
        None,
        Some("https://evil.example"),
        Some("http://app.test.evil.example"),
    ] {
        let mut request = app.client.post(app.url("/api/auth/signup")).json(&body);
        if let Some(origin) = origin {
            request = request.header(header::ORIGIN, origin);
        }
        let resp = request.send().await.unwrap();
        expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    }
    // Nothing was created by the refused requests.
    assert_eq!(
        app.login("csrf@example.com", PASSWORD).await.status(),
        StatusCode::UNAUTHORIZED
    );

    // A forged logout doesn't end the session.
    let resp = app
        .client
        .post(app.url("/api/auth/logout"))
        .header(header::COOKIE, &cookie)
        .header(header::ORIGIN, "https://evil.example")
        .send()
        .await
        .unwrap();
    expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    assert!(app.me(&cookie).await.is_ok());

    // Reads don't need an Origin.
    let resp = app.get("/api/auth/config").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

pub async fn malformed_requests_get_api_errors(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;

    let resp = app
        .post("/api/auth/login")
        .body("{not json")
        .send()
        .await
        .unwrap();
    expect_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;

    let resp = app
        .post("/api/auth/login")
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"email": "x@example.com"}"#)
        .send()
        .await
        .unwrap();
    expect_error(resp, StatusCode::BAD_REQUEST, ErrorCode::BadRequest).await;

    for cookie in [
        "__Host-session=",
        "__Host-session=garbage",
        "__Host-session=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    ] {
        assert_eq!(
            app.me(cookie).await,
            Err(StatusCode::UNAUTHORIZED),
            "{cookie}"
        );
    }
}
