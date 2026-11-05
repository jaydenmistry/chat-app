//! The HTTP surface: routes, middleware and handlers.

pub mod auth;
pub mod client_ip;
pub mod cookies;
pub mod error;
pub mod health;
pub mod messages;
pub mod origin;
pub mod rooms;

use std::time::Duration;

use axum::{
    Router,
    extract::{DefaultBodyLimit, Request},
    http::StatusCode,
    middleware,
    routing::{delete, get, post},
};
use tower_http::{
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    services::{ServeDir, ServeFile},
    timeout::TimeoutLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::Level;

use crate::{http::error::AppError, state::AppState};

/// JSON bodies are small; 64 KiB is generous and caps memory per request.
const BODY_LIMIT: usize = 64 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub fn router(state: AppState) -> Router {
    let api = Router::new()
        .route("/auth/config", get(auth::config))
        .route("/auth/signup", post(auth::signup))
        .route("/auth/login", post(auth::login))
        .route("/auth/guest", post(auth::guest))
        .route("/auth/logout", post(auth::logout))
        .route("/auth/github/start", get(auth::github_start))
        .route("/auth/github/callback", get(auth::github_callback))
        .route("/me", get(auth::current_user))
        .route("/sessions", get(auth::list_sessions))
        .route("/sessions/{id}", delete(auth::revoke_session))
        .route("/rooms", get(rooms::list).post(rooms::create))
        .route("/rooms/public", get(rooms::directory))
        .route("/rooms/{id}", delete(rooms::delete))
        .route("/rooms/{id}/join", post(rooms::join))
        .route("/rooms/{id}/leave", post(rooms::leave))
        .route("/rooms/{id}/members", get(rooms::members))
        .route(
            "/rooms/{id}/members/{user_id}",
            delete(rooms::remove_member),
        )
        .route("/rooms/{id}/invites", post(rooms::create_invite))
        .route("/rooms/{id}/messages", get(messages::history))
        .route("/rooms/{id}/receipts", get(messages::receipts))
        .route("/invites/{code}/accept", post(rooms::accept_invite))
        .route("/dms", post(rooms::open_dm))
        .route("/users", get(rooms::profiles))
        .route("/sync", post(messages::sync))
        // `route_layer` runs only for matched routes, so unknown paths get a
        // 404 rather than a 403.
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            origin::require_allowed_origin,
        ))
        .fallback(|| async { AppError::NotFound })
        .layer(DefaultBodyLimit::max(BODY_LIMIT));

    let mut app = Router::new()
        // The upgrade does its own Origin and session checks.
        .route("/ws", get(crate::realtime::gateway::upgrade))
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .nest("/api", api);

    if let Some(dir) = &state.config.static_dir {
        // The SPA: real files as they are, every other path gets index.html
        // so client-side routes survive a reload.
        let index = ServeFile::new(dir.join("index.html"));
        app = app.fallback_service(ServeDir::new(dir).fallback(index));
    }

    // Layers wrap everything added before them, so the last one listed runs
    // first: assign a request ID, copy it to the response, open the tracing
    // span (which reads the ID), then apply the timeout.
    app.layer(TimeoutLayer::with_status_code(
        StatusCode::SERVICE_UNAVAILABLE,
        REQUEST_TIMEOUT,
    ))
    .layer(
        TraceLayer::new_for_http()
            .make_span_with(|req: &Request| {
                let request_id = req
                    .headers()
                    .get("x-request-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-");
                // The path only: query strings can hold OAuth codes and state.
                tracing::info_span!(
                    "http",
                    method = %req.method(),
                    path = req.uri().path(),
                    request_id,
                )
            })
            .on_response(DefaultOnResponse::new().level(Level::INFO)),
    )
    .layer(PropagateRequestIdLayer::x_request_id())
    .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
    .with_state(state)
}
