//! Origin check for state-changing requests.
//!
//! Browsers attach `Origin` to every cross-origin request and to every
//! same-origin POST, PUT, PATCH and DELETE. Requiring it to be one of ours
//! blocks cross-site request forgery, including from other `*.jmistry.com`
//! sites, which `SameSite=Lax` treats as the same site.

use axum::{
    extract::{Request, State},
    http::{Method, header::ORIGIN},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::{http::error::AppError, state::AppState};

pub async fn require_allowed_origin(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let safe_method = matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS);
    if safe_method || origin_allowed(&state.config.allowed_origins, req.headers().get(ORIGIN)) {
        next.run(req).await
    } else {
        AppError::Forbidden("origin not allowed").into_response()
    }
}

/// A missing `Origin` is rejected too: every browser sends it on these
/// methods, so its absence means a non-browser client or a stripped header.
pub fn origin_allowed(allowed: &[String], origin: Option<&axum::http::HeaderValue>) -> bool {
    origin
        .and_then(|value| value.to_str().ok())
        .is_some_and(|origin| allowed.iter().any(|a| a == origin))
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    #[test]
    fn only_exact_origins_pass() {
        let allowed = vec!["https://chat.example.com".to_string()];
        let check = |o: &'static str| origin_allowed(&allowed, Some(&HeaderValue::from_static(o)));
        assert!(check("https://chat.example.com"));
        assert!(!check("https://chat.example.com.evil.com"));
        assert!(!check("http://chat.example.com"));
        assert!(!check("https://other.example.com"));
        assert!(!check("null"));
        assert!(!origin_allowed(&allowed, None));
    }
}
