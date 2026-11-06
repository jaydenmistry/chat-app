//! The API error type. Every handler returns `Result<_, AppError>`, and every
//! error becomes a JSON [`ApiError`] body with a stable code.

use axum::{
    Json,
    extract::{FromRequest, Request, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use protocol::api::{ApiError, ErrorCode};
use serde::de::DeserializeOwned;

#[derive(Debug)]
pub enum AppError {
    BadRequest(String),
    Invalid {
        field: &'static str,
        message: String,
    },
    Unauthorized(&'static str),
    Forbidden(&'static str),
    NotFound,
    Conflict {
        field: &'static str,
        message: &'static str,
    },
    /// Rate limit exceeded (docs/design.md 11.1). Answered with 429.
    TooManyRequests(&'static str),
    /// A dependency failed in a way that may succeed on retry.
    Unavailable(anyhow::Error),
    /// A bug or an unexpected failure. Details are logged, never returned.
    Internal(anyhow::Error),
}

impl AppError {
    pub fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::Invalid {
            field,
            message: message.into(),
        }
    }

    pub fn internal(err: impl Into<anyhow::Error>) -> Self {
        Self::Internal(err.into())
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message) => write!(f, "bad request: {message}"),
            Self::Invalid { field, message } => write!(f, "invalid {field}: {message}"),
            Self::Unauthorized(message) | Self::Forbidden(message) => f.write_str(message),
            Self::NotFound => f.write_str("not found"),
            Self::Conflict { field, message } => write!(f, "conflict on {field}: {message}"),
            Self::TooManyRequests(message) => write!(f, "too many requests: {message}"),
            Self::Unavailable(err) => write!(f, "unavailable: {err:#}"),
            Self::Internal(err) => write!(f, "internal: {err:#}"),
        }
    }
}

impl std::error::Error for AppError {}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message, field) = match self {
            Self::BadRequest(message) => (
                StatusCode::BAD_REQUEST,
                ErrorCode::BadRequest,
                message,
                None,
            ),
            Self::Invalid { field, message } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                ErrorCode::InvalidInput,
                message,
                Some(field),
            ),
            Self::Unauthorized(message) => (
                StatusCode::UNAUTHORIZED,
                ErrorCode::Unauthorized,
                message.into(),
                None,
            ),
            Self::Forbidden(message) => (
                StatusCode::FORBIDDEN,
                ErrorCode::Forbidden,
                message.into(),
                None,
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                ErrorCode::NotFound,
                "not found".into(),
                None,
            ),
            Self::Conflict { field, message } => (
                StatusCode::CONFLICT,
                ErrorCode::Conflict,
                message.into(),
                Some(field),
            ),
            Self::TooManyRequests(message) => (
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::TooManyRequests,
                message.into(),
                None,
            ),
            Self::Unavailable(err) => {
                tracing::warn!(error = format!("{err:#}"), "dependency unavailable");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    ErrorCode::Unavailable,
                    "temporarily unavailable, try again".into(),
                    None,
                )
            }
            Self::Internal(err) => {
                tracing::error!(error = format!("{err:#}"), "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorCode::Internal,
                    "internal error".into(),
                    None,
                )
            }
        };
        let body = ApiError {
            code,
            message,
            field: field.map(String::from),
        };
        (status, Json(body)).into_response()
    }
}

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::Io(_)
            | sqlx::Error::Tls(_) => Self::Unavailable(err.into()),
            other => Self::Internal(other.into()),
        }
    }
}

impl From<redis::RedisError> for AppError {
    fn from(err: redis::RedisError) -> Self {
        if err.is_io_error()
            || err.is_timeout()
            || err.is_connection_dropped()
            || err.is_connection_refusal()
        {
            Self::Unavailable(err.into())
        } else {
            Self::Internal(err.into())
        }
    }
}

/// Like `axum::Json`, but a body that fails to parse becomes an [`ApiError`]
/// instead of axum's plain-text rejection.
pub struct ApiJson<T>(pub T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(Self(value)),
            Err(rejection) => Err(AppError::BadRequest(match rejection {
                JsonRejection::MissingJsonContentType(_) => {
                    "expected Content-Type: application/json".into()
                }
                other => other.body_text(),
            })),
        }
    }
}
