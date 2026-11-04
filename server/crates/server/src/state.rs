//! Shared application state, handed to every handler.

use std::{
    ops::Deref,
    sync::{Arc, atomic::AtomicBool},
};

use redis::aio::ConnectionManager;
use scylla::client::session::Session;
use sqlx::PgPool;

use crate::{auth::password::PasswordHasher, config::Config};

/// Cloned for every request, so it is a single `Arc`: one atomic increment
/// per clone, however many fields `Inner` grows.
#[derive(Clone)]
pub struct AppState(Arc<Inner>);

pub struct Inner {
    pub config: Config,
    pub pg: PgPool,
    /// Cloned per use: `ConnectionManager` needs `&mut self` to send a
    /// command, and clones share one multiplexed connection.
    pub redis: ConnectionManager,
    pub scylla: Session,
    pub http: reqwest::Client,
    pub passwords: PasswordHasher,
    /// Set when shutdown begins, so `/readyz` turns the load balancer away.
    pub shutting_down: AtomicBool,
}

impl AppState {
    pub fn new(inner: Inner) -> Self {
        Self(Arc::new(inner))
    }
}

impl Deref for AppState {
    type Target = Inner;

    fn deref(&self) -> &Inner {
        &self.0
    }
}
