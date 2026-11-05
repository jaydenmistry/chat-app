//! Shared application state, handed to every handler.

use std::{
    ops::Deref,
    sync::{Arc, atomic::AtomicBool},
};

use redis::aio::ConnectionManager;
use sqlx::PgPool;
use storage::messages::MessageStore;
use uuid::Uuid;

use crate::{
    auth::password::PasswordHasher,
    config::Config,
    http::cookies::CookiePolicy,
    ids::IdGenerator,
    realtime::{hub::Hub, subscriber::Subscriber},
};

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
    /// Messages in Scylla; also owns the Scylla session.
    pub messages: MessageStore,
    pub ids: Arc<IdGenerator>,
    /// This instance's sockets and what each one receives.
    pub hub: Arc<Hub>,
    /// Asks the pub/sub task to (un)subscribe.
    pub subscriber: Subscriber,
    /// Distinguishes this process in worker-ID leases and logs.
    pub instance_id: Uuid,
    pub http: reqwest::Client,
    pub passwords: PasswordHasher,
    /// Secure `__Host-` cookies over HTTPS, plain ones on http://localhost.
    pub cookies: CookiePolicy,
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
