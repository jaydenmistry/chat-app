//! Data access for the three stores.
//!
//! - [`postgres`]: accounts, sessions metadata, rooms and memberships.
//! - [`scylladb`]: messages, reactions and the change feed.
//! - [`redisdb`]: live sessions, pub/sub, presence, rate limits.
//!
//! The domain modules ([`users`], [`sessions`], [`rooms`], [`messages`])
//! hold the queries. Nothing in
//! this crate knows about HTTP.

pub mod messages;
pub mod postgres;
pub mod redisdb;
pub mod rooms;
pub mod scylladb;
pub mod search;
pub mod sessions;
pub mod users;
