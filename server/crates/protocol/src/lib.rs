//! Types shared by the server and the web client.
//!
//! Every type that crosses the wire is defined here once. ts-rs generates the
//! TypeScript equivalents into `web/src/protocol/generated/` when the tests of
//! this crate run (`cargo test -p protocol`). CI regenerates them and fails if
//! the committed files differ.

pub mod api;
pub mod frames;
pub mod ids;
pub mod reactions;
pub mod validate;
