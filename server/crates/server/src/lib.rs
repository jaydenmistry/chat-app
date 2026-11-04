//! The chat server: REST API, WebSocket gateway and background jobs.
//!
//! `main.rs` is a thin command-line wrapper. Everything lives in this library
//! so integration tests can start real server instances in-process.

pub mod config;
