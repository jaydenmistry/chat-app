//! The chat server: REST API, WebSocket gateway and background jobs.
//!
//! `main.rs` is a thin command-line wrapper. Everything lives in this library
//! so integration tests can start real server instances in-process.

pub mod auth;
pub mod config;
pub mod healthcheck;
pub mod http;
pub mod startup;
pub mod state;
pub mod telemetry;

use std::{
    future::Future,
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use anyhow::Context;
use tokio::net::TcpListener;

use crate::{
    auth::password::PasswordHasher,
    config::Config,
    state::{AppState, Inner},
};

/// At most this many Argon2 hashes run at once (each uses 19 MiB).
const MAX_CONCURRENT_HASHES: usize = 4;

/// Runs the server on `listener` until `shutdown` completes.
pub async fn serve(
    config: Config,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let stores = startup::connect(&config).await?;
    let http = reqwest::Client::builder()
        .user_agent("chat-app")
        .timeout(Duration::from_secs(10))
        .build()
        .context("building the HTTP client")?;
    let state = AppState::new(Inner {
        config,
        pg: stores.pg,
        redis: stores.redis,
        scylla: stores.scylla,
        http,
        passwords: PasswordHasher::new(MAX_CONCURRENT_HASHES),
        shutting_down: AtomicBool::new(false),
    });

    let app = http::router(state.clone());
    tracing::info!(addr = %listener.local_addr()?, "listening");
    axum::serve(
        listener,
        // Makes the TCP peer address available to handlers (`ClientIp`).
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.await;
        state.shutting_down.store(true, Ordering::Relaxed);
        tracing::info!("shutting down");
    })
    .await
    .context("server error")
}

/// Applies Postgres and Scylla migrations. Run once per deploy, by a single
/// process, before the servers start.
pub async fn migrate(config: &Config) -> anyhow::Result<()> {
    let pg =
        storage::postgres::pool(config.database_url.expose(), 1).context("invalid DATABASE_URL")?;
    startup::retry("postgres", || storage::postgres::MIGRATOR.run(&pg)).await?;
    tracing::info!("postgres migrations applied");

    let scylla = startup::connect_scylla(config, None).await?;
    let applied = storage::scylladb::migrate(
        &scylla,
        &config.scylla_keyspace,
        config.scylla_replication_factor,
    )
    .await?;
    tracing::info!(
        ?applied,
        keyspace = config.scylla_keyspace,
        "scylla migrations applied"
    );
    Ok(())
}

/// Completes on Ctrl-C or SIGTERM (what `docker stop` sends).
pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("installing the Ctrl-C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing the SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
