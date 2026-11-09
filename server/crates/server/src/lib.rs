//! The chat server: REST API, WebSocket gateway and background jobs.
//!
//! `main.rs` is a thin command-line wrapper. Everything lives in this library
//! so integration tests can start real server instances in-process.

pub mod admin;
pub mod auth;
pub mod cleanup;
pub mod config;
pub mod demo;
pub mod healthcheck;
pub mod http;
pub mod ids;
pub mod maintenance;
pub mod observe;
pub mod permissions;
pub mod rate_limit;
pub mod realtime;
pub mod search;
pub mod startup;
pub mod state;
pub mod telemetry;

use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use tokio::{net::TcpListener, sync::watch};
use uuid::Uuid;

use crate::{
    auth::password::PasswordHasher,
    config::Config,
    http::cookies::CookiePolicy,
    ids::IdGenerator,
    realtime::{hub::Hub, presence, subscriber},
    state::{AppState, Inner},
};

/// At most this many Argon2 hashes run at once (each uses 19 MiB).
const MAX_CONCURRENT_HASHES: usize = 4;

/// On shutdown, clients are told to reconnect after a random delay in
/// `0..=RECONNECT_SPREAD_MS`, so a restart doesn't send them all to the other
/// instance at once (docs/design.md 13).
const RECONNECT_SPREAD_MS: u32 = 3_000;
/// How long to let the writers flush the `reconnect` frames before closing the
/// sockets. The frames are tiny; this is comfortably enough.
const RECONNECT_FLUSH: Duration = Duration::from_millis(250);

/// Runs the server on `listener` until `shutdown` completes.
pub async fn serve(
    config: Config,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    observe::install();
    let stores = startup::connect(&config).await?;
    let http = reqwest::Client::builder()
        .user_agent("chat-app")
        .timeout(Duration::from_secs(10))
        .build()
        .context("building the HTTP client")?;
    let cookies = CookiePolicy::for_public_url(&config.public_url);
    if !cookies.is_secure() {
        tracing::warn!(
            public_url = config.public_url,
            "PUBLIC_URL is not https: cookies are sent without Secure (development only)"
        );
    }
    let messages = storage::messages::MessageStore::new(stores.scylla)
        .await
        .context("preparing scylla statements")?;
    let instance_id = Uuid::now_v7();
    let ids = Arc::new(IdGenerator::new(config.clock_offset_ms));
    // Background tasks watch this and stop when it turns true.
    let (stop_background, background_stopped) = watch::channel(false);
    let lease = tokio::spawn(ids::run_lease(
        Arc::clone(&ids),
        stores.redis.clone(),
        instance_id,
        config.id_lease,
        background_stopped.clone(),
    ));

    let hub = Arc::new(Hub::default());
    let (subscriber, subscriber_task) = subscriber::spawn(
        subscriber::Deps {
            redis_url: config.redis_url.expose().to_string(),
            hub: Arc::clone(&hub),
            pg: stores.pg.clone(),
            redis: stores.redis.clone(),
        },
        background_stopped.clone(),
    );

    let state = AppState::new(Inner {
        config,
        pg: stores.pg,
        redis: stores.redis,
        messages,
        ids,
        hub,
        subscriber,
        instance_id,
        http,
        passwords: PasswordHasher::new(MAX_CONCURRENT_HASHES),
        cookies,
        shutting_down: AtomicBool::new(false),
    });

    let presence_task = tokio::spawn(presence::run_background(
        state.clone(),
        background_stopped.clone(),
    ));
    let cleanup_task = tokio::spawn(cleanup::run_background(
        state.clone(),
        state.config.guest_cleanup_every,
        background_stopped.clone(),
    ));
    let demo_task = tokio::spawn(demo::run_background(
        state.clone(),
        state.config.demo_reset_every,
        background_stopped.clone(),
    ));
    let indexer_task = tokio::spawn(search::run_indexer(state.clone(), background_stopped));
    let app = http::router(state.clone());
    tracing::info!(addr = %listener.local_addr()?, %instance_id, "listening");
    axum::serve(
        listener,
        // Makes the TCP peer address available to handlers (`ClientIp`).
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.await;
        state.shutting_down.store(true, Ordering::Relaxed);
        tracing::info!("shutting down");
        // `/readyz` now fails and new `/ws` upgrades are refused, so the load
        // balancer moves traffic away. Tell the open sockets to reconnect
        // (spread over a few seconds), give the writers a moment to send those
        // frames, then close them with `1012` (docs/design.md 13). The clients
        // reconnect to another instance and sync.
        let told = state.hub.drain(RECONNECT_SPREAD_MS);
        tracing::info!(connections = told, "told clients to reconnect");
        tokio::time::sleep(RECONNECT_FLUSH).await;
        state
            .hub
            .close_all((protocol::frames::close::RESTARTING, "server restarting"));
        let _ = stop_background.send(true);
    })
    .await
    .context("server error")?;

    // Let the background tasks finish (the lease task releases its worker
    // ID), but don't hang on them.
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = tokio::join!(
            lease,
            subscriber_task,
            presence_task,
            cleanup_task,
            demo_task,
            indexer_task
        );
    })
    .await;
    Ok(())
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
