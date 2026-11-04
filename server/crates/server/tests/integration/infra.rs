//! The shared containers.

use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use server::config::{Config, LogFormat, Secret};
use sqlx::{Connection, PgConnection};
use testcontainers_modules::{
    postgres::Postgres,
    redis::Redis,
    scylladb::ScyllaDB,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};
use tokio::sync::Mutex;

// The same versions as deploy/compose.yaml.
const POSTGRES_TAG: &str = "18.6-alpine";
const REDIS_TAG: &str = "8.10.3-alpine";
const SCYLLA_TAG: &str = "2026.3.4";

pub const KEYSPACE: &str = "chat_test";
/// https, so tests exercise the production cookie attributes (`__Host-`,
/// `Secure`). The server itself is reached over plain HTTP on loopback.
pub const TEST_ORIGIN: &str = "https://app.test";

pub struct Infra {
    _postgres: ContainerAsync<Postgres>,
    _redis: ContainerAsync<Redis>,
    _scylla: ContainerAsync<ScyllaDB>,
    /// `postgres://postgres:postgres@host:port`, without a database name.
    pg_base: String,
    pub redis_url: String,
    pub scylla_node: String,
    next_database: AtomicU32,
    /// `CREATE DATABASE ... TEMPLATE` fails if another session is copying the
    /// same template at that moment, so database creation is serialized.
    create_lock: Mutex<()>,
}

/// Registry to pull images from. Docker Hub by default; CI and the cloud
/// sandbox set `IMAGE_REGISTRY=mirror.gcr.io` to avoid Docker Hub's
/// anonymous pull limit.
fn registry() -> String {
    std::env::var("IMAGE_REGISTRY").unwrap_or_else(|_| "docker.io".into())
}

impl Infra {
    pub async fn start() -> Self {
        let registry = registry();
        let (postgres, redis, scylla) = tokio::join!(
            Postgres::default()
                .with_name(format!("{registry}/library/postgres"))
                .with_tag(POSTGRES_TAG)
                .start(),
            Redis::default()
                .with_name(format!("{registry}/library/redis"))
                .with_tag(REDIS_TAG)
                .start(),
            ScyllaDB::default()
                .with_name(format!("{registry}/scylladb/scylla"))
                .with_tag(SCYLLA_TAG)
                .with_cmd([
                    "--smp",
                    "1",
                    "--memory",
                    "750M",
                    "--overprovisioned",
                    "1",
                    "--developer-mode",
                    "1",
                    // Tests reach Scylla through a mapped port on loopback.
                    // Scylla normally advertises its container IP, and the
                    // driver then dials <container IP>:<mapped port>, which
                    // nothing listens on. Advertising 127.0.0.1 makes the
                    // driver use the address it was given.
                    "--broadcast-rpc-address",
                    "127.0.0.1",
                ])
                .start(),
        );
        let postgres = postgres.expect("starting postgres");
        let redis = redis.expect("starting redis");
        let scylla = scylla.expect("starting scylla");

        let pg_port = postgres.get_host_port_ipv4(5432).await.unwrap();
        let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();
        let scylla_port = scylla.get_host_port_ipv4(9042).await.unwrap();
        let pg_base = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}");

        // Migrate a template once; each test copies it, which is much faster
        // than migrating a fresh database per test.
        let mut admin = PgConnection::connect(&format!("{pg_base}/postgres"))
            .await
            .unwrap();
        sqlx::query("CREATE DATABASE chat_template")
            .execute(&mut admin)
            .await
            .unwrap();
        admin.close().await.unwrap();
        let mut template = PgConnection::connect(&format!("{pg_base}/chat_template"))
            .await
            .unwrap();
        storage::postgres::MIGRATOR
            .run(&mut template)
            .await
            .unwrap();
        template.close().await.unwrap();

        let scylla_node = format!("127.0.0.1:{scylla_port}");
        let session = storage::scylladb::connect(std::slice::from_ref(&scylla_node), None)
            .await
            .expect("connecting to scylla");
        wait_for_scylla(&session).await;
        storage::scylladb::migrate(&session, KEYSPACE, 1)
            .await
            .expect("migrating scylla");

        Self {
            _postgres: postgres,
            _redis: redis,
            _scylla: scylla,
            pg_base,
            redis_url: format!("redis://127.0.0.1:{redis_port}"),
            scylla_node,
            next_database: AtomicU32::new(0),
            create_lock: Mutex::new(()),
        }
    }

    /// A new, migrated database for one test.
    pub async fn fresh_database(&self) -> String {
        let name = format!(
            "test_{}",
            self.next_database.fetch_add(1, Ordering::Relaxed)
        );
        let _guard = self.create_lock.lock().await;
        let mut admin = PgConnection::connect(&format!("{}/postgres", self.pg_base))
            .await
            .unwrap();
        // `name` is built from a counter above, never from input.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE DATABASE {name} TEMPLATE chat_template"
        )))
        .execute(&mut admin)
        .await
        .unwrap();
        admin.close().await.unwrap();
        format!("{}/{name}", self.pg_base)
    }

    pub fn config(&self, database_url: String) -> Config {
        Config {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            public_url: TEST_ORIGIN.into(),
            allowed_origins: vec![TEST_ORIGIN.into()],
            client_ip_header: None,
            log_format: LogFormat::Pretty,
            static_dir: None,
            database_url: Secret::new(database_url),
            database_max_connections: 5,
            redis_url: Secret::new(self.redis_url.clone()),
            scylla_nodes: vec![self.scylla_node.clone()],
            scylla_keyspace: KEYSPACE.into(),
            scylla_replication_factor: 1,
            github: None,
            session_ttl: Duration::from_secs(30 * 24 * 3600),
            guest_ttl: Duration::from_secs(24 * 3600),
        }
    }
}

/// See `storage::scylladb::connect`: the first queries after startup can fail
/// while the driver's connection pool comes up.
async fn wait_for_scylla(session: &scylla::client::session::Session) {
    let mut last_error = String::new();
    for _ in 0..60 {
        match storage::scylladb::ping(session).await {
            Ok(()) => return,
            Err(err) => last_error = err.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("scylla did not start answering queries: {last_error}");
}
