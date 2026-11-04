//! PostgreSQL connection pool and migrations.

use std::time::Duration;

use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

/// The migrations in `server/migrations/postgres`, embedded at compile time.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations/postgres");

/// Creates a pool that opens connections on first use, so the server can
/// start (and report itself not ready) while Postgres is still coming up.
pub fn pool(url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    let options: PgConnectOptions = url.parse()?;
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        // A request waits at most this long for a free connection, then fails
        // with `PoolTimedOut` (reported to clients as `unavailable`).
        .acquire_timeout(Duration::from_secs(3))
        .connect_lazy_with(options))
}

pub async fn ping(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT 1").execute(pool).await.map(|_| ())
}
