//! ScyllaDB session and schema migrations.
//!
//! sqlx handles Postgres migrations. Scylla has no equivalent we can depend
//! on, so this module has a small runner: numbered CQL files embedded at
//! compile time, applied in order, and recorded in `schema_migrations`.

use std::{collections::HashSet, time::Duration};

use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    errors::{ExecutionError, NewSessionError, SchemaAgreementError, UseKeyspaceError},
};

/// One migration file. `version` must match the file's number prefix.
pub struct Migration {
    pub version: i32,
    pub name: &'static str,
    pub cql: &'static str,
}

/// Every migration, in order. A test checks that this list matches the files
/// in `server/migrations/scylla`, so a new file can't be forgotten here.
pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "messages",
    cql: include_str!("../../../migrations/scylla/0001_messages.cql"),
}];

/// The driver's error types are large (hundreds of bytes), so they are boxed
/// to keep `Result<_, MigrateError>` small on the success path.
#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    #[error("scylla query failed: {0}")]
    Execution(Box<ExecutionError>),
    #[error("switching keyspace failed: {0}")]
    UseKeyspace(Box<UseKeyspaceError>),
    #[error("schema agreement failed: {0}")]
    SchemaAgreement(Box<SchemaAgreementError>),
    #[error("reading applied migrations failed: {0}")]
    ReadApplied(Box<dyn std::error::Error + Send + Sync>),
}

impl From<ExecutionError> for MigrateError {
    fn from(err: ExecutionError) -> Self {
        Self::Execution(Box::new(err))
    }
}

impl From<UseKeyspaceError> for MigrateError {
    fn from(err: UseKeyspaceError) -> Self {
        Self::UseKeyspace(Box::new(err))
    }
}

impl From<SchemaAgreementError> for MigrateError {
    fn from(err: SchemaAgreementError) -> Self {
        Self::SchemaAgreement(Box::new(err))
    }
}

/// Connects to the cluster. With `keyspace`, unqualified table names in
/// queries refer to that keyspace.
///
/// A successful connect doesn't mean queries work yet: right after startup
/// Scylla accepts the first connection before its per-shard port is up, and
/// the driver's pool can come up broken for a moment. Callers that need the
/// cluster usable should also wait for [`ping`] to succeed.
pub async fn connect(
    nodes: &[String],
    keyspace: Option<&str>,
) -> Result<Session, Box<NewSessionError>> {
    let mut builder = SessionBuilder::new()
        .known_nodes(nodes)
        .connection_timeout(Duration::from_secs(3));
    if let Some(keyspace) = keyspace {
        builder = builder.use_keyspace(keyspace, false);
    }
    builder.build().await.map_err(Box::new)
}

pub async fn ping(session: &Session) -> Result<(), Box<ExecutionError>> {
    session
        .query_unpaged("SELECT now() FROM system.local", ())
        .await
        .map(|_| ())
        .map_err(Box::new)
}

/// Creates the keyspace if needed and applies every migration not yet
/// recorded. Returns the versions it applied.
///
/// Run from one process at a time (the `migrate` command); two runners racing
/// on schema changes could conflict. `keyspace` must already be validated as
/// a plain identifier, because it is interpolated into CQL.
pub async fn migrate(
    session: &Session,
    keyspace: &str,
    replication_factor: u8,
) -> Result<Vec<i32>, MigrateError> {
    session
        .query_unpaged(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = \
                 {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}"
            ),
            (),
        )
        .await?;
    session.await_schema_agreement().await?;
    session.use_keyspace(keyspace, false).await?;
    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS schema_migrations \
             (version int PRIMARY KEY, name text, applied_at timestamp)",
            (),
        )
        .await?;
    session.await_schema_agreement().await?;

    let applied = applied_versions(session).await?;

    let mut newly_applied = Vec::new();
    for migration in MIGRATIONS {
        if applied.contains(&migration.version) {
            continue;
        }
        for statement in statements(migration.cql) {
            session.query_unpaged(statement, ()).await?;
            // Wait until every node has the new schema before the next
            // statement depends on it.
            session.await_schema_agreement().await?;
        }
        session
            .query_unpaged(
                "INSERT INTO schema_migrations (version, name, applied_at) \
                 VALUES (?, ?, toTimestamp(now()))",
                (migration.version, migration.name),
            )
            .await?;
        newly_applied.push(migration.version);
    }
    Ok(newly_applied)
}

async fn applied_versions(session: &Session) -> Result<HashSet<i32>, MigrateError> {
    let read = |err: Box<dyn std::error::Error + Send + Sync>| MigrateError::ReadApplied(err);
    session
        .query_unpaged("SELECT version FROM schema_migrations", ())
        .await?
        .into_rows_result()
        .map_err(|e| read(e.into()))?
        .rows::<(i32,)>()
        .map_err(|e| read(e.into()))?
        .map(|row| row.map(|(version,)| version))
        .collect::<Result<_, _>>()
        .map_err(|e| read(e.into()))
}

/// Splits a migration file into statements: drops `--` comment lines, then
/// splits on `;`. Migrations must not contain `;` or `--` inside string
/// literals; none of ours do, and the tests below check every file.
pub fn statements(cql: &str) -> Vec<String> {
    let without_comments: String = cql
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    without_comments
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_statements_and_drops_comments() {
        let cql = "-- header\nCREATE TABLE IF NOT EXISTS a (x int PRIMARY KEY);\n\n  -- note\nCREATE TABLE IF NOT EXISTS b (\n  y int PRIMARY KEY\n);\n";
        assert_eq!(
            statements(cql),
            [
                "CREATE TABLE IF NOT EXISTS a (x int PRIMARY KEY)",
                "CREATE TABLE IF NOT EXISTS b (\n  y int PRIMARY KEY\n)",
            ]
        );
    }

    #[test]
    fn migration_list_matches_files() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations/scylla");
        let mut files: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.ends_with(".cql"))
            .collect();
        files.sort();
        let listed: Vec<String> = MIGRATIONS
            .iter()
            .map(|m| format!("{:04}_{}.cql", m.version, m.name))
            .collect();
        assert_eq!(files, listed);
    }

    #[test]
    fn every_statement_is_idempotent() {
        for migration in MIGRATIONS {
            for statement in statements(migration.cql) {
                assert!(
                    statement.contains("IF NOT EXISTS"),
                    "migration {} has a statement that is not safe to re-run: {statement}",
                    migration.version
                );
            }
        }
    }
}
