use std::sync::Arc;

use crate::infra::Infra;

/// The runner creates the keyspace and tables, records what it applied, and
/// applies nothing the second time.
pub async fn scylla_migrations_apply_once(infra: Arc<Infra>) {
    let keyspace = format!("migrate_{}", uuid::Uuid::now_v7().simple());
    let session = storage::scylladb::connect(std::slice::from_ref(&infra.scylla_node), None)
        .await
        .unwrap();

    let applied = storage::scylladb::migrate(&session, &keyspace, 1)
        .await
        .unwrap();
    assert_eq!(applied, [1]);

    let tables: Vec<String> = session
        .query_unpaged(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
            (keyspace.as_str(),),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();
    for table in [
        "messages",
        "room_buckets",
        "reactions",
        "room_changes",
        "schema_migrations",
    ] {
        assert!(
            tables.iter().any(|t| t == table),
            "missing table {table}: {tables:?}"
        );
    }

    let again = storage::scylladb::migrate(&session, &keyspace, 1)
        .await
        .unwrap();
    assert!(again.is_empty(), "re-applied {again:?}");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .unwrap();
}
