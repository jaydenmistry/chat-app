//! The operator maintenance commands (docs/design.md 10, 4.2), against real
//! stores.

use std::{sync::Arc, time::Duration};

use protocol::api::SearchResults;
use scylla::client::session::Session;
use server::config::{Config, Secret};
use uuid::Uuid;

use crate::{
    app::TestApp,
    infra::{self, Infra},
    rooms::{User, user},
    ws,
};

async fn search(app: &TestApp, who: &User, query: &str) -> Vec<(String, Uuid)> {
    let url = reqwest::Url::parse_with_params(&app.url("/api/search"), [("q", query)]).unwrap();
    let resp = app
        .client
        .get(url)
        .header(reqwest::header::COOKIE, &who.cookie)
        .send()
        .await
        .unwrap();
    let results: SearchResults = resp.json().await.unwrap();
    results
        .messages
        .into_iter()
        .map(|m| (m.content, m.room_id))
        .collect()
}

/// Polls search until it returns `want` rows, or times out.
async fn wait_for_search(
    app: &TestApp,
    who: &User,
    query: &str,
    want: usize,
) -> Vec<(String, Uuid)> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let hits = search(app, who, query).await;
        if hits.len() == want || tokio::time::Instant::now() >= deadline {
            return hits;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn scylla(infra: &Infra) -> Session {
    storage::scylladb::connect(
        std::slice::from_ref(&infra.scylla_node),
        Some(infra::KEYSPACE),
    )
    .await
    .expect("connecting to scylla")
}

async fn bucket_count(session: &Session, room_id: Uuid) -> usize {
    let result = session
        .query_unpaged(
            "SELECT bucket FROM room_buckets WHERE room_id = ?",
            (room_id,),
        )
        .await
        .unwrap();
    result.into_rows_result().unwrap().rows_num()
}

/// `reindex --room` rebuilds a room's search entries from Scylla, even after
/// the index rows were lost.
pub async fn reindex_rebuilds_search_entries(infra: Arc<Infra>) {
    // Its own Redis, so this app's indexer is the only consumer of the stream.
    let (_redis, redis_url) = infra.dedicated_redis().await;
    let point_at_redis = |c: &mut Config| c.redis_url = Secret::new(redis_url.clone());
    let app = TestApp::spawn_with(&infra, point_at_redis).await;
    let ada = user(&app, "ada").await;
    let room = app.create_room(&ada, "Index", "public").await;
    let mut socket = ws::connect(&app, &ada.cookie).await;
    socket.send_message(room.id, "a reindexable narwhal").await;

    assert_eq!(wait_for_search(&app, &ada, "narwhal", 1).await.len(), 1);

    // The index row is lost (a trim while no indexer ran); search goes blank.
    let pool = storage::postgres::pool(&app.database_url, 2).unwrap();
    sqlx::query("DELETE FROM message_search WHERE room_id = $1")
        .bind(room.id)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(search(&app, &ada, "narwhal").await, []);

    // Reindex re-enqueues the room's messages; the indexer rebuilds the row.
    let mut config = infra.config(app.database_url.clone());
    point_at_redis(&mut config);
    server::maintenance::run(
        &config,
        &["reindex".into(), "--room".into(), room.id.to_string()],
    )
    .await
    .unwrap();

    let hits = wait_for_search(&app, &ada, "narwhal", 1).await;
    assert_eq!(hits, [("a reindexable narwhal".to_string(), room.id)]);

    app.stop().await;
}

/// `reclaim --room` deletes the Scylla partitions of a room that has already
/// been removed from Postgres, and refuses one that still exists.
pub async fn reclaim_frees_a_deleted_rooms_scylla_space(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let room = app.create_room(&ada, "Doomed", "public").await;
    let mut socket = ws::connect(&app, &ada.cookie).await;
    socket.send_message(room.id, "first").await;
    socket.send_message(room.id, "second").await;

    let session = scylla(&infra).await;
    assert!(
        bucket_count(&session, room.id).await >= 1,
        "messages are in scylla"
    );

    // Reclaim refuses while the room still exists.
    let config = infra.config(app.database_url.clone());
    let args = ["reclaim".into(), "--room".into(), room.id.to_string()];
    assert!(
        server::maintenance::run(&config, &args).await.is_err(),
        "refuses a live room"
    );

    // Delete the room (Postgres only; the Scylla rows linger), then reclaim.
    let resp = app
        .delete(&format!("/api/rooms/{}", room.id))
        .header(reqwest::header::COOKIE, &ada.cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert!(
        bucket_count(&session, room.id).await >= 1,
        "still in scylla after delete"
    );

    server::maintenance::run(&config, &args).await.unwrap();
    assert_eq!(
        bucket_count(&session, room.id).await,
        0,
        "scylla space reclaimed"
    );

    app.stop().await;
}
