//! Guest cleanup (docs/design.md 11.3), end to end against real stores.

use std::{sync::Arc, time::Duration};

use protocol::{api::RoomInfo, frames::ServerFrame};
use serde_json::json;
use uuid::Uuid;

use crate::{
    app::TestApp,
    infra::Infra,
    rooms::{User, guest, user},
    ws,
};

/// A room still exists in Postgres.
async fn room_exists(pool: &sqlx::PgPool, id: Uuid) -> bool {
    sqlx::query_scalar::<_, Uuid>("SELECT id FROM rooms WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .unwrap()
        .is_some()
}

/// The storage pass deletes an expired guest, their DM rooms and memberships,
/// and leaves everyone else and their public rooms alone.
pub async fn expired_guests_and_their_dms_are_deleted(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let visitor = guest(&app).await;
    let room = app.create_room(&ada, "Lobby", "public").await;
    app.join(&visitor, &room).await;
    let dm: RoomInfo = app
        .post_as(&visitor, "/api/dms", Some(json!({"handle": "ada"})))
        .await
        .json()
        .await
        .unwrap();

    let pool = storage::postgres::pool(&app.database_url, 2).unwrap();
    // Expire the guest, as if their TTL had passed.
    sqlx::query("UPDATE users SET expires_at = now() - interval '1 minute' WHERE id = $1")
        .bind(visitor.me.id)
        .execute(&pool)
        .await
        .unwrap();

    let expired = storage::users::delete_expired_guests(&pool, 100)
        .await
        .unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].user_id, visitor.me.id);
    assert!(expired[0].rooms.contains(&room.id), "the public room");
    assert!(expired[0].rooms.contains(&dm.id), "the DM");

    // The guest and their DM are gone; ada and the public room remain.
    assert!(
        storage::users::find_by_id(&pool, visitor.me.id)
            .await
            .unwrap()
            .is_none(),
        "guest deleted"
    );
    assert!(
        storage::users::find_by_id(&pool, ada.me.id)
            .await
            .unwrap()
            .is_some(),
        "ada untouched"
    );
    assert!(!room_exists(&pool, dm.id).await, "DM deleted");
    assert!(room_exists(&pool, room.id).await, "public room kept");

    app.stop().await;
}

/// The background job runs, and a member of the guest's room is told to hide
/// their messages with a `user.expired` frame.
pub async fn guest_cleanup_announces_user_expired(infra: Arc<Infra>) {
    let app = TestApp::spawn_with(&infra, |c| {
        c.guest_ttl = Duration::from_secs(1);
        c.guest_cleanup_every = Duration::from_millis(250);
    })
    .await;
    let ada = user(&app, "ada").await;
    let room = app.create_room(&ada, "Lobby", "public").await;
    let visitor: User = guest(&app).await;
    app.join(&visitor, &room).await;

    // ada is watching the room; the guest expires a second later.
    let mut socket = ws::connect(&app, &ada.cookie).await;
    socket
        .expect("user.expired", |f| {
            matches!(f, ServerFrame::UserExpired { room_id, user_id }
                if *room_id == room.id && *user_id == visitor.me.id)
        })
        .await;

    // It really was deleted, not just announced.
    let pool = storage::postgres::pool(&app.database_url, 2).unwrap();
    assert!(
        storage::users::find_by_id(&pool, visitor.me.id)
            .await
            .unwrap()
            .is_none(),
        "the guest was deleted"
    );

    app.stop().await;
}
