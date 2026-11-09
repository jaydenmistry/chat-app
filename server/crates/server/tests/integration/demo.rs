//! Demo rooms and the 24-hour reset (docs/design.md 11.4), end to end.

use std::{sync::Arc, time::Duration};

use protocol::{
    api::{HistoryPage, PublicRoom},
    frames::ServerFrame,
};
use storage::users::SYSTEM_USER_ID;
use uuid::Uuid;

use crate::{app::TestApp, infra::Infra, rooms::user, ws};

async fn history(app: &TestApp, who: &crate::rooms::User, room: Uuid) -> HistoryPage {
    app.get_as(who, &format!("/api/rooms/{room}/messages"))
        .await
        .json()
        .await
        .unwrap()
}

/// The reset job seeds a demo room, raises its history floor and sets its
/// expiry, and announces `room.reset` to members who are connected.
pub async fn demo_rooms_are_seeded_and_reset(infra: Arc<Infra>) {
    let app = TestApp::spawn_with(&infra, |c| {
        c.demo_reset_every = Duration::from_millis(200);
    })
    .await;
    let ada = user(&app, "ada").await;

    // The demo rooms are in the public directory from the start, owned by the
    // system user.
    let directory: Vec<PublicRoom> = app
        .get_as(&ada, "/api/rooms/public")
        .await
        .json()
        .await
        .unwrap();
    let general = directory
        .iter()
        .find(|r| r.name == "general")
        .expect("the general demo room is in the directory");
    app.post_as(&ada, &format!("/api/rooms/{}/join", general.id), None)
        .await;

    // The first pass (the room has never been reset) seeds it.
    let mut page = history(&app, &ada, general.id).await;
    for _ in 0..100 {
        if !page.messages.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        page = history(&app, &ada, general.id).await;
    }
    assert!(!page.messages.is_empty(), "the demo room was seeded");
    assert!(
        page.messages.iter().all(|m| m.author_id == SYSTEM_USER_ID),
        "seed messages are authored by the system user"
    );
    assert!(
        page.messages.iter().all(|m| !m.content.is_empty()),
        "seed messages have content"
    );

    // The reset raised the floor and set when the day's messages expire.
    let pool = storage::postgres::pool(&app.database_url, 2).unwrap();
    let (floor, expiry): (i64, Option<time::OffsetDateTime>) =
        sqlx::query_as("SELECT history_floor, message_expiry_at FROM rooms WHERE id = $1")
            .bind(general.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(floor > 0, "history floor was raised");
    let expiry = expiry.expect("message_expiry_at was set");
    assert!(
        expiry > time::OffsetDateTime::now_utc(),
        "expiry is in the future"
    );

    // ada watches the room, then its day is forced to end: the next pass
    // resets it again and tells her to reload.
    let mut socket = ws::connect(&app, &ada.cookie).await;
    sqlx::query("UPDATE rooms SET message_expiry_at = now() - interval '1 minute' WHERE id = $1")
        .bind(general.id)
        .execute(&pool)
        .await
        .unwrap();
    socket
        .expect(
            "room.reset",
            |f| matches!(f, ServerFrame::RoomReset { room_id } if *room_id == general.id),
        )
        .await;

    app.stop().await;
}
