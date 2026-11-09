//! The operator admin command (docs/design.md 11.4), against real stores.

use std::sync::Arc;

use protocol::{
    api::HistoryPage,
    frames::{ServerFrame, close},
};
use reqwest::StatusCode;
use uuid::Uuid;

use crate::{app::TestApp, infra::Infra, rooms::user, ws};

async fn history(app: &TestApp, who: &crate::rooms::User, room: Uuid) -> HistoryPage {
    app.get_as(who, &format!("/api/rooms/{room}/messages"))
        .await
        .json()
        .await
        .unwrap()
}

/// `admin ban` suspends an account: its open sockets close, its session stops
/// working, and it can't sign back in.
pub async fn admin_bans_an_account(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let mut socket = ws::connect(&app, &ada.cookie).await;

    let config = infra.config(app.database_url.clone());
    server::admin::run(&config, &["ban".into(), "ada".into()])
        .await
        .unwrap();

    assert_eq!(socket.closed().await, Some(close::SESSION_ENDED));
    assert_eq!(
        app.me(&ada.cookie).await.unwrap_err(),
        StatusCode::UNAUTHORIZED,
        "the session was revoked"
    );
    let resp = app.login("ada@example.com", "correct horse battery").await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "login is refused");

    app.stop().await;
}

/// `admin delete-message` removes a message exactly as the author's delete
/// would: watchers get `message.deleted`, and history shows it gone.
pub async fn admin_deletes_a_message(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let room = app.create_room(&ada, "General", "public").await;
    let mut socket = ws::connect(&app, &ada.cookie).await;
    let sent = socket.send_message(room.id, "to be removed").await;
    socket.expect_message("to be removed").await;

    // Quarantine is zero in the test config, so the lease is usable at once.
    let config = infra.config(app.database_url.clone());
    server::admin::run(
        &config,
        &[
            "delete-message".into(),
            room.id.to_string(),
            sent.id.to_string(),
        ],
    )
    .await
    .unwrap();

    socket
        .expect("message.deleted", |f| {
            matches!(f, ServerFrame::MessageDeleted { message_id, .. } if *message_id == sent.id)
        })
        .await;
    let page = history(&app, &ada, room.id).await;
    let message = page
        .messages
        .iter()
        .find(|m| m.id == sent.id)
        .expect("the message is still in history");
    assert!(message.deleted, "it is marked deleted");
    assert!(message.content.is_empty(), "its content is cleared");

    app.stop().await;
}
