//! The room REST API.

use std::sync::Arc;

use protocol::api::{ErrorCode, Invite, Me, Member, MemberRole, PublicRoom, RoomInfo, RoomKind};
use reqwest::{StatusCode, header};
use serde_json::{Value, json};

use crate::{
    app::{TestApp, expect_error, session_cookie},
    infra::Infra,
};

/// A signed-in user: their profile and cookie.
pub struct User {
    pub me: Me,
    pub cookie: String,
}

pub async fn user(app: &TestApp, handle: &str) -> User {
    let (me, cookie) = app.signup(&format!("{handle}@example.com"), handle).await;
    User { me, cookie }
}

pub async fn guest(app: &TestApp) -> User {
    let resp = app.post("/api/auth/guest").send().await.unwrap();
    let cookie = session_cookie(&resp).unwrap();
    User {
        me: resp.json().await.unwrap(),
        cookie,
    }
}

impl TestApp {
    pub async fn get_as(&self, user: &User, path: &str) -> reqwest::Response {
        self.get(path)
            .header(header::COOKIE, &user.cookie)
            .send()
            .await
            .unwrap()
    }

    pub async fn post_as(&self, user: &User, path: &str, body: Option<Value>) -> reqwest::Response {
        let mut request = self.post(path).header(header::COOKIE, &user.cookie);
        if let Some(body) = body {
            request = request.json(&body);
        }
        request.send().await.unwrap()
    }

    pub async fn delete_as(&self, user: &User, path: &str) -> reqwest::Response {
        self.delete(path)
            .header(header::COOKIE, &user.cookie)
            .send()
            .await
            .unwrap()
    }

    pub async fn create_room(&self, owner: &User, name: &str, kind: &str) -> RoomInfo {
        let resp = self
            .post_as(
                owner,
                "/api/rooms",
                Some(json!({"name": name, "kind": kind})),
            )
            .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        resp.json().await.unwrap()
    }

    pub async fn join(&self, user: &User, room: &RoomInfo) -> RoomInfo {
        let resp = self
            .post_as(user, &format!("/api/rooms/{}/join", room.id), None)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        resp.json().await.unwrap()
    }

    pub async fn my_rooms(&self, user: &User) -> Vec<RoomInfo> {
        self.get_as(user, "/api/rooms").await.json().await.unwrap()
    }
}

pub async fn public_rooms_can_be_created_browsed_joined_and_left(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let bob = user(&app, "bob").await;

    let room = app.create_room(&ada, "  General ", "public").await;
    assert_eq!(room.name.as_deref(), Some("General"));
    assert_eq!(room.kind, RoomKind::Public);
    assert_eq!(room.role, MemberRole::Owner);
    assert_eq!(app.my_rooms(&ada).await, std::slice::from_ref(&room));

    let directory: Vec<PublicRoom> = app
        .get_as(&bob, "/api/rooms/public")
        .await
        .json()
        .await
        .unwrap();
    let listed = directory.iter().find(|r| r.id == room.id).unwrap();
    assert_eq!((listed.member_count, listed.joined), (1, false));

    let joined = app.join(&bob, &room).await;
    assert_eq!(joined.role, MemberRole::Member);
    // Joining twice is harmless.
    app.join(&bob, &room).await;
    let members: Vec<Member> = app
        .get_as(&bob, &format!("/api/rooms/{}/members", room.id))
        .await
        .json()
        .await
        .unwrap();
    let handles: Vec<_> = members
        .iter()
        .map(|m| (m.user.handle.as_str(), m.role))
        .collect();
    assert_eq!(
        handles,
        [("ada", MemberRole::Owner), ("bob", MemberRole::Member)]
    );

    // The owner can't leave; a member can.
    let resp = app
        .post_as(&ada, &format!("/api/rooms/{}/leave", room.id), None)
        .await;
    expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    let resp = app
        .post_as(&bob, &format!("/api/rooms/{}/leave", room.id), None)
        .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(app.my_rooms(&bob).await.is_empty());
    let resp = app
        .get_as(&bob, &format!("/api/rooms/{}/members", room.id))
        .await;
    expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;

    let resp = app
        .post_as(
            &ada,
            "/api/rooms",
            Some(json!({"name": "", "kind": "public"})),
        )
        .await;
    let error = expect_error(
        resp,
        StatusCode::UNPROCESSABLE_ENTITY,
        ErrorCode::InvalidInput,
    )
    .await;
    assert_eq!(error.field.as_deref(), Some("name"));
}

pub async fn private_rooms_are_invite_only(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let bob = user(&app, "bob").await;
    let carol = user(&app, "carol").await;
    let visitor = guest(&app).await;
    let room = app.create_room(&ada, "Secret", "private").await;

    // Invisible to everyone else: not listed, can't be joined, and its
    // existence isn't confirmed (404, not 403).
    let directory: Vec<PublicRoom> = app
        .get_as(&bob, "/api/rooms/public")
        .await
        .json()
        .await
        .unwrap();
    assert!(directory.iter().all(|r| r.id != room.id));
    let resp = app
        .post_as(&bob, &format!("/api/rooms/{}/join", room.id), None)
        .await;
    expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;

    // Only the owner invites.
    let resp = app
        .post_as(&bob, &format!("/api/rooms/{}/invites", room.id), None)
        .await;
    expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;
    let resp = app
        .post_as(&ada, &format!("/api/rooms/{}/invites", room.id), None)
        .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let invite: Invite = resp.json().await.unwrap();
    assert!(invite.code.len() >= 22, "128 bits of base64url");

    let accept_path = format!("/api/invites/{}/accept", invite.code);
    // An async closure, so the future may borrow its argument.
    let accept = async |who: &User| app.post_as(who, &accept_path, None).await;
    let resp = accept(&bob).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let joined: RoomInfo = resp.json().await.unwrap();
    assert_eq!(joined.id, room.id);
    assert_eq!(
        accept(&bob).await.status(),
        StatusCode::OK,
        "accepting twice is harmless"
    );
    assert_eq!(accept(&carol).await.status(), StatusCode::OK);

    // Guests can't join private rooms.
    let resp = accept(&visitor).await;
    expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    // An unknown code is a plain 404.
    let resp = app
        .post_as(&bob, "/api/invites/not-a-real-code/accept", None)
        .await;
    expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;

    // Public rooms have no invites.
    let public = app.create_room(&ada, "Open", "public").await;
    let resp = app
        .post_as(&ada, &format!("/api/rooms/{}/invites", public.id), None)
        .await;
    expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
}

pub async fn guests_cant_create_rooms(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let visitor = guest(&app).await;
    let resp = app
        .post_as(
            &visitor,
            "/api/rooms",
            Some(json!({"name": "Mine", "kind": "public"})),
        )
        .await;
    expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    // But they can join public rooms.
    let ada = user(&app, "ada").await;
    let room = app.create_room(&ada, "Lobby", "public").await;
    app.join(&visitor, &room).await;
}

pub async fn only_the_owner_removes_members(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let bob = user(&app, "bob").await;
    let carol = user(&app, "carol").await;
    let room = app.create_room(&ada, "Team", "public").await;
    app.join(&bob, &room).await;
    app.join(&carol, &room).await;

    let path = |who: &User| format!("/api/rooms/{}/members/{}", room.id, who.me.id);
    let resp = app.delete_as(&bob, &path(&carol)).await;
    expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    let resp = app.delete_as(&ada, &path(&ada)).await;
    expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    let resp = app.delete_as(&ada, &path(&carol)).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(app.my_rooms(&carol).await.is_empty());
    let resp = app.delete_as(&ada, &path(&carol)).await;
    expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;
}

pub async fn deleting_a_room_removes_everyone(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let bob = user(&app, "bob").await;
    let room = app.create_room(&ada, "Temp", "public").await;
    app.join(&bob, &room).await;

    let resp = app
        .delete_as(&bob, &format!("/api/rooms/{}", room.id))
        .await;
    expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    let resp = app
        .delete_as(&ada, &format!("/api/rooms/{}", room.id))
        .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(app.my_rooms(&ada).await.is_empty());
    assert!(app.my_rooms(&bob).await.is_empty());
    let resp = app
        .get_as(&bob, &format!("/api/rooms/{}/messages", room.id))
        .await;
    expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;
}

pub async fn one_dm_per_pair(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let bob = user(&app, "bob").await;

    let open = async |from: &User, handle: &str| {
        app.post_as(from, "/api/dms", Some(json!({"handle": handle})))
            .await
    };
    let first: RoomInfo = open(&ada, "bob").await.json().await.unwrap();
    assert_eq!(first.kind, RoomKind::Dm);
    assert_eq!(first.name, None);
    assert_eq!(first.dm_peer.as_ref().unwrap().handle, "bob");

    // The same pair, either way round and in any case, is the same room.
    let again: RoomInfo = open(&ada, "@BOB").await.json().await.unwrap();
    let reverse: RoomInfo = open(&bob, "ada").await.json().await.unwrap();
    assert_eq!(again.id, first.id);
    assert_eq!(reverse.id, first.id);
    assert_eq!(reverse.dm_peer.as_ref().unwrap().handle, "ada");

    // Concurrent first opens can't create two.
    let carol = user(&app, "carol").await;
    let (a, b) = tokio::join!(open(&carol, "ada"), open(&ada, "carol"));
    let a: RoomInfo = a.json().await.unwrap();
    let b: RoomInfo = b.json().await.unwrap();
    assert_eq!(a.id, b.id);

    let resp = open(&ada, "ada").await;
    expect_error(
        resp,
        StatusCode::UNPROCESSABLE_ENTITY,
        ErrorCode::InvalidInput,
    )
    .await;
    let resp = open(&ada, "nobody_here").await;
    expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;

    // DMs can't be left or deleted.
    let resp = app
        .post_as(&ada, &format!("/api/rooms/{}/leave", first.id), None)
        .await;
    expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;
    let resp = app
        .delete_as(&ada, &format!("/api/rooms/{}", first.id))
        .await;
    expect_error(resp, StatusCode::FORBIDDEN, ErrorCode::Forbidden).await;

    // Guests can DM.
    let visitor = guest(&app).await;
    let resp = open(&visitor, "ada").await;
    assert_eq!(resp.status(), StatusCode::OK);
}

pub async fn non_members_see_nothing(infra: Arc<Infra>) {
    let app = TestApp::spawn(&infra).await;
    let ada = user(&app, "ada").await;
    let mallory = user(&app, "mallory").await;
    let room = app.create_room(&ada, "Private", "private").await;

    for path in [
        format!("/api/rooms/{}/members", room.id),
        format!("/api/rooms/{}/messages", room.id),
    ] {
        let resp = app.get_as(&mallory, &path).await;
        expect_error(resp, StatusCode::NOT_FOUND, ErrorCode::NotFound).await;
    }
    let sync: Value = app
        .post_as(
            &mallory,
            "/api/sync",
            Some(json!({"rooms": [{"room_id": room.id, "after": "0"}]})),
        )
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(sync["rooms"][0]["member"], false);
    assert_eq!(sync["rooms"][0]["messages"], json!([]));
}
