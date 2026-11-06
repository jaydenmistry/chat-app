//! A WebSocket test client: connects like a browser would, answers pings,
//! and lets tests wait for specific frames.

use std::{collections::VecDeque, time::Duration};

use futures_util::{SinkExt, StreamExt};
use protocol::{
    api::Message,
    frames::{ClientFrame, SUBPROTOCOL, ServerFrame},
};
use reqwest::StatusCode;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{self, Message as WsMessage, client::IntoClientRequest, http::HeaderValue},
};
use uuid::Uuid;

use crate::{app::TestApp, infra::TEST_ORIGIN};

const WAIT: Duration = Duration::from_secs(5);

pub struct Client {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// Frames received but not yet consumed by a test.
    pending: VecDeque<ServerFrame>,
    pub user_id: Uuid,
    next_req: u32,
}

/// Why a connection attempt failed: the HTTP status of the refused upgrade.
#[derive(Debug, PartialEq, Eq)]
pub struct Refused(pub StatusCode);

pub async fn connect_with(
    app: &TestApp,
    cookie: Option<&str>,
    origin: Option<&str>,
    subprotocol: Option<&str>,
) -> Result<Client, Refused> {
    let mut request = format!("ws://{}/ws", app.addr)
        .into_client_request()
        .unwrap();
    let headers = request.headers_mut();
    if let Some(cookie) = cookie {
        headers.insert("cookie", HeaderValue::from_str(cookie).unwrap());
    }
    if let Some(origin) = origin {
        headers.insert("origin", HeaderValue::from_str(origin).unwrap());
    }
    if let Some(protocol) = subprotocol {
        headers.insert(
            "sec-websocket-protocol",
            HeaderValue::from_str(protocol).unwrap(),
        );
    }
    let (socket, _) = match tokio_tungstenite::connect_async(request).await {
        Ok(ok) => ok,
        Err(tungstenite::Error::Http(response)) => {
            return Err(Refused(
                StatusCode::from_u16(response.status().as_u16()).unwrap(),
            ));
        }
        Err(err) => panic!("websocket connect failed: {err}"),
    };
    let mut client = Client {
        socket,
        pending: VecDeque::new(),
        user_id: Uuid::nil(),
        next_req: 0,
    };
    match client.next().await {
        ServerFrame::Hello { user_id, .. } => client.user_id = user_id,
        other => panic!("expected hello, got {other:?}"),
    }
    Ok(client)
}

/// Connects as a browser on our origin would.
pub async fn connect(app: &TestApp, cookie: &str) -> Client {
    connect_with(app, Some(cookie), Some(TEST_ORIGIN), Some(SUBPROTOCOL))
        .await
        .expect("connection refused")
}

/// Connects and reports the outcome of the first frame: `Ok` with the client
/// once `hello` arrives, or `Err(close_code)` if the server closes the socket
/// instead (how the connection caps refuse an upgrade). `client_ip` sets the
/// `x-test-ip` header, for exercising the per-IP cap on an isolated key.
pub async fn connect_capped(
    app: &TestApp,
    cookie: &str,
    client_ip: Option<&str>,
) -> Result<Client, u16> {
    let mut request = format!("ws://{}/ws", app.addr)
        .into_client_request()
        .unwrap();
    let headers = request.headers_mut();
    headers.insert("cookie", HeaderValue::from_str(cookie).unwrap());
    headers.insert("origin", HeaderValue::from_str(TEST_ORIGIN).unwrap());
    headers.insert(
        "sec-websocket-protocol",
        HeaderValue::from_str(SUBPROTOCOL).unwrap(),
    );
    if let Some(ip) = client_ip {
        headers.insert("x-test-ip", HeaderValue::from_str(ip).unwrap());
    }
    let (socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("the upgrade itself succeeds; the cap closes the socket after");
    let mut client = Client {
        socket,
        pending: VecDeque::new(),
        user_id: Uuid::nil(),
        next_req: 0,
    };
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let message = tokio::time::timeout_at(deadline, client.socket.next())
            .await
            .expect("timed out waiting for the first frame");
        match message {
            Some(Ok(WsMessage::Text(text))) => {
                match serde_json::from_str::<ServerFrame>(&text).unwrap() {
                    ServerFrame::Hello { user_id, .. } => {
                        client.user_id = user_id;
                        return Ok(client);
                    }
                    ServerFrame::Ping => {
                        client.send(&ClientFrame::Pong).await;
                    }
                    other => panic!("expected hello or a close, got {other:?}"),
                }
            }
            Some(Ok(WsMessage::Close(frame))) => {
                return Err(frame.map(|f| f.code.into()).unwrap_or(0));
            }
            None | Some(Err(_)) => return Err(0),
            Some(Ok(_)) => {}
        }
    }
}

impl Client {
    pub async fn send(&mut self, frame: &ClientFrame) {
        let text = serde_json::to_string(frame).unwrap();
        self.socket.send(WsMessage::text(text)).await.unwrap();
    }

    pub async fn send_raw(&mut self, text: &str) {
        self.socket
            .send(WsMessage::text(text.to_string()))
            .await
            .unwrap();
    }

    /// The next frame, answering pings along the way. Panics after 5 s.
    pub async fn next(&mut self) -> ServerFrame {
        if let Some(frame) = self.pending.pop_front() {
            return frame;
        }
        self.read(WAIT)
            .await
            .expect("timed out waiting for a frame")
    }

    async fn read(&mut self, wait: Duration) -> Option<ServerFrame> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let message = tokio::time::timeout_at(deadline, self.socket.next())
                .await
                .ok()?;
            match message {
                Some(Ok(WsMessage::Text(text))) => {
                    let frame: ServerFrame = serde_json::from_str(&text).unwrap();
                    if frame == ServerFrame::Ping {
                        self.send(&ClientFrame::Pong).await;
                        continue;
                    }
                    return Some(frame);
                }
                Some(Ok(WsMessage::Close(_))) | None | Some(Err(_)) => return None,
                Some(Ok(_)) => continue,
            }
        }
    }

    /// Waits for a frame matching `want`, keeping the others for later.
    pub async fn expect(&mut self, what: &str, want: impl Fn(&ServerFrame) -> bool) -> ServerFrame {
        if let Some(pos) = self.pending.iter().position(&want) {
            return self.pending.remove(pos).unwrap();
        }
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let frame = self
                .read(left)
                .await
                .unwrap_or_else(|| panic!("timed out waiting for {what}; had {:?}", self.pending));
            if want(&frame) {
                return frame;
            }
            self.pending.push_back(frame);
        }
    }

    /// Asserts no frame matching `unwanted` arrives within `wait`.
    pub async fn expect_none(
        &mut self,
        what: &str,
        wait: Duration,
        unwanted: impl Fn(&ServerFrame) -> bool,
    ) {
        assert!(
            !self.pending.iter().any(&unwanted),
            "unexpected {what}: {:?}",
            self.pending
        );
        let deadline = tokio::time::Instant::now() + wait;
        while let Some(frame) = self
            .read(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            assert!(!unwanted(&frame), "unexpected {what}: {frame:?}");
            self.pending.push_back(frame);
        }
    }

    /// The next `message.created` for a message matching `want`.
    pub async fn expect_message(&mut self, content: &str) -> Message {
        let frame = self
            .expect(&format!("message {content:?}"), |f| {
                matches!(f, ServerFrame::MessageCreated { message } if message.content == content)
            })
            .await;
        match frame {
            ServerFrame::MessageCreated { message } => message,
            _ => unreachable!(),
        }
    }

    /// Sends a message with a fresh idempotency key and waits for its ack.
    pub async fn send_message(&mut self, room_id: Uuid, content: &str) -> Message {
        self.send_with_key(room_id, Uuid::now_v7(), content).await.0
    }

    /// Sends and waits for the reply to this request: the acked message and
    /// whether it was a duplicate. Panics on an error reply.
    pub async fn send_with_key(
        &mut self,
        room_id: Uuid,
        key: Uuid,
        content: &str,
    ) -> (Message, bool) {
        match self.request_send(room_id, key, content).await {
            ServerFrame::MessageAck {
                message, duplicate, ..
            } => (message, duplicate),
            other => panic!("send failed: {other:?}"),
        }
    }

    /// Sends and returns whatever the server replied (ack or error).
    pub async fn request_send(&mut self, room_id: Uuid, key: Uuid, content: &str) -> ServerFrame {
        self.next_req += 1;
        let req_id = format!("r{}", self.next_req);
        self.send(&ClientFrame::MessageSend {
            req_id: req_id.clone(),
            room_id,
            idempotency_key: key,
            content: content.into(),
        })
        .await;
        self.expect("the reply", |f| match f {
            ServerFrame::MessageAck { req_id: r, .. } => *r == req_id,
            ServerFrame::Error {
                req_id: Some(r), ..
            } => *r == req_id,
            _ => false,
        })
        .await
    }

    /// Sends a request built around a fresh `req_id` and returns the reply
    /// to it: `ok` or an error.
    pub async fn request(&mut self, frame: impl FnOnce(String) -> ClientFrame) -> ServerFrame {
        self.next_req += 1;
        let req_id = format!("r{}", self.next_req);
        self.send(&frame(req_id.clone())).await;
        self.expect("the reply", |f| match f {
            ServerFrame::Ok { req_id: r } => *r == req_id,
            ServerFrame::Error {
                req_id: Some(r), ..
            } => *r == req_id,
            _ => false,
        })
        .await
    }

    /// Waits for the server to close the connection; returns the close code.
    pub async fn closed(&mut self) -> Option<u16> {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let message = tokio::time::timeout_at(deadline, self.socket.next())
                .await
                .expect("connection was not closed");
            match message {
                Some(Ok(WsMessage::Close(frame))) => return frame.map(|f| f.code.into()),
                None | Some(Err(_)) => return None,
                Some(Ok(_)) => continue,
            }
        }
    }
}
