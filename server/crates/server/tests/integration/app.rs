//! A real server instance on a random port, plus request helpers.

use std::net::SocketAddr;

use protocol::api::{ApiError, ErrorCode, Me};
use reqwest::{Response, StatusCode, header};
use serde_json::{Value, json};
use server::config::Config;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

use crate::infra::{Infra, TEST_ORIGIN};

pub struct TestApp {
    pub addr: SocketAddr,
    /// Shared by a second instance started with `spawn_peer`.
    pub database_url: String,
    pub client: reqwest::Client,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}

impl TestApp {
    pub async fn spawn(infra: &Infra) -> Self {
        Self::spawn_with(infra, |_| {}).await
    }

    /// Starts a server with its own database and a config adjusted by `tweak`.
    pub async fn spawn_with(infra: &Infra, tweak: impl FnOnce(&mut Config)) -> Self {
        let database_url = infra.fresh_database().await;
        Self::spawn_on(infra, database_url, tweak).await
    }

    /// A second instance of the same deployment: same Postgres database (and,
    /// like every test app, the same Redis and Scylla).
    pub async fn spawn_peer(&self, infra: &Infra, tweak: impl FnOnce(&mut Config)) -> Self {
        Self::spawn_on(infra, self.database_url.clone(), tweak).await
    }

    async fn spawn_on(
        infra: &Infra,
        database_url: String,
        tweak: impl FnOnce(&mut Config),
    ) -> Self {
        let mut config = infra.config(database_url.clone());
        tweak(&mut config);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, stop) = oneshot::channel::<()>();
        let task = tokio::spawn(server::serve(config, listener, async {
            let _ = stop.await;
        }));
        let app = Self {
            addr,
            database_url,
            // No redirects, so tests can inspect them; no cookie store, so
            // tests pass cookies explicitly and can assert on them.
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            shutdown: Some(shutdown),
            task: Some(task),
        };
        app.wait_until_serving().await;
        app
    }

    /// The listener is bound before `serve` connects to the stores, so early
    /// requests would just queue; polling makes startup failures visible.
    /// Also waits for the worker-ID lease, so tests can send messages.
    async fn wait_until_serving(&self) {
        for _ in 0..100 {
            if let Ok(resp) = self.get("/readyz").send().await
                && let Ok(body) = resp.json::<Value>().await
                && body["ids"] == true
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("server did not start serving");
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    pub fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.client.get(self.url(path))
    }

    /// A POST as a browser on our origin would send it.
    pub fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.client
            .post(self.url(path))
            .header(header::ORIGIN, TEST_ORIGIN)
    }

    pub fn delete(&self, path: &str) -> reqwest::RequestBuilder {
        self.client
            .delete(self.url(path))
            .header(header::ORIGIN, TEST_ORIGIN)
    }

    /// Signs up a password account and returns the user and session cookie.
    pub async fn signup(&self, email: &str, handle: &str) -> (Me, String) {
        let resp = self
            .post("/api/auth/signup")
            .json(&json!({
                "email": email,
                "password": "correct horse battery",
                "handle": handle,
                "display_name": "Test User",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED, "signup failed");
        let cookie = session_cookie(&resp).expect("signup sets a session cookie");
        (resp.json().await.unwrap(), cookie)
    }

    pub async fn login(&self, email: &str, password: &str) -> Response {
        self.post("/api/auth/login")
            .json(&json!({"email": email, "password": password}))
            .send()
            .await
            .unwrap()
    }

    /// GET /api/me with the given cookie: the user, or the error status.
    pub async fn me(&self, cookie: &str) -> Result<Me, StatusCode> {
        let resp = self
            .get("/api/me")
            .header(header::COOKIE, cookie)
            .send()
            .await
            .unwrap();
        match resp.status() {
            StatusCode::OK => Ok(resp.json().await.unwrap()),
            status => Err(status),
        }
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        // The server task finishes by itself after the signal; a server that
        // crashed shows up as a failing request in the test, not here.
        drop(self.task.take());
    }
}

/// Every `Set-Cookie` header value on a response.
pub fn set_cookies(resp: &Response) -> Vec<String> {
    resp.headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect()
}

/// The full `Set-Cookie` value for cookie `name`, if set.
pub fn set_cookie(resp: &Response, name: &str) -> Option<String> {
    set_cookies(resp)
        .into_iter()
        .find(|c| c.starts_with(&format!("{name}=")))
}

/// `__Host-session=<token>`, ready to send back in a `Cookie` header.
pub fn session_cookie(resp: &Response) -> Option<String> {
    let full = set_cookie(resp, "__Host-session")?;
    let pair = full.split(';').next()?.to_string();
    // A cleared cookie has an empty value; that is not a session.
    (!pair.ends_with('=')).then_some(pair)
}

/// Asserts the response is an API error with this status and code, and
/// returns the body.
pub async fn expect_error(resp: Response, status: StatusCode, code: ErrorCode) -> ApiError {
    assert_eq!(resp.status(), status);
    let body: Value = resp.json().await.unwrap();
    let error: ApiError = serde_json::from_value(body.clone())
        .unwrap_or_else(|_| panic!("not an ApiError body: {body}"));
    assert_eq!(error.code, code, "{body}");
    error
}
