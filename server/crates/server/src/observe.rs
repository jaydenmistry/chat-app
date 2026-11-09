//! Metrics (docs/design.md 13).
//!
//! A single process-wide Prometheus recorder, rendered by the app's own
//! `GET /metrics` route (not a separate port). The recorder is global to the
//! `metrics` crate, so it is installed once per process; the in-process
//! integration tests run many server instances against that one recorder,
//! which is why installation is idempotent.
//!
//! `/metrics` is served on the app port, so the reverse proxy must not expose
//! it publicly (the deploy guide says so).

use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Installs the global Prometheus recorder on the first call and returns the
/// handle that renders it. Later calls return the same handle.
pub fn install() -> &'static PrometheusHandle {
    HANDLE.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("installing the Prometheus recorder")
    })
}

/// The `/metrics` body: the Prometheus text exposition, or empty before the
/// recorder is installed (which `serve` does at startup).
pub async fn render() -> String {
    HANDLE
        .get()
        .map(PrometheusHandle::render)
        .unwrap_or_default()
}

/// A socket opened or closed (`ws_connections_open`).
pub fn connection_opened() {
    metrics::gauge!("ws_connections_open").increment(1.0);
}
pub fn connection_closed() {
    metrics::gauge!("ws_connections_open").decrement(1.0);
}

/// A message was stored and fanned out (`messages_sent_total`).
pub fn message_sent() {
    metrics::counter!("messages_sent_total").increment(1);
}

/// A request was refused by a rate limit (`rate_limit_rejections_total{action}`).
pub fn rate_limit_rejected(action: &str) {
    metrics::counter!("rate_limit_rejections_total", "action" => action.to_string()).increment(1);
}

/// A slow consumer's socket was closed (`slow_consumer_disconnects_total`).
pub fn slow_consumer_disconnected() {
    metrics::counter!("slow_consumer_disconnects_total").increment(1);
}

/// How far behind real time the search indexer is (`index_lag_seconds`): the
/// age of the entry it just handled.
pub fn index_lag(seconds: f64) {
    metrics::gauge!("index_lag_seconds").set(seconds);
}

/// Whether this instance currently has no usable worker-ID lease
/// (`worker_id_fenced`): 1 while fenced (lease lost or clock-skewed), 0 when
/// issuing IDs.
pub fn worker_id_fenced(fenced: bool) {
    metrics::gauge!("worker_id_fenced").set(f64::from(u8::from(fenced)));
}
