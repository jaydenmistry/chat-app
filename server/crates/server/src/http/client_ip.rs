//! The client's IP address, for session metadata and (in Phase 4) rate limits.

use std::net::{IpAddr, SocketAddr};

use axum::{
    extract::{ConnectInfo, FromRequestParts},
    http::request::Parts,
};

use crate::state::AppState;

/// From `CLIENT_IP_HEADER` when configured (behind Cloudflare that is
/// `CF-Connecting-IP`), otherwise the TCP peer. Trusting the header is safe
/// only when nothing can reach the server except through the proxy that sets
/// it; `deploy/README.md` states that requirement.
pub struct ClientIp(pub IpAddr);

impl FromRequestParts<AppState> for ClientIp {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let from_header = state.config.client_ip_header.as_deref().and_then(|name| {
            let value = parts.headers.get(name)?.to_str().ok()?;
            // X-Forwarded-For style lists put the original client first.
            value.split(',').next()?.trim().parse().ok()
        });
        let peer = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip());
        Ok(Self(
            from_header.or(peer).unwrap_or(IpAddr::from([0, 0, 0, 0])),
        ))
    }
}
