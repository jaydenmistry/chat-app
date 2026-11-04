//! Reading and writing the two cookies we use.
//!
//! When the app is served over HTTPS (production), both cookies use the
//! `__Host-` prefix: the browser only accepts such a cookie if it is `Secure`,
//! has `Path=/` and no `Domain`, so no other `*.jmistry.com` site can set or
//! overwrite it.
//!
//! Local development runs on `http://localhost`, where WebKit refuses
//! `Secure` cookies (Chromium and Firefox accept them). There the cookies drop
//! `Secure` and the prefix. The choice follows `PUBLIC_URL`, so an HTTPS
//! deployment can't end up with insecure cookies by a forgotten flag.

use std::time::Duration;

use axum::http::{HeaderMap, HeaderValue, header::COOKIE};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cookie {
    Session,
    /// Binds a GitHub sign-in to the browser that started it (login CSRF
    /// defence).
    OauthNonce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CookiePolicy {
    secure: bool,
}

impl CookiePolicy {
    /// Secure cookies exactly when the app is served over HTTPS.
    pub fn for_public_url(public_url: &str) -> Self {
        Self {
            secure: public_url.starts_with("https://"),
        }
    }

    pub fn is_secure(self) -> bool {
        self.secure
    }

    pub fn name(self, cookie: Cookie) -> &'static str {
        match (cookie, self.secure) {
            (Cookie::Session, true) => "__Host-session",
            (Cookie::Session, false) => "session",
            (Cookie::OauthNonce, true) => "__Host-oauth",
            (Cookie::OauthNonce, false) => "oauth",
        }
    }

    /// `SameSite=Lax` keeps the cookie off cross-site subrequests and POSTs
    /// but sends it on top-level navigations, which the GitHub redirect back
    /// needs.
    pub fn set(self, cookie: Cookie, value: &str, max_age: Duration) -> HeaderValue {
        let secure = if self.secure { "; Secure" } else { "" };
        HeaderValue::from_str(&format!(
            "{}={value}; Path=/; Max-Age={}; HttpOnly{secure}; SameSite=Lax",
            self.name(cookie),
            max_age.as_secs()
        ))
        .expect("cookie names and base64url values are valid header characters")
    }

    pub fn clear(self, cookie: Cookie) -> HeaderValue {
        self.set(cookie, "", Duration::ZERO)
    }

    /// The cookie's value, from any `Cookie` header.
    pub fn get(self, headers: &HeaderMap, cookie: Cookie) -> Option<&str> {
        let name = self.name(cookie);
        headers
            .get_all(COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(';'))
            .filter_map(|pair| pair.trim().split_once('='))
            .find_map(|(key, value)| (key == name).then_some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECURE: CookiePolicy = CookiePolicy { secure: true };
    const DEV: CookiePolicy = CookiePolicy { secure: false };

    #[test]
    fn https_means_host_prefixed_secure_cookies() {
        assert_eq!(
            CookiePolicy::for_public_url("https://chat.example.com"),
            SECURE
        );
        assert_eq!(CookiePolicy::for_public_url("http://localhost:5173"), DEV);
        assert_eq!(
            SECURE.set(Cookie::Session, "abc", Duration::from_secs(60)),
            "__Host-session=abc; Path=/; Max-Age=60; HttpOnly; Secure; SameSite=Lax"
        );
        assert!(
            SECURE
                .clear(Cookie::Session)
                .to_str()
                .unwrap()
                .starts_with("__Host-session=; Path=/; Max-Age=0;")
        );
    }

    #[test]
    fn plain_http_drops_secure_and_the_prefix() {
        assert_eq!(
            DEV.set(Cookie::OauthNonce, "n", Duration::from_secs(600)),
            "oauth=n; Path=/; Max-Age=600; HttpOnly; SameSite=Lax"
        );
    }

    #[test]
    fn finds_cookie_among_several_headers() {
        let mut headers = HeaderMap::new();
        headers.append(COOKIE, HeaderValue::from_static("theme=dark; other=1"));
        headers.append(COOKIE, HeaderValue::from_static("__Host-session=tok"));
        assert_eq!(SECURE.get(&headers, Cookie::Session), Some("tok"));
        // The development name is a different cookie.
        assert_eq!(DEV.get(&headers, Cookie::Session), None);
        // A cookie whose name merely contains the one we want doesn't match.
        headers.insert(COOKIE, HeaderValue::from_static("x__Host-session=evil"));
        assert_eq!(SECURE.get(&headers, Cookie::Session), None);
    }
}
