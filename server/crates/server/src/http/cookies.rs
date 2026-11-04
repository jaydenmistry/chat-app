//! Reading and writing the two cookies we use.
//!
//! Both use the `__Host-` prefix: the browser only accepts such a cookie if
//! it is `Secure`, has `Path=/` and no `Domain`, so no other `*.jmistry.com`
//! site can set or overwrite it.

use std::time::Duration;

use axum::http::{HeaderMap, HeaderValue, header::COOKIE};

pub const SESSION: &str = "__Host-session";
/// Binds a GitHub sign-in to the browser that started it (login CSRF defence).
pub const OAUTH_NONCE: &str = "__Host-oauth";

/// `SameSite=Lax` keeps the cookie off cross-site subrequests and POSTs but
/// sends it on top-level navigations, which the GitHub redirect back needs.
pub fn set(name: &str, value: &str, max_age: Duration) -> HeaderValue {
    HeaderValue::from_str(&format!(
        "{name}={value}; Path=/; Max-Age={}; HttpOnly; Secure; SameSite=Lax",
        max_age.as_secs()
    ))
    .expect("cookie names and base64url values are valid header characters")
}

pub fn clear(name: &str) -> HeaderValue {
    set(name, "", Duration::ZERO)
}

/// The value of cookie `name`, from any `Cookie` header.
pub fn get<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find_map(|(key, value)| (key == name).then_some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_cookie_has_all_attributes() {
        let value = set(SESSION, "abc", Duration::from_secs(60));
        assert_eq!(
            value,
            "__Host-session=abc; Path=/; Max-Age=60; HttpOnly; Secure; SameSite=Lax"
        );
        assert!(
            clear(SESSION)
                .to_str()
                .unwrap()
                .starts_with("__Host-session=; Path=/; Max-Age=0;")
        );
    }

    #[test]
    fn finds_cookie_among_several_headers() {
        let mut headers = HeaderMap::new();
        headers.append(COOKIE, HeaderValue::from_static("theme=dark; other=1"));
        headers.append(COOKIE, HeaderValue::from_static("__Host-session=tok"));
        assert_eq!(get(&headers, SESSION), Some("tok"));
        assert_eq!(get(&headers, "theme"), Some("dark"));
        assert_eq!(get(&headers, "missing"), None);
        // A cookie whose name merely contains the one we want doesn't match.
        headers.insert(COOKIE, HeaderValue::from_static("x__Host-session=evil"));
        assert_eq!(get(&headers, SESSION), None);
    }
}
