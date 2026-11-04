//! Random tokens and their hashes.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::Rng;
use sha2::{Digest, Sha256};
use storage::sessions::TokenHash;

/// `bytes` random bytes from the thread-local CSPRNG (ChaCha, seeded from the
/// OS), encoded as unpadded base64url.
pub fn random_string(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut buf);
    URL_SAFE_NO_PAD.encode(&buf)
}

/// A new session token (for the cookie) and its hash (for storage).
pub fn new_session_token() -> (String, TokenHash) {
    let token = random_string(32);
    let hash = hash_session_token(&token).expect("a fresh token is well-formed");
    (token, hash)
}

/// Hashes a token taken from a cookie. `None` if it isn't 32 bytes of
/// base64url, which no token we issued can be.
pub fn hash_session_token(token: &str) -> Option<TokenHash> {
    let raw = URL_SAFE_NO_PAD.decode(token).ok()?;
    if raw.len() != 32 {
        return None;
    }
    Some(sha256(&raw))
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

/// Compares two byte strings in time that depends only on their lengths, so
/// an attacker can't learn a secret one matching byte at a time.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_unique_and_round_trip() {
        let (token_a, hash_a) = new_session_token();
        let (token_b, hash_b) = new_session_token();
        assert_ne!(token_a, token_b);
        assert_ne!(hash_a, hash_b);
        assert_eq!(token_a.len(), 43);
        assert_eq!(hash_session_token(&token_a), Some(hash_a));
    }

    #[test]
    fn malformed_tokens_have_no_hash() {
        assert_eq!(hash_session_token(""), None);
        assert_eq!(hash_session_token("not base64!"), None);
        assert_eq!(hash_session_token(&random_string(16)), None);
    }

    #[test]
    fn constant_time_eq_compares_contents() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }
}
