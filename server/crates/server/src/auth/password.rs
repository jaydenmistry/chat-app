//! Argon2id password hashing.
//!
//! Hashing is deliberately slow and memory-hungry (19 MiB per hash), so it
//! runs on Tokio's blocking thread pool instead of the async worker threads,
//! where it would stall every other request on that thread. A semaphore caps
//! concurrent hashes, so a login flood can't exhaust memory.

use std::sync::Arc;

use argon2::{
    Algorithm, Argon2, Params, Version,
    password_hash::{PasswordHasher as _, PasswordVerifier as _, phc::PasswordHash},
};
use tokio::sync::Semaphore;

use crate::http::error::AppError;

/// OWASP Password Storage Cheat Sheet minimum for Argon2id, checked
/// 2026-11-04: m = 19 MiB, t = 2, p = 1.
const MEMORY_KIB: u32 = 19 * 1024;
const ITERATIONS: u32 = 2;
const PARALLELISM: u32 = 1;

pub struct PasswordHasher {
    argon2: Argon2<'static>,
    permits: Arc<Semaphore>,
    /// A real hash of a random password, verified against when the email is
    /// unknown, so "no such user" takes as long as "wrong password".
    dummy_hash: String,
}

impl PasswordHasher {
    pub fn new(max_concurrent: usize) -> Self {
        let params = Params::new(MEMORY_KIB, ITERATIONS, PARALLELISM, None)
            .expect("constant Argon2 parameters are valid");
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
        let dummy_hash = argon2
            .hash_password(super::token::random_string(32).as_bytes())
            .expect("hashing a random password succeeds")
            .to_string();
        Self {
            argon2,
            permits: Arc::new(Semaphore::new(max_concurrent)),
            dummy_hash,
        }
    }

    /// Returns a PHC string (`$argon2id$v=19$m=19456,t=2,p=1$salt$hash`) with
    /// a fresh random salt.
    pub async fn hash(&self, password: String) -> Result<String, AppError> {
        let argon2 = self.argon2.clone();
        self.run_blocking(move || {
            argon2
                .hash_password(password.as_bytes())
                .map(|hash| hash.to_string())
                .map_err(|err| anyhow::anyhow!("hashing failed: {err}"))
        })
        .await
    }

    /// Checks `password` against `stored`. With `None` (unknown account) it
    /// verifies against the dummy hash and returns false.
    pub async fn verify(&self, password: String, stored: Option<String>) -> Result<bool, AppError> {
        let argon2 = self.argon2.clone();
        let known = stored.is_some();
        let stored = stored.unwrap_or_else(|| self.dummy_hash.clone());
        self.run_blocking(move || {
            let parsed = PasswordHash::new(&stored)
                .map_err(|err| anyhow::anyhow!("stored hash is malformed: {err}"))?;
            // The parameters come from the stored hash, so old hashes keep
            // verifying if the constants above are raised later.
            let matches = argon2.verify_password(password.as_bytes(), &parsed).is_ok();
            Ok(known && matches)
        })
        .await
    }

    async fn run_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
    ) -> Result<T, AppError> {
        // The permit moves into the blocking closure. If the client hangs up
        // and this future is dropped, the hash still runs to completion, and
        // the permit is released only then, so the cap stays accurate.
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(AppError::internal)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(AppError::internal)?
        .map_err(AppError::Internal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hash_then_verify() {
        let hasher = PasswordHasher::new(2);
        let hash = hasher.hash("correct horse".into()).await.unwrap();
        assert!(hash.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"));
        assert!(
            hasher
                .verify("correct horse".into(), Some(hash.clone()))
                .await
                .unwrap()
        );
        assert!(
            !hasher
                .verify("wrong horse".into(), Some(hash))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn unknown_account_never_verifies() {
        let hasher = PasswordHasher::new(1);
        assert!(!hasher.verify("anything".into(), None).await.unwrap());
    }

    #[tokio::test]
    async fn salts_differ() {
        let hasher = PasswordHasher::new(2);
        let a = hasher.hash("same".into()).await.unwrap();
        let b = hasher.hash("same".into()).await.unwrap();
        assert_ne!(a, b);
    }
}
