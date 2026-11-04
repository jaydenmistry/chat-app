//! Integration tests against real Postgres, Redis and ScyllaDB.
//!
//! Scylla takes tens of seconds to start, so the containers are started once
//! and shared. libtest can't do that cleanly: statics are never dropped, so
//! shared containers would outlive the test process. This binary therefore
//! has its own small runner (`harness = false` in Cargo.toml):
//!
//!   cargo test -p server --test integration            all tests
//!   cargo test -p server --test integration -- signup  tests whose name contains "signup"
//!
//! Tests run concurrently. Each gets its own Postgres database; Redis keys and
//! Scylla partitions are random per test, so the shared instances don't leak
//! state between tests.

mod app;
mod auth;
mod github;
mod health;
mod infra;
mod migrations;

use std::{future::Future, pin::Pin, process::ExitCode, sync::Arc, time::Duration};

use infra::Infra;

type TestFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type TestFn = fn(Arc<Infra>) -> TestFuture;

macro_rules! tests {
    ($($path:path),* $(,)?) => {
        &[$((stringify!($path), |infra| Box::pin($path(infra)))),*]
    };
}

const TESTS: &[(&str, TestFn)] = tests![
    health::healthz_is_always_ok,
    health::readyz_reports_dependencies,
    health::readyz_fails_when_postgres_is_down,
    health::request_ids_are_assigned_and_propagated,
    health::unknown_api_routes_are_json_404,
    migrations::scylla_migrations_apply_once,
    auth::signup_creates_account_and_session,
    auth::signup_rejects_taken_email_and_handle,
    auth::signup_validates_fields,
    auth::login_checks_password,
    auth::logout_ends_the_session,
    auth::sessions_can_be_listed_and_revoked,
    auth::guest_session_ends_with_the_account,
    auth::state_changing_requests_need_an_allowed_origin,
    auth::malformed_requests_get_api_errors,
    github::github_sign_in_with_pkce,
    github::github_callback_rejects_bad_state_and_nonce,
    github::github_disabled_without_config,
];

const TEST_TIMEOUT: Duration = Duration::from_secs(120);

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--list") {
        for (name, _) in TESTS {
            println!("{name}: test");
        }
        return ExitCode::SUCCESS;
    }
    let filters: Vec<&str> = args
        .iter()
        .map(String::as_str)
        .filter(|a| !a.starts_with('-'))
        .collect();
    let selected: Vec<_> = TESTS
        .iter()
        .filter(|(name, _)| filters.is_empty() || filters.iter().any(|f| name.contains(f)))
        .collect();
    // `cargo test some_unit_test` passes the filter to every test binary;
    // don't start containers when nothing here matches.
    if selected.is_empty() {
        println!("running 0 integration tests");
        return ExitCode::SUCCESS;
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("building the Tokio runtime");
    let failed = runtime.block_on(async {
        let infra = Arc::new(Infra::start().await);
        println!("running {} integration tests", selected.len());

        let mut running = tokio::task::JoinSet::new();
        for &(name, test) in &selected {
            let infra = Arc::clone(&infra);
            running.spawn(async move {
                let outcome = tokio::time::timeout(TEST_TIMEOUT, tokio::spawn(test(infra))).await;
                (*name, outcome)
            });
        }
        let mut failed = Vec::new();
        while let Some(joined) = running.join_next().await {
            let (name, outcome) = joined.expect("the runner task itself doesn't panic");
            match outcome {
                Ok(Ok(())) => println!("test {name} ... ok"),
                // The panic message was already printed by the panic hook.
                Ok(Err(_)) => {
                    println!("test {name} ... FAILED");
                    failed.push(name);
                }
                Err(_) => {
                    println!("test {name} ... FAILED (timed out after {TEST_TIMEOUT:?})");
                    failed.push(name);
                }
            }
        }

        // Dropping the last reference removes the containers. It must happen
        // here, inside the runtime, because removal is asynchronous.
        drop(Arc::into_inner(infra));
        failed
    });

    let passed = selected.len() - failed.len();
    if failed.is_empty() {
        println!("\nintegration test result: ok. {passed} passed");
        ExitCode::SUCCESS
    } else {
        println!(
            "\nintegration test result: FAILED. {passed} passed, {} failed:",
            failed.len()
        );
        for name in failed {
            println!("    {name}");
        }
        ExitCode::FAILURE
    }
}
