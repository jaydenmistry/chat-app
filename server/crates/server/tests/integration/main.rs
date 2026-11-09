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

mod admin;
mod app;
mod auth;
mod cleanup;
mod demo;
mod failures;
mod features;
mod github;
mod health;
mod ids;
mod infra;
mod limits;
mod maintenance;
mod messages;
mod migrations;
mod realtime;
mod rooms;
mod ws;

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
    health::metrics_report_activity,
    migrations::scylla_migrations_apply_once,
    ids::instances_get_distinct_worker_ids,
    ids::a_stolen_lease_stops_ids_and_moves_on,
    ids::shutdown_releases_the_lease,
    ids::quarantine_keeps_the_lease_alive,
    messages::history_pages_across_real_buckets,
    messages::insert_is_idempotent,
    messages::expired_messages_leave_empty_buckets,
    rooms::public_rooms_can_be_created_browsed_joined_and_left,
    rooms::private_rooms_are_invite_only,
    rooms::guests_cant_create_rooms,
    rooms::only_the_owner_removes_members,
    rooms::deleting_a_room_removes_everyone,
    rooms::one_dm_per_pair,
    rooms::non_members_see_nothing,
    realtime::upgrade_requires_session_origin_and_version,
    realtime::messages_cross_instances,
    realtime::retries_return_the_original_message,
    realtime::bad_sends_get_error_frames,
    realtime::removal_stops_delivery_on_every_instance,
    realtime::joining_routes_the_room_to_open_sockets,
    realtime::logout_closes_sockets_everywhere,
    realtime::sync_returns_missed_messages,
    realtime::resume_survives_clock_skew,
    realtime::typing_crosses_instances,
    realtime::presence_crosses_instances,
    realtime::guest_expiry_ends_the_socket_and_the_messages,
    realtime::graceful_shutdown_tells_clients_to_reconnect,
    features::edits_and_deletes_reach_everyone,
    features::edit_survives_a_replayed_insert,
    features::reactions_count_once_per_user,
    features::sync_returns_changes_to_loaded_messages,
    features::late_inserts_reach_sync_through_the_change_feed,
    features::read_marks_unread_counts_and_receipts,
    features::dm_reads_are_live_receipts,
    features::member_frames_announce_joins_and_leaves,
    features::search_respects_membership_edits_and_deletes,
    failures::redis_outage_then_resync,
    failures::instance_restart_loses_nothing,
    limits::signup_is_rate_limited_per_ip,
    limits::sends_are_rate_limited_per_user,
    limits::room_membership_is_capped,
    limits::sockets_are_capped_per_user,
    limits::sockets_are_capped_per_ip,
    cleanup::expired_guests_and_their_dms_are_deleted,
    cleanup::guest_cleanup_announces_user_expired,
    demo::demo_rooms_are_seeded_and_reset,
    admin::admin_bans_an_account,
    admin::admin_deletes_a_message,
    maintenance::reindex_rebuilds_search_entries,
    maintenance::reclaim_frees_a_deleted_rooms_scylla_space,
    auth::signup_creates_account_and_session,
    auth::signup_rejects_taken_email_and_handle,
    auth::signup_validates_fields,
    auth::login_checks_password,
    auth::logout_ends_the_session,
    auth::sessions_can_be_listed_and_revoked,
    auth::guest_session_ends_with_the_account,
    auth::state_changing_requests_need_an_allowed_origin,
    auth::malformed_requests_get_api_errors,
    auth::plain_http_uses_development_cookies,
    github::github_sign_in_with_pkce,
    github::github_callback_rejects_bad_state_and_nonce,
    github::github_disabled_without_config,
];

const TEST_TIMEOUT: Duration = Duration::from_secs(120);
/// How many tests run at once (`TEST_CONCURRENCY` overrides). Each starts
/// one or two servers against shared containers; the bound keeps CPU, memory
/// and connections well inside what a 4-core CI machine handles.
const DEFAULT_CONCURRENCY: usize = 8;

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
    // Server logs go to stderr, filtered by RUST_LOG (e.g. RUST_LOG=warn).
    if std::env::var_os("RUST_LOG").is_some() {
        server::telemetry::init(server::config::LogFormat::Pretty);
    }
    let failed = runtime.block_on(async {
        let infra = Arc::new(Infra::start().await);
        println!("running {} integration tests", selected.len());

        let concurrency = std::env::var("TEST_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_CONCURRENCY);
        let permits = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let mut running = tokio::task::JoinSet::new();
        for &(name, test) in &selected {
            let infra = Arc::clone(&infra);
            let permits = Arc::clone(&permits);
            running.spawn(async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .expect("semaphore is never closed");
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
