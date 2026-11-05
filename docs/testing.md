# Testing

## How to run

```bash
# Rust unit tests and integration tests (needs Docker; containers start automatically)
cd server && cargo test --workspace

# Only the integration tests, filtered by name
cd server && cargo test -p server --test integration -- signup

# Web unit tests, lint and typecheck
cd web && pnpm test && pnpm lint && pnpm typecheck

# End-to-end, against the running stack (`docker compose up` first)
cd web && pnpm e2e
```

If Docker Hub's anonymous pull limit blocks image pulls, set
`IMAGE_REGISTRY=mirror.gcr.io` (CI does).

## What runs where

| Layer | Tool | What it covers |
|---|---|---|
| Unit (Rust) | `cargo test` | config, validation, tokens and hashing, Argon2 parameters, PKCE (RFC 7636 test vector), cookies, Origin matching, GitHub handles, Scylla migration files, protocol JSON shapes, message IDs and bucket math, the ID generator's clock handling, history pagination against an in-memory store (including 50 randomized layouts), the permission rule table, the socket hub (routing, reference-counted subscriptions, slow consumers) |
| Integration (Rust) | custom runner in `server/crates/server/tests/integration` | real Postgres 18, Redis 8 and ScyllaDB 2026.3 in containers, with real server instances on random ports. Accounts and sessions; rooms, invites and DMs; the Scylla message store and pagination over real partitions and expired rows; worker-ID leases; and the WebSocket gateway with **two instances**: fan-out, idempotent retries, removal, joins, logout, sync, clock skew, typing, presence, guest expiry |
| Failure (Rust) | same runner | Redis frozen mid-conversation (`docker pause`) with clients on two instances; an instance shut down under its clients. Every acknowledged message must reach every client exactly once |
| Unit (web) | Vitest + Testing Library | API client error handling, sign-in form error display, session labels |
| End to end | Playwright (Chromium, Firefox, WebKit) | the full compose stack (Vite, Traefik, two server instances): guest sign-in, account creation, sign-in with a wrong and a right password, a taken handle, revoking another device's session |

The integration tests share one container per store and start them once
(Scylla takes tens of seconds). Each test gets its own Postgres database
copied from a migrated template. Redis keys and Scylla partitions are random
per test, so tests don't see each other's data.

## Planted-bug checks

A test that targets a bug class must fail when that bug is present. For each
one, the bug was planted in a scratch `git worktree`, the test was run, and
the failure is recorded here. The planted change was never committed.

| Date | Bug class | Planted change | Test | Result |
|---|---|---|---|---|
| 2026-11-04 | CSRF (permission) | `require_allowed_origin` lets every request through (`if true \|\| ...`) | `auth::state_changing_requests_need_an_allowed_origin` | failed: forged sign-up returned `201`, expected `403` |
| 2026-11-04 | Session revocation | logout revokes the Postgres row but doesn't delete the Redis session | `auth::logout_ends_the_session` | failed: `/api/me` with the old cookie returned the user, expected `401` |
| 2026-11-04 | Cross-user access | `sessions::revoke` drops the `user_id = $2` condition | `auth::sessions_can_be_listed_and_revoked` | failed: another user's revoke returned `204`, expected `404` |
| 2026-11-04 | Login CSRF | GitHub callback skips the browser-nonce comparison | `github::github_callback_rejects_bad_state_and_nonce` | failed: callback without the nonce cookie redirected to `/` (signed in), expected `/?auth_error=github` |

### Phase 2

| Date | Bug class | Planted change | Test | Result |
|---|---|---|---|---|
| 2026-11-05 | Duplicates | the send path ignores a reused idempotency key, so every attempt is a new message | `realtime::retries_return_the_original_message` | failed: the retry was not marked `duplicate` |
| 2026-11-05 | Gaps | sync starts exactly at the client's cursor (no overlap window) | `realtime::resume_survives_clock_skew` | failed: "message lost on resume" (the message from the instance with the slower clock) |
| 2026-11-05 | Permission leak (frames) | the send path skips the per-frame membership check | `realtime::removal_stops_delivery_on_every_instance` | failed: a removed user's send was accepted |
| 2026-11-05 | Permission leak (fan-out) | a removal event doesn't unroute the removed user's sockets | `realtime::removal_stops_delivery_on_every_instance` | failed: timed out waiting for `room.left` |
| 2026-11-05 | Missed fan-out | after reconnecting to Redis, instances don't tell clients to resync | `failures::redis_outage_then_resync` | failed: timed out waiting for `resync` |
| 2026-11-05 | History gap | pagination ends the room at the first bucket that returns no rows | `messages::tests::recorded_but_empty_buckets_are_skipped` | failed: only the newest message came back |
| 2026-11-05 | History gap | pagination ends the room when nothing was found yet | `messages::tests::empty_newest_buckets_dont_hide_older_history` | failed: empty first page (see below) |

The last plant was first run against `recorded_but_empty_buckets_are_skipped`
and was **not caught**: that fixture always has messages in its newest
bucket, so "nothing found yet" never happens there. That was a real gap in
coverage. A room whose most recent messages were all a guest's has expired
buckets at its newest end. `empty_newest_buckets_dont_hide_older_history`
was added for that layout, and it catches the plant.

Not covered by an integration test: a slow consumer being disconnected.
TCP buffers absorb far more than the 256-frame queue before the server's
writer blocks, so a test would need megabytes of traffic. The hub's unit test
`a_full_queue_closes_that_connection_only` covers the decision itself.
