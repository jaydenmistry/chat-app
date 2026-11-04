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
| Unit (Rust) | `cargo test` | config parsing and validation, input validation, tokens and hashing, Argon2 parameters, PKCE (RFC 7636 test vector), cookie building and parsing, Origin matching, GitHub handle derivation, Scylla migration file checks, protocol JSON shapes |
| Integration (Rust) | custom runner in `server/crates/server/tests/integration` | real Postgres 18, Redis 8 and ScyllaDB 2026.3 in containers, with real server instances on random ports: sign-up, login, logout, session listing and revocation, guest expiry, Origin enforcement, GitHub OAuth against a fake GitHub that enforces PKCE, health and readiness, Scylla migrations |
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
