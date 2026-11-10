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

# Load test, against a deployed stack (not the homelab); see load/README.md
k6 run -e BASE_URL=https://chat.example.com load/chat.js
```

If Docker Hub's anonymous pull limit blocks image pulls, set
`IMAGE_REGISTRY=mirror.gcr.io` (CI does).

## What runs where

| Layer | Tool | What it covers |
|---|---|---|
| Unit (Rust) | `cargo test` | config, validation, tokens and hashing, Argon2 parameters, PKCE (RFC 7636 test vector), cookies, Origin matching, GitHub handles, Scylla migration files, protocol JSON shapes, message IDs and bucket math, the ID generator's clock handling, history pagination against an in-memory store (including 50 randomized layouts), the permission rule table, the socket hub (routing, reference-counted subscriptions, slow consumers) |
| Integration (Rust) | custom runner in `server/crates/server/tests/integration` | real Postgres 18, Redis 8 and ScyllaDB 2026.3 in containers, with real server instances on random ports. Accounts and sessions; rooms, invites and DMs; the Scylla message store and pagination over real partitions and expired rows; worker-ID leases; and the WebSocket gateway with **two instances**: fan-out, idempotent retries, removal, joins, logout, sync, clock skew, typing, presence, guest expiry. Phase 3: edits and deletes (author-only, final, surviving a replayed insert), reactions across instances, the change feed in sync including late inserts, unread counts, read marks and receipts, member frames, and search (membership, edits, deletes, on a dedicated Redis so other tests' indexers stay out of it). Phase 4: rate limits (sign-up refused per IP after the burst, sends refused per user), the per-user room membership cap (create and join refused at the cap), and the global socket caps (the socket over the per-user cap closed with `4029`, a freed slot admitting the next; the per-IP cap refusing across users); and guest cleanup (the storage pass deletes an expired guest, their DM rooms and memberships while leaving others alone; the background job announces `user.expired` to a member of the guest's room); and demo rooms (the reset job seeds a demo room, raises its history floor and sets its expiry, and announces `room.reset` to a connected member when the room's day is forced to end); and the admin command (`ban` closes the account's sockets, kills its session and refuses its next login; `delete-message` removes a message through the same `message.deleted` fan-out and history as an author's delete); and graceful shutdown (a draining instance sends each socket a `reconnect` frame, then closes it with `1012`); and metrics (`/metrics` renders Prometheus text, with `ws_connections_open` and `messages_sent_total` moving with real activity, and the `fanout`, `pg`, `scylla` and `redis` timing histograms registered); and the maintenance commands (`reindex` rebuilds a room's search entries after the index rows are dropped; `reclaim` deletes a deleted room's Scylla partitions and refuses a live one) |
| Failure (Rust) | same runner | Redis frozen mid-conversation (`docker pause`) with clients on two instances; an instance shut down under its clients. Every acknowledged message must reach every client exactly once |
| Unit (web) | Vitest + Testing Library | API client error handling, sign-in form error display, session labels; the chat reducer (merge by ID and version, deletes are final, updates that arrive before their message, reaction ordering by change ID, change cursors, unread counts and the 99+ cap, read positions, dropping an expired guest's messages, forgetting a reset demo room's messages); the realtime client (reconnects, honouring the server's spread-out `reconnect` delay, at-least-once sends, request replies for edits, deletes and reactions) |
| End to end | Playwright (Chromium, Firefox, WebKit) | the full compose stack (Vite, Traefik, two server instances): guest sign-in, account creation, sign-in with a wrong and a right password, a taken handle, revoking another device's session; two users chatting, typing and presence, offline sends, history paging; edits, deletes and reactions across two users; unread badges, "Seen by" and DM "Seen"; search limited to the user's rooms |

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

A bug none of these caught, found by running the compose stack: the
worker-ID lease wasn't renewed during its quarantine. With the production
timings (quarantine as long as the lease TTL) the key expired just as the
quarantine ended, and the instance re-acquired forever, never ready. The
integration tests used a zero quarantine for speed, so they never exercised
it. `ids::quarantine_keeps_the_lease_alive` now uses a quarantine longer than
the TTL; against the old code it fails with "lease never became ready".

Not covered by an integration test: a slow consumer being disconnected.
TCP buffers absorb far more than the 256-frame queue before the server's
writer blocks, so a test would need megabytes of traffic. The hub's unit test
`a_full_queue_closes_that_connection_only` covers the decision itself.

### Phase 3

| Date | Bug class | Planted change | Test | Result |
|---|---|---|---|---|
| 2026-11-05 | Lost edit | the insert is written at the server's current time instead of the ID's timestamp | `features::edit_survives_a_replayed_insert` | failed: "the replay overwrote the edit" |
| 2026-11-05 | Missed change on resume | edits, deletes and reactions don't write the `room_changes` row | `features::sync_returns_changes_to_loaded_messages` | failed: the edited message missing from `changed` |
| 2026-11-05 | Gap (late insert) | an ack long after the ID was assigned writes no late-insert row | `features::late_inserts_reach_sync_through_the_change_feed` | failed: "late insert missing from sync" |
| 2026-11-05 | Permission (author) | edit and delete skip the author check | `features::edits_and_deletes_reach_everyone` | failed: another member's edit was accepted, expected `forbidden` |
| 2026-11-05 | Read state regression | `read.mark` drops `last_read_id < $3`, so marks can move backwards | `features::read_marks_unread_counts_and_receipts` | failed: a backwards mark produced a `read.updated` |
| 2026-11-05 | Wrong counts | a reaction's count includes removed (`active = false`) rows | `features::reactions_count_once_per_user` | failed: timed out waiting for the count to drop |
| 2026-11-05 | Read receipts leak | group-room reads are published to the whole room | `features::read_marks_unread_counts_and_receipts` | failed: another member received `read.updated` |
| 2026-11-05 | Search leak | the search query's membership join no longer filters on the caller | `features::search_respects_membership_edits_and_deletes` | failed: bob's results included a message from a private room he isn't in |
| 2026-11-05 | Deleted content shown | search results aren't checked against Scylla (no hydration) | `features::search_respects_membership_edits_and_deletes` | failed: a just-deleted message came back |
| 2026-11-05 | Unindexed messages | the send script doesn't add the stream entry | `features::search_respects_membership_edits_and_deletes` | failed: "never happened: both messages indexed" |
| 2026-11-05 | Stale index | edits don't add a stream entry | `features::search_respects_membership_edits_and_deletes` | failed: "never happened: the edit indexed" |

The Phase 3 plants were applied and reverted by a script. Its first
version restored each file by moving a backup back, which kept the
backup's older modification time, so cargo didn't rebuild and the next
plant ran on top of the previous one. That showed up as the hydration plant
failing on the membership assertion. The script now rewrites the file, and
every row above comes from a rerun after a clean rebuild, each plant on its
own, failing at the assertion named.
