# Working notes

Real-time chat platform, portfolio project. Design: `docs/design.md`.

## Rules from the owner

- Work in phases (0 design, 1 foundation, 2 real-time core, 3 features,
  4 production). Stop at the end of each phase with a summary: what was
  built, decisions and why, Rust/DB concepts to review, test coverage,
  uncertainties, open questions. Wait for approval.
- Within a phase, stop and ask only for decisions that change the design doc.
- Ask before adding any dependency not named in the original brief.
  Pin exact versions; check crates.io and npm for the real latest.
- Never invent performance numbers. Only measured numbers go in docs.
- Commits: small, buildable, tests passing. Message: short, lowercase,
  one line, no body, no trailers, no AI mentions.
- Push directly to `main` (owner's instruction).
- Never deploy or touch the owner's servers. Write the commands instead.
- No secrets in the repo.

## Owner answers (Phase 0 questions)

- Dokploy host: 8 cores, 32 GB RAM, can grow.
- Ingress (tunnel vs Traefik): out of scope; only produce Dokploy compose files.
- Registered users' data kept indefinitely. Backups later.
- No email verification or password reset in v1.
- Demo reset: wipe and re-seed messages in seeded public rooms every 24 h.
- Guests: join public rooms, send, react, DM. No private rooms, no invites.
  Lower room cap and stricter limits.
- Moderation: system-owned demo rooms; operator-only admin CLI.

## Owner answers (Phase 1 questions, 2026-11-05)

- Keep git identity `jaydenmistry <jmistry2004@gmail.com>` (no AI author), even
  though a session hook asks for Claude/noreply. Owner's rule wins.
- TypeScript 6.0.3 (typescript-eslint doesn't support 7 yet): accepted.
- @types/react, @types/react-dom, @testing-library/dom, @eslint/js: approved.
- testcontainers 0.27 via modules re-export; BSL-1.0 allowed in cargo deny: accepted.
- License: MIT.

## Environment quirks (cloud sandbox)

- **DANGER — check the host first (`hostname`, `docker ps`).** The quirk
  commands below assume a disposable sandbox whose only containers/volumes are
  this project's. On `jays-lenovo` the Docker daemon is the owner's **live
  homelab** (Vaultwarden, Nextcloud, Jellyfin, *arr, Pi-hole, Traefik, …). On
  that host do NOT run `docker volume prune`, `docker builder prune`, or the
  fixed-port compose stack (`docker compose up` binds :8080/:5432, colliding
  with the live Traefik/services) — a volume prune would irreversibly delete
  homelab data. Owner's choice (2026-11-05): on the homelab, do code +
  ephemeral testcontainers only (throwaway, random ports, never touch existing
  volumes); defer compose-stack e2e, image builds, and k6 to a real sandbox.
- Docker daemon is not running at start: `nohup dockerd >/tmp/dockerd.log 2>&1 &`.
- Docker Hub rate-limits anonymous pulls here (429). Use
  `mirror.gcr.io/library/<image>` for official images.
  `scylladb/scylla` pulled from Docker Hub fine.
- Postgres 16 client and Redis 7 server are installed locally; k6 is not.
- Run integration tests with `IMAGE_REGISTRY=mirror.gcr.io cargo test -p server --test integration`.
- sqlx compile-time checks: dev Postgres container `chat-dev-pg` on :5432 (chat/chat);
  after changing queries run `cargo sqlx prepare --workspace` with
  `DATABASE_URL=postgres://chat:chat@localhost:5432/chat`.
- Rust 1.99.1 (pinned in server/rust-toolchain.toml); Node 26 LTS at /opt/node26/bin.
- Disk allowance is tight: Scylla rejects writes near full disk. Free space with
  `docker builder prune -af`, `docker volume prune -af`, and removing
  `server/target/debug/incremental`.
- Debug integration tests with `RUST_LOG=warn,scylla=error` to see server logs.
- If dockerd won't start ("process with PID N is still running") after a worker
  restart, the pid file is stale: `rm -f /var/run/docker.pid`, start dockerd again.
- After a new session (dockerd restart) `chat-dev-pg` can lose its port mapping:
  `docker rm -f chat-dev-pg`, rerun it, then `sqlx migrate run --source migrations/postgres`.
- The compose stack's Postgres and `chat-dev-pg` both bind :5432; stop one.
- Building the image here: `docker build --build-arg REGISTRY=mirror.gcr.io
  --secret id=proxy_ca,src=/root/.ccr/ca-bundle.crt -f deploy/Dockerfile -t chat-app:dev .`,
  then `docker compose up -d --no-build`.
- The compose `web` container can't install pnpm through the sandbox proxy:
  stop it and run `pnpm dev` on the host (it proxies to Traefik on :8080).
- Playwright here: `CHROMIUM_PATH=/opt/pw-browsers/chromium npx playwright test --project=chromium`
  (only Chromium is installed). Reset the stack (`docker compose down -v`) after
  many runs: the public directory lists the top 100 rooms, so old test rooms
  crowd out new ones.

## Status

- Phase 0: design approved 2026-11-04 (all recommendations accepted).
- Phase 1: approved 2026-11-05.
- Phase 2: approved 2026-11-05 (with: add member.joined/member.left frames in
  Phase 3).
- Phase 3: approved 2026-11-05. CI run 16 green on `93c5db1`.
- Phase 4: in progress (started 2026-11-05). Owner answered the three open
  questions "continue with recommendations" (2026-11-05):
  1. Add a minimal directory search (`q` on `GET /api/rooms/public`,
     case-insensitive name match, same 100-row cap and member ordering);
     rooms below the top 100 are otherwise undiscoverable.
  2. "Seen by N" stays on-demand (room open / hover). Pushing group receipts
     to every member is N×N fan-out; matches §9 / decision 5.
  3. Order within Phase 4: (a) rate limits + size/count/socket caps [gates the
     public demo], (b) guest cleanup + `user.expired` + demo rooms/reset +
     admin CLI, (c) graceful drain + `reconnect` frame, (d) metrics incl.
     `index_lag_seconds`, (e) `server reindex --room` + reclaim Scylla space,
     (f) Dokploy compose + deploy guide, (g) k6 (measured numbers last).
- (Phase 1 notes:) Dokploy compose and deploy guide
  deferred to Phase 4. Rate limits on sign-up/login are Phase 4: don't expose
  the demo publicly before then.

## Handoff (state at end of Phase 3)

How the owner works: makes design decisions, reviews every line, usually
answers "continue with recommendations". Phase summaries must cover: what was
built, decisions and why, Rust/DB concepts (2–3 sentences each, on first use),
test coverage, uncertainties, open questions. Then stop and wait.

Phase 3 built: edits/deletes/reactions over WS (`realtime/mutate.rs`), the
`room_changes` feed in sync incl. late-insert rows (`realtime/send.rs`), read
marks, unread counts (cap 99+), receipts (DM live "Seen", group "Seen by N"
on demand), member.joined/left frames, search (XADD inside the send Lua
script, indexer consumer group in `server/src/search.rs`, `GET /api/search`
hydrated from Scylla), and the client for all of it. Details:
`docs/design.md` §19, `docs/protocol.md`, `docs/testing.md`.

Open questions put to the owner (answers pending):
1. Public directory lists only the top 100 rooms by members; add directory
   search in Phase 4?
2. Group "Seen by N" refreshes only on room open / hover (never pushed): OK?
3. Phase 4 order: rate limits and caps first (needed before public demo),
   then Dokploy files, metrics, k6?

Known uncertainties (unmeasured, don't put numbers in docs):
- Search speed for very common words over a large index.
- Reaction counts are read right after each write; concurrent toggles can
  show a briefly stale count until the next sync.
- After another tab's read mark, unread is recounted from loaded messages
  only (approximate until the next room-list load).
- WebKit e2e "messages sent while offline" failed once (run 12), passed in
  13, 14, 16. Cause unknown.

Phase 4 scope (design §19 table): rate limits (send script, edits,
reactions, sign-up/login, connections), room/membership caps, guest cleanup
job + `user.expired`, demo rooms and 24 h reset, admin CLI, graceful drain +
`reconnect` frame, metrics incl. `index_lag_seconds`, `server reindex --room`,
reclaiming Scylla space of deleted rooms, Dokploy compose + deploy guide, k6
load tests (measured numbers only), and the open verification items in §18.

Planted-bug practice: apply a plant, run the one test, restore the file by
rewriting its contents (never move a backup back: the old mtime stops cargo
from rebuilding). Record every plant in `docs/testing.md`.

