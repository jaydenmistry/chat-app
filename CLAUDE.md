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

## Environment quirks (cloud sandbox)

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

## Status

- Phase 0: `docs/design.md` drafted, awaiting review.
