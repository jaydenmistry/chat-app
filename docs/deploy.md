# Deploying to Dokploy

The whole app is one image (`deploy/Dockerfile`): the Rust server plus the
built web client, which the server serves from `STATIC_DIR`. Dokploy builds it,
runs the stack in `deploy/dokploy.compose.yaml`, and puts its own Traefik in
front. TLS and DNS are Dokploy's job; this guide covers the stack and its
configuration.

## Prerequisites

- A Dokploy host (the project's is 8 cores, 32 GB RAM).
- A domain pointed at the host (optionally through Cloudflare).
- For GitHub sign-in: an OAuth app whose callback is
  `https://<your-domain>/api/auth/github/callback`.

## Create the app

1. In Dokploy, create a **Compose** application from this repository with the
   compose path `deploy/dokploy.compose.yaml`.
2. Set the environment variables below.
3. Add a **domain** pointing at the `server` service on port **8080**. Leave
   sticky sessions **off** — any instance can serve any request, and the
   design depends on that (docs/design.md 2).
4. Deploy. The `migrate` service applies the Postgres and Scylla migrations and
   exits; the two `server` replicas start once it succeeds.

## Environment variables

Set the secrets in Dokploy; never commit them.

| Variable | Required | Notes |
|---|---|---|
| `PUBLIC_URL` | yes | The origin browsers load, e.g. `https://chat.example.com`. Used for the GitHub redirect and as the default allowed origin. |
| `POSTGRES_PASSWORD` | yes | Postgres superuser password. |
| `ALLOWED_ORIGINS` | no | Defaults to `PUBLIC_URL`; a comma-separated list if the app is reached from more than one origin. |
| `CLIENT_IP_HEADER` | no | Defaults to `CF-Connecting-IP`. The header the real client IP arrives in (see below). |
| `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET` | no | Enable GitHub sign-in. Set both or neither. |
| `SCYLLA_SMP` / `SCYLLA_MEMORY` | no | Shards and memory for Scylla (default `2` / `4G`). Raise to fit the host. |
| `SCYLLA_REPLICATION_FACTOR` | no | `1` for a single node. |
| `RUST_LOG` | no | Log filter; default `info`. |

## The client-IP header

The per-IP rate limits (sign-up, guest, login) read the client IP from
`CLIENT_IP_HEADER`. That is only safe because port 8080 is **not reachable
except through the proxy** — nothing in the compose publishes a host port, so
the only way in is Dokploy's Traefik, which sets the header. Do not publish
8080 to the host or the internet, or a client could spoof its IP and bypass the
limits (docs/design.md 11.1).

## Keep `/metrics` internal

`GET /metrics` (Prometheus) is served on the app port, so it is reachable at
`https://<your-domain>/metrics` unless the proxy blocks it. Add a Traefik rule
(or Cloudflare firewall rule) that refuses `/metrics` from the public internet
and scrape it over the internal network instead (docs/design.md 13).

## Deploys and restarts

A redeploy recreates the `server` replicas. On SIGTERM each instance drains:
`/readyz` starts failing (Traefik stops sending it traffic), every socket gets
a `reconnect` frame with a short randomised delay, and then the socket closes.
Clients reconnect to the other instance and resync, so a deploy is a brief
blip, not lost messages (docs/design.md 13). Keep at least two replicas so one
is always up during a rolling redeploy.

## Operator commands

Run these in a `server` container with `docker exec` (there is no UI or
endpoint for them):

```bash
# Suspend an account: ends its sessions and refuses future logins.
docker exec <container> /app/server admin ban <handle>

# Delete a single message (fans out like the author's own delete).
docker exec <container> /app/server admin delete-message <room_id> <message_id>

# Rebuild a room's search index (e.g. after a Redis flush).
docker exec <container> /app/server reindex --room <room_id>

# Free the Scylla space of a room already deleted from Postgres.
docker exec <container> /app/server reclaim --room <room_id>
```

`admin delete-message`, `reindex` and `reclaim` connect to the stores
themselves; `delete-message` briefly leases a worker ID, so it takes a few
seconds to start.

## Data and backups

Postgres, Redis and Scylla each keep a named volume. Registered users' data is
kept indefinitely; guests and the demo rooms expire on their own (docs/design.md
11). Backups are out of scope for v1 (Phase 0) — snapshot the volumes at the
host or storage layer if you need them.
