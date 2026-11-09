# Load tests (k6)

`chat.js` drives the realtime write path: each virtual user signs in as a
guest, joins a public room, opens a WebSocket and sends a message a second,
timing each acknowledgement. It reports `chat_ack_latency_ms` (p95 etc.),
`chat_messages_sent` and `chat_messages_acked`.

## Running

Run it against a **deployed stack**, from a machine that is not the server —
never the homelab, which can't run k6 and whose Docker daemon is the owner's
live services (see `../CLAUDE.md`). The design defers the load test and its
measured numbers to a real sandbox.

```bash
# Ramp as needed; start small and watch the server's /metrics and logs.
k6 run -e BASE_URL=https://chat.example.com -e VUS=200 -e DURATION=2m chat.js
```

`BASE_URL` is also sent as the `Origin` header, so it must be an allowed origin
(`ALLOWED_ORIGINS`, which defaults to `PUBLIC_URL`). The script joins the first
public room it finds — the seeded demo rooms are always present.

## Results

Not yet measured. This repository records only measured numbers
(`../CLAUDE.md`), so fill in a baseline — p95 ack latency, messages/second and
the VU count where it degrades — here and in `../docs/design.md` (decision 14,
connection capacity) only after a sandbox run, and set real
`thresholds` in `chat.js` from it.
