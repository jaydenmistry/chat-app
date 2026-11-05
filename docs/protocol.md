# Protocol

The client talks to the server two ways:

- **WebSocket** (`/ws`): everything real-time, plus sending messages, which
  need an acknowledgement.
- **REST** (`/api/...`, JSON): accounts, rooms, history and sync.

Every frame and body is defined once in Rust (`server/crates/protocol`) and
generated as TypeScript (`web/src/protocol/generated`). This document
describes behaviour; the types are the source of truth for shapes.

Status: this is protocol `chat.v1` as of Phase 2. Phase 3 adds edits,
deletes, reactions, read receipts and search; their frames are marked
"Phase 3" below and are not served yet.

## Conventions

- **Message IDs are strings**, like `"111659093842698240"`. They are 64-bit
  integers, and JavaScript numbers lose precision above 2^53. They sort by
  creation time: compare by length first, then as text (no leading zeros).
- Room and user IDs are UUIDs. Timestamps are RFC 3339 strings.
- `version` is the write timestamp of a message's latest change, in
  microseconds. Of two copies of one message, keep the higher `version`.

## Connecting

```
GET /ws
Cookie: __Host-session=<token>        (session=<token> on http://localhost)
Origin: https://chat.jmistry.com
Sec-WebSocket-Protocol: chat.v1
```

The upgrade is refused with an HTTP status (the browser sees a failed
connection):

| Status | When |
|---|---|
| 401 | no session cookie, or the session has ended |
| 403 | the `Origin` is missing or not one of ours |
| 400 | the client didn't offer `chat.v1` |

After the upgrade, the first frame is always `hello`. The server sends it
only once the connection's subscriptions are live: every message sent after
`hello` arrives on this socket. Anything earlier comes from history or sync.

```json
{"type": "hello", "user_id": "01a1…", "conn_id": "01a1…", "heartbeat_ms": 25000}
```

If the server can't subscribe (Redis is down), it closes right away with
`1013` (try again later) instead of sending `hello`.

## Heartbeat

The server sends `{"type": "ping"}` every 25 seconds. The client answers
`{"type": "pong"}`. Either side closes the connection after 60 seconds
without hearing any frame. These are JSON frames, not WebSocket pings:
browsers answer WebSocket pings themselves without telling the page, so the
page couldn't otherwise notice a dead connection. The 25-second interval also
stays well inside Cloudflare's 100-second idle timeout.

## Client frames

| `type` | Fields | Notes |
|---|---|---|
| `message.send` | `req_id`, `room_id`, `idempotency_key` (UUID), `content` | Retry with the same key until acked (see "Sending") |
| `typing` | `room_id` | At most one every 3 s while typing; the server drops extras (one per 2 s per room) |
| `presence.set` | `state`: `active` or `idle` | `idle` after 5 minutes without input or while the tab is hidden |
| `presence.watch` | `user_ids` (up to 200) | Replaces the set of users whose `presence` this socket receives |
| `pong` | | The answer to `ping` |
| `message.edit` | `req_id`, `room_id`, `message_id`, `content` | Phase 3 |
| `message.delete` | `req_id`, `room_id`, `message_id` | Phase 3 |
| `reaction.set` | `req_id`, `room_id`, `message_id`, `emoji`, `active` | Phase 3 |
| `read.mark` | `room_id`, `message_id` | Phase 3 |

`req_id` is chosen by the client (at most 64 characters) and echoed in the
reply.

## Server frames

| `type` | Fields | Meaning |
|---|---|---|
| `hello` | `user_id`, `conn_id`, `heartbeat_ms` | Connected, and live delivery is on |
| `ping` | | Answer with `pong` |
| `message.ack` | `req_id`, `message`, `duplicate` | The message is stored. `duplicate: true` when this was a retry of a send already stored; `message` is the original |
| `error` | `req_id` (or null), `code`, `message` | See "Errors" |
| `message.created` | `message` | A new message in one of your rooms, including your own (the fan-out copy) |
| `typing` | `room_id`, `user_id` | Show "is typing" for about 5 s. Your own typing comes back too; ignore it |
| `presence` | `user_id`, `status`: `online`, `away` or `offline` | For watched users: once when watching starts, then on every change |
| `room.joined` | `room` | You're in a new room (joined on another tab, accepted an invite, someone opened a DM with you). Live delivery for it is already on |
| `room.left` | `room_id`, `reason`: `left`, `removed` or `deleted` | You're no longer in the room. Nothing more arrives for it |
| `resync` | `reason` | This connection may have missed live messages. Run the sync procedure |

A `message`:

```json
{
  "id": "4194316288",
  "room_id": "00000000-0000-0000-0000-000000000001",
  "author_id": "00000000-0000-0000-0000-000000000009",
  "content": "hello",
  "created_at": "2026-01-01T00:00:01Z",
  "edited_at": null,
  "deleted": false,
  "version": 1767225601000000
}
```

## Errors

| `code` | Retry? | Meaning |
|---|---|---|
| `bad_frame` | no | Not JSON, unknown `type`, or a missing field. `req_id` is null if the frame couldn't be read |
| `invalid_content` | no | Empty, over 4096 bytes (UTF-8, after trimming), or contains control characters other than newline and tab |
| `forbidden` | no | Not a member of the room, or your account (guest) or the room has expired |
| `idempotency_conflict` | no | The key was already used for a different room or content. Generate a new key for new content |
| `unavailable` | yes | A dependency is down. Retry the same frame with the same key |
| `internal` | yes | A bug on our side. Retry |

Close codes:

| Code | Meaning | Client should |
|---|---|---|
| `4001` | Session ended (logout, revoked, guest expired) | Go to sign-in; don't reconnect |
| `4008` | Too slow: the client didn't read fast enough | Reconnect and sync |
| `4400` | Protocol violation (no heartbeat, binary frame) | Reconnect |
| `1012` | Server restarting | Reconnect, to another instance |
| `1011`, `1013` | Couldn't set up the connection | Reconnect with backoff |

Reconnects use exponential backoff with full jitter: a random delay between
0 and `min(30 s, 0.5 s × 2^attempt)`. The browser's `online` event resets the
backoff and reconnects immediately.

## Sending

Delivery from client to server is at least once:

1. When the user sends, generate an idempotency key (a UUID) for that
   message, once.
2. Send `message.send` with it. Show the message as pending.
3. On `message.ack`, the message is stored. Replace the pending entry with
   `message`.
4. On a retryable `error`, if no ack arrives within 10 s, or after a
   reconnect: send again, **with the same key**, until acked.
5. On a non-retryable `error`, show it as failed. The user can retry (same
   key) or discard it.

The server stores the message once however many times it arrives, even when
two attempts run concurrently on two instances. A retry of a stored message
gets the original back with `duplicate: true`. Keys are remembered for 24
hours.

The ack arrives before your own `message.created` copy. Both carry the same
ID; keep one.

## The sync procedure

Run it after every `hello` (first connection and every reconnect) and after
every `resync` frame:

1. `GET /api/rooms` to refresh the room list.
2. For every room with loaded messages, take the highest message ID you hold
   and `POST /api/sync`:

   ```json
   {"rooms": [{"room_id": "…", "after": "111659093842698240"}]}
   ```

3. For each room in the response:
   - `member: false`: you left or were removed; drop the room.
   - `complete: false`: more than 500 messages were missed; discard the
     room's messages and load its latest page instead.
   - Otherwise merge `messages` (oldest first) into the room.

The response deliberately starts **5 seconds before** `after`, so it repeats
messages you already have. A message can be stored slightly after one with a
higher ID (instances' clocks differ by up to 1 s, and a write may take up to
2 s), and without the overlap a resume could skip it. Merge by ID: a message
you already have is not new. A late message with a smaller ID goes in its
place by ID, not at the bottom.

Merge rules, everywhere messages arrive (history, sync, live):

- Deduplicate by `id`.
- Of two copies, keep the higher `version`.
- A deleted copy always wins: never show content again once deleted.

## History

`GET /api/rooms/<room_id>/messages?before=<message_id>&limit=50` returns up
to `limit` (at most 100) messages older than `before`, newest first, plus
`next_before`, the cursor for the next older page. Omit `before` for the
latest page. `next_before: null` means the start of the room.

A page can be shorter than `limit` and still have a cursor: the server reads
at most 32 hourly partitions per request, so a long run of empty hours (all
expired) is crossed over several requests. Keep asking while there's a
cursor.

## REST endpoints

All bodies are JSON. State-changing requests (POST, DELETE) must carry an
allowed `Origin`, which browsers send automatically. Errors are
`{"code", "message", "field"?}` with stable codes (`bad_request`,
`invalid_input`, `unauthorized`, `forbidden`, `not_found`, `conflict`,
`unavailable`, `internal`).

| Method and path | Purpose |
|---|---|
| `GET /api/auth/config` | Whether GitHub sign-in is available |
| `POST /api/auth/signup` | Create a password account; sets the cookie |
| `POST /api/auth/login` | Sign in; sets the cookie |
| `POST /api/auth/guest` | Start a 24-hour guest account |
| `POST /api/auth/logout` | End this session (and close its sockets everywhere) |
| `GET /api/auth/github/start`, `GET /api/auth/github/callback` | GitHub sign-in (browser redirects) |
| `GET /api/me` | The signed-in user |
| `GET /api/sessions`, `DELETE /api/sessions/<id>` | List and revoke your sessions |
| `GET /api/rooms` | Your rooms |
| `GET /api/rooms/public` | The public room directory |
| `POST /api/rooms` | Create a room (`{"name", "kind": "public" or "private"}`) |
| `POST /api/rooms/<id>/join` | Join a public room |
| `POST /api/rooms/<id>/leave` | Leave a room (not as owner, not a DM) |
| `DELETE /api/rooms/<id>` | Delete a room (owner) |
| `GET /api/rooms/<id>/members` | Members |
| `DELETE /api/rooms/<id>/members/<user_id>` | Remove a member (owner) |
| `POST /api/rooms/<id>/invites` | Create an invite link for a private room (owner) |
| `POST /api/invites/<code>/accept` | Join with an invite |
| `POST /api/dms` | Open the DM with `{"handle"}` (or get the existing one) |
| `GET /api/users?ids=a,b` | Profiles, for message authors |
| `GET /api/rooms/<id>/messages` | History (above) |
| `POST /api/sync` | Sync (above) |

A room you can't see answers `404`, the same as one that doesn't exist, so
private rooms can't be probed.

## Sequences

### Connect

```
client                                  server
  | GET /ws (cookie, Origin, chat.v1)     |
  |-------------------------------------->|  check Origin, session, version
  |                                       |  route rooms, subscribe, wait for Redis
  |<--------------------------- hello ----|
  | POST /api/sync (loaded rooms)         |
  |-------------------------------------->|
  |<----------------- missed messages ----|
  |<------------------------- ping -------|  every 25 s
  |-------- pong ------------------------>|
```

### Send, with a retry after a lost ack

```
client                                  server
  | message.send {req_id: q1, key: K}     |
  |-------------------------------------->|  claim K -> id 42, store id 42
  |        X <------ message.ack q1 ------|  (lost: connection dropped)
  |  ... reconnect, hello, sync ...       |
  | message.send {req_id: q2, key: K}     |
  |-------------------------------------->|  K already claimed -> id 42
  |                                       |  store id 42 again (no change)
  |<-- message.ack q2 {duplicate: true} --|
  |<-- message.created {id 42} -----------|  (copies are dropped by ID)
```

### Reconnect and resume

```
client                     instance A              instance B
  | (has up to id 100)         |                       |
  |       X  A shuts down: close 1012                  |
  |  wait 0..0.5 s (jitter)                            |
  | GET /ws ------------------------------------------>|
  |<---------------------------------------------- hello
  | POST /api/sync {after: 100} ---------------------->|
  |<--- messages from 5 s before id 100 onwards -------|  merge by ID
  | resend any unacked sends, same keys -------------->|
```

### Redis outage on the client's instance

```
client                         instance                  Redis
  |                              | PING (every 5 s) ------>|  (no answer)
  |                              | reconnect, resubscribe ->|  (once back)
  |                              | rebuild routing from Postgres
  |<------------------- resync --|
  | POST /api/sync ------------->|
```

Sends during the outage get `unavailable` (no deduplication without Redis)
and are retried until acked.

### Typing

```
ada's client               server                bob's client
  | typing {room} ---------->|  member? yes          |
  |                          |-- typing {room, ada} ->|  show for 5 s
  | (types on: one more      |                       |
  |  every 3 s at most)      |                       |
  | message.send ----------->|-- message.created --->|  hide ada's indicator
```

### Removed from a room while connected on two instances

```
owner             instance A          instance B           bob (on A and B)
  | DELETE member ->|  commit             |                      |
  |                 |-- member_left on room channel (Redis) ---->|
  |                 |  unroute bob        |  unroute bob         |
  |                 |------------------------- room.left -------->| (both sockets)
  |                 |  later messages in the room no longer reach bob
```

### Edit and delete

Phase 3. Edits and deletes will be `message.edit` and `message.delete`,
answered with `ok` or `error`, fanned out as `message.updated` and
`message.deleted` with a higher `version`, and recorded in a change feed
that the sync procedure reads, so a reconnecting client also catches up on
changes to messages it already has.
