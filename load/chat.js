// k6 load test for the chat app: the realtime path end to end.
//
// Each virtual user signs in as a guest, joins a public room, opens a
// WebSocket, and sends a message every second for the iteration, timing how
// long each one takes to be acknowledged. This exercises the write path, the
// idempotency claim, the Scylla write and the fan-out under concurrency —
// decision 14 (connection capacity) in docs/design.md.
//
// Run against a deployed stack (never the homelab; see docs/testing.md):
//   k6 run -e BASE_URL=https://chat.example.com -e VUS=200 -e DURATION=2m load/chat.js
//
// BASE_URL   the app origin (also sent as the Origin header; must be allowed)
// VUS        number of virtual users (default 50)
// DURATION   how long to hold them (default 1m)

import ws from "k6/ws";
import http from "k6/http";
import { check, fail } from "k6";
import { Trend, Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const VUS = Number(__ENV.VUS || 50);
const DURATION = __ENV.DURATION || "1m";

const ackLatency = new Trend("chat_ack_latency_ms", true);
const sent = new Counter("chat_messages_sent");
const acked = new Counter("chat_messages_acked");

export const options = {
  scenarios: {
    chat: { executor: "constant-vus", vus: VUS, duration: DURATION },
  },
  thresholds: {
    // Fill in real targets once a baseline is measured in a sandbox.
    chat_ack_latency_ms: ["p(95)>=0"],
  },
};

function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// A state-changing request: the server requires a matching Origin and the
// session cookie.
function headers(cookie) {
  const h = { Origin: BASE_URL };
  if (cookie) h.Cookie = cookie;
  return h;
}

export default function () {
  // 1. Guest session.
  const guest = http.post(`${BASE_URL}/api/auth/guest`, null, { headers: headers() });
  if (!check(guest, { "guest created": (r) => r.status === 200 })) {
    fail(`guest sign-in failed: ${guest.status}`);
  }
  const cookie = guest.cookies["__Host-session"]?.[0]?.value
    ? `__Host-session=${guest.cookies["__Host-session"][0].value}`
    : guest.headers["Set-Cookie"];

  // 2. A public room to join (the demo rooms are always there).
  const dir = http.get(`${BASE_URL}/api/rooms/public`, { headers: headers(cookie) });
  const rooms = dir.json();
  if (!check(rooms, { "a public room exists": (r) => Array.isArray(r) && r.length > 0 })) {
    fail("no public rooms to join");
  }
  const roomId = rooms[0].id;
  http.post(`${BASE_URL}/api/rooms/${roomId}/join`, null, { headers: headers(cookie) });

  // 3. WebSocket: send a message a second, time each ack.
  const wsUrl = `${BASE_URL.replace(/^http/, "ws")}/ws`;
  const pendingAt = {}; // req_id -> send time (ms)
  const params = { headers: { Origin: BASE_URL, Cookie: cookie } };

  ws.connect(wsUrl, { ...params, subprotocols: ["chat.v1"] }, (socket) => {
    let live = false;
    let seq = 0;

    socket.on("message", (data) => {
      const frame = JSON.parse(data);
      if (frame.type === "ping") {
        socket.send(JSON.stringify({ type: "pong" }));
      } else if (frame.type === "hello") {
        live = true;
      } else if (frame.type === "message.ack") {
        const started = pendingAt[frame.req_id];
        if (started !== undefined) {
          ackLatency.add(Date.now() - started);
          acked.add(1);
          delete pendingAt[frame.req_id];
        }
      }
    });

    socket.setInterval(() => {
      if (!live) return;
      seq += 1;
      const reqId = `q${seq}`;
      pendingAt[reqId] = Date.now();
      socket.send(
        JSON.stringify({
          type: "message.send",
          req_id: reqId,
          room_id: roomId,
          idempotency_key: uuidv4(),
          content: `load test message ${seq}`,
        }),
      );
      sent.add(1);
    }, 1000);

    // Close at the end of this VU's iteration window.
    socket.setTimeout(() => socket.close(), 30000);
  });
}
