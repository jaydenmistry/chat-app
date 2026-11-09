import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { must } from "../test/must";
import type { Message } from "../protocol/generated/Message";
import type { ServerFrame } from "../protocol/generated/ServerFrame";
import { type ConnectionState, type PendingSend, RealtimeClient, SUBPROTOCOL } from "./client";

/** A WebSocket the test drives from the server side. */
class FakeSocket {
  static all: FakeSocket[] = [];
  sent: Record<string, unknown>[] = [];
  closed: number | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: (() => void) | null = null;

  constructor(
    readonly url: string,
    readonly protocols: string,
  ) {
    FakeSocket.all.push(this);
  }

  send(data: string) {
    this.sent.push(JSON.parse(data) as Record<string, unknown>);
  }

  close(code = 1000) {
    this.closed = code;
  }

  serverSend(frame: ServerFrame) {
    this.onmessage?.({ data: JSON.stringify(frame) } as MessageEvent);
  }

  serverClose(code: number) {
    this.onclose?.({ code } as CloseEvent);
  }

  sentOfType(type: string) {
    return this.sent.filter((f) => f.type === type);
  }
}

const hello: ServerFrame = { type: "hello", user_id: "u", conn_id: "c", heartbeat_ms: 25000 };

function message(id: string): Message {
  return {
    id,
    room_id: "room",
    author_id: "u",
    content: "hi",
    created_at: "2026-11-05T00:00:00Z",
    edited_at: null,
    deleted: false,
    version: 1,
    reactions: [],
  };
}

let keys = 0;
let network: EventTarget;
function client(handlers: ConstructorParameters<typeof RealtimeClient>[0]["handlers"] = {}) {
  const c = new RealtimeClient({
    url: "ws://test/ws",
    handlers,
    WebSocketImpl: FakeSocket as unknown as typeof WebSocket,
    random: () => 1,
    newKey: () => `key-${++keys}`,
    network: network as unknown as Window,
  });
  c.start();
  return c;
}

function latest(): FakeSocket {
  return must(FakeSocket.all[FakeSocket.all.length - 1]);
}

beforeEach(() => {
  vi.useFakeTimers();
  FakeSocket.all = [];
  keys = 0;
  network = new EventTarget();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("RealtimeClient", () => {
  it("offers the protocol version and answers pings", () => {
    client();
    expect(latest().protocols).toBe(SUBPROTOCOL);
    latest().serverSend(hello);
    latest().serverSend({ type: "ping" });
    expect(latest().sentOfType("pong")).toHaveLength(1);
  });

  it("holds sends until hello, then sends them", () => {
    const acked: string[] = [];
    const c = client({ acked: (key) => acked.push(key) });
    const pending = c.send("room", "hi");
    expect(latest().sentOfType("message.send")).toHaveLength(0);

    latest().serverSend(hello);
    const [frame] = latest().sentOfType("message.send");
    expect(frame).toMatchObject({ room_id: "room", content: "hi", idempotency_key: pending.key });

    latest().serverSend({ type: "message.ack", req_id: must(frame).req_id as string, message: message("1"), duplicate: false });
    expect(acked).toEqual([pending.key]);
  });

  it("retries retryable errors with the same key, and fails the rest", () => {
    const updates: PendingSend[] = [];
    const c = client({ pending: (p) => updates.push(p) });
    latest().serverSend(hello);
    const retried = c.send("room", "a");
    const rejected = c.send("room", "b");
    const [first, second] = latest().sentOfType("message.send");

    latest().serverSend({ type: "error", req_id: must(first).req_id as string, code: "unavailable", message: "try again" });
    latest().serverSend({ type: "error", req_id: must(second).req_id as string, code: "forbidden", message: "not a member" });
    vi.advanceTimersByTime(1000);

    const sends = latest().sentOfType("message.send");
    expect(sends).toHaveLength(3);
    expect(sends[2]).toMatchObject({ idempotency_key: retried.key });
    expect(updates.at(-1)).toMatchObject({ key: rejected.key, status: "failed", error: "not a member" });
  });

  it("resends with the same key when no ack arrives", () => {
    const c = client();
    latest().serverSend(hello);
    const pending = c.send("room", "slow");
    vi.advanceTimersByTime(10_000);
    const sends = latest().sentOfType("message.send");
    expect(sends).toHaveLength(2);
    expect(must(sends[0]).idempotency_key).toBe(pending.key);
    expect(must(sends[1]).idempotency_key).toBe(pending.key);
    expect(must(sends[0]).req_id).not.toBe(must(sends[1]).req_id);
  });

  it("reconnects with backoff and resends unacked messages on the new socket", () => {
    const states: ConnectionState[] = [];
    const hellos: boolean[] = [];
    const c = client({ state: (s) => states.push(s), hello: (h) => hellos.push(h.reconnected) });
    latest().serverSend(hello);
    const pending = c.send("room", "in flight");
    latest().serverClose(1012);
    expect(states).toContain("reconnecting");

    // With random() = 1 the first delay is the full 500 ms.
    vi.advanceTimersByTime(499);
    expect(FakeSocket.all).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(FakeSocket.all).toHaveLength(2);

    latest().serverSend(hello);
    expect(hellos).toEqual([false, true]);
    expect(latest().sentOfType("message.send")[0]).toMatchObject({ idempotency_key: pending.key });
  });

  it("uses the server's spread-out delay after a graceful restart", () => {
    const c = client();
    latest().serverSend(hello);
    c.send("room", "in flight");
    // The server is draining: it sends reconnect, then closes with 1012.
    latest().serverSend({ type: "reconnect", retry_after_ms: 2000 });
    latest().serverClose(1012);

    // Not the 500 ms backoff (random() = 1) — it waits the server's delay.
    vi.advanceTimersByTime(500);
    expect(FakeSocket.all).toHaveLength(1);
    vi.advanceTimersByTime(1500);
    expect(FakeSocket.all).toHaveLength(2);
  });

  it("backs off further after each failed attempt, up to 30 s", () => {
    client();
    latest().serverClose(1006);
    vi.advanceTimersByTime(500);
    latest().serverClose(1006);
    vi.advanceTimersByTime(999);
    expect(FakeSocket.all).toHaveLength(2);
    vi.advanceTimersByTime(1);
    expect(FakeSocket.all).toHaveLength(3);
    for (let i = 0; i < 10; i++) {
      latest().serverClose(1006);
      vi.advanceTimersByTime(30_000);
    }
    expect(FakeSocket.all).toHaveLength(13);
  });

  it("stops for good when the session ends", () => {
    const states: ConnectionState[] = [];
    client({ state: (s) => states.push(s) });
    latest().serverSend(hello);
    latest().serverClose(4001);
    vi.advanceTimersByTime(60_000);
    expect(FakeSocket.all).toHaveLength(1);
    expect(states.at(-1)).toBe("ended");
  });

  it("drops a silent connection and reconnects", () => {
    client();
    latest().serverSend(hello);
    vi.advanceTimersByTime(60_000);
    expect(must(FakeSocket.all[0]).closed).toBe(4000);
    vi.advanceTimersByTime(500);
    expect(FakeSocket.all).toHaveLength(2);
  });

  it("drops the socket when the device goes offline, reconnects at once when back", () => {
    const states: ConnectionState[] = [];
    client({ state: (s) => states.push(s) });
    latest().serverSend(hello);
    network.dispatchEvent(new Event("offline"));
    expect(must(FakeSocket.all[0]).closed).toBe(4000);
    expect(states.at(-1)).toBe("reconnecting");
    // Back online before the backoff ran out: no waiting.
    network.dispatchEvent(new Event("online"));
    expect(FakeSocket.all).toHaveLength(2);
    // The pending backoff timer was cancelled; no third socket appears.
    vi.advanceTimersByTime(60_000);
    expect(FakeSocket.all).toHaveLength(2);
  });

  it("re-sends the presence watch list after reconnecting", () => {
    const c = client();
    latest().serverSend(hello);
    c.watch(["a", "b"]);
    latest().serverClose(1012);
    vi.advanceTimersByTime(500);
    latest().serverSend(hello);
    expect(latest().sentOfType("presence.watch")[0]).toMatchObject({ user_ids: ["a", "b"] });
  });

  it("answers edits, deletes and reactions with their own reply", async () => {
    const c = client();
    expect(await c.editMessage("room", "5", "x")).toMatchObject({ ok: false, code: "offline" });
    latest().serverSend(hello);

    const edited = c.editMessage("room", "5", "fixed");
    const reacted = c.setReaction("room", "5", "👍", true);
    const [edit] = latest().sentOfType("message.edit");
    const [react] = latest().sentOfType("reaction.set");
    expect(edit).toMatchObject({ room_id: "room", message_id: "5", content: "fixed" });
    expect(react).toMatchObject({ emoji: "👍", active: true });
    // Replies can come in any order; each settles its own request.
    latest().serverSend({ type: "error", req_id: must(react).req_id as string, code: "not_found", message: "no such message" });
    latest().serverSend({ type: "ok", req_id: must(edit).req_id as string });
    expect(await edited).toEqual({ ok: true });
    expect(await reacted).toEqual({ ok: false, code: "not_found", message: "no such message" });
  });

  it("settles open requests when the connection drops or the server is silent", async () => {
    const c = client();
    latest().serverSend(hello);
    const dropped = c.deleteMessage("room", "5");
    latest().serverClose(1006);
    expect(await dropped).toMatchObject({ ok: false, code: "offline" });

    vi.advanceTimersByTime(1000);
    latest().serverSend(hello);
    const silent = c.deleteMessage("room", "5");
    vi.advanceTimersByTime(10_000);
    expect(await silent).toMatchObject({ ok: false, code: "timeout" });
  });

  it("sends read marks without waiting for a reply", () => {
    const c = client();
    latest().serverSend(hello);
    c.markRead("room", "9");
    expect(latest().sentOfType("read.mark")).toEqual([{ type: "read.mark", room_id: "room", message_id: "9" }]);
  });
});
