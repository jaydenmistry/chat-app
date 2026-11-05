// The WebSocket client: connection, heartbeat, reconnect, and reliable sends.
//
// Sends are at-least-once: each message gets an idempotency key when it is
// written, and is sent again with the same key until the server acks it,
// after a retryable error, an ack timeout, or a reconnect. The server stores
// it once whatever happens (docs/protocol.md, "Sending").
//
// After every `hello` the app syncs the rooms it has loaded: anything sent
// while this client wasn't subscribed comes from history, never only from
// live delivery.

import type { ClientFrame } from "../protocol/generated/ClientFrame";
import type { FrameErrorCode } from "../protocol/generated/FrameErrorCode";
import type { Message } from "../protocol/generated/Message";
import type { PresenceState } from "../protocol/generated/PresenceState";
import type { ServerFrame } from "../protocol/generated/ServerFrame";

export const SUBPROTOCOL = "chat.v1";
/** Close codes from protocol::frames::close. */
export const CLOSE_SESSION_ENDED = 4001;

export type ConnectionState = "connecting" | "live" | "reconnecting" | "ended";

export interface PendingSend {
  key: string;
  roomId: string;
  content: string;
  status: "sending" | "failed";
  error?: string;
}

/**
 * The answer to an edit, delete or reaction. These are idempotent, so after
 * `offline` or `timeout` (applied or not, nobody knows) trying again is safe.
 */
export type CallResult =
  | { ok: true }
  | { ok: false; code: FrameErrorCode | "offline" | "timeout"; message: string };

export interface RealtimeHandlers {
  state?: (state: ConnectionState) => void;
  /** `reconnected` is false only for the very first connection. */
  hello?: (info: { reconnected: boolean }) => void;
  /** Every server frame the client doesn't handle itself. */
  frame?: (frame: ServerFrame) => void;
  acked?: (key: string, message: Message) => void;
  pending?: (pending: PendingSend) => void;
}

export interface RealtimeOptions {
  url: string;
  handlers: RealtimeHandlers;
  WebSocketImpl?: typeof WebSocket;
  /** For jitter; injectable so tests are deterministic. */
  random?: () => number;
  /** Resend if no ack arrives within this long. */
  ackTimeoutMs?: number;
  /** Close and reconnect after this long without any frame. */
  idleTimeoutMs?: number;
  newKey?: () => string;
  /** Where to listen for the browser's `online`/`offline` events. */
  network?: Pick<Window, "addEventListener" | "removeEventListener"> | null;
}

const MIN_BACKOFF_MS = 500;
const MAX_BACKOFF_MS = 30_000;
const RETRY_AFTER_ERROR_MS = 1_000;

type Timer = ReturnType<typeof setTimeout>;

export class RealtimeClient {
  private readonly options: Required<Omit<RealtimeOptions, "handlers">> & { handlers: RealtimeHandlers };
  private socket: WebSocket | null = null;
  private state: ConnectionState = "connecting";
  private live = false;
  private everConnected = false;
  private attempt = 0;
  private stopped = false;
  private reconnectTimer: Timer | null = null;
  private idleTimer: Timer | null = null;
  private nextReq = 0;
  private readonly pending = new Map<string, PendingSend>();
  private readonly ackTimers = new Map<string, Timer>();
  /** req_id -> idempotency key, for matching replies. */
  private readonly requests = new Map<string, string>();
  /** req_id -> the caller waiting for `ok` or an error. */
  private readonly calls = new Map<string, { resolve: (result: CallResult) => void; timer: Timer }>();
  private watched: string[] = [];
  private presence: PresenceState = "active";

  constructor(options: RealtimeOptions) {
    this.options = {
      WebSocketImpl: WebSocket,
      random: Math.random,
      ackTimeoutMs: 10_000,
      idleTimeoutMs: 60_000,
      newKey: () => crypto.randomUUID(),
      network: typeof window === "undefined" ? null : window,
      ...options,
    };
  }

  start(): void {
    this.stopped = false;
    this.options.network?.addEventListener("offline", this.onOffline);
    this.options.network?.addEventListener("online", this.onOnline);
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    this.options.network?.removeEventListener("offline", this.onOffline);
    this.options.network?.removeEventListener("online", this.onOnline);
    this.clearTimers();
    this.socket?.close(1000, "client stopped");
    this.socket = null;
  }

  /** The device lost its network: the socket is as good as dead. */
  private readonly onOffline = () => {
    if (this.stopped || !this.socket) return;
    const socket = this.socket;
    this.onDisconnect(4000);
    socket.close(4000, "offline");
  };

  /** Back online: reconnect now instead of waiting out the backoff. */
  private readonly onOnline = () => {
    if (this.stopped || this.socket || this.state === "ended") return;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.attempt = 0;
    this.connect();
  };

  connectionState(): ConnectionState {
    return this.state;
  }

  /** Queues a message. It is sent now if connected, otherwise on `hello`. */
  send(roomId: string, content: string): PendingSend {
    const pending: PendingSend = { key: this.options.newKey(), roomId, content, status: "sending" };
    this.pending.set(pending.key, pending);
    this.options.handlers.pending?.(pending);
    this.transmit(pending.key);
    return pending;
  }

  /** Tries a failed send again, with the same key. */
  retry(key: string): void {
    const pending = this.pending.get(key);
    if (!pending) return;
    this.update(key, { status: "sending", error: undefined });
    this.transmit(key);
  }

  /** Gives up on a failed send. */
  discard(key: string): void {
    this.pending.delete(key);
    this.clearAckTimer(key);
  }

  editMessage(roomId: string, messageId: string, content: string): Promise<CallResult> {
    return this.call((req_id) => ({ type: "message.edit", req_id, room_id: roomId, message_id: messageId, content }));
  }

  deleteMessage(roomId: string, messageId: string): Promise<CallResult> {
    return this.call((req_id) => ({ type: "message.delete", req_id, room_id: roomId, message_id: messageId }));
  }

  setReaction(roomId: string, messageId: string, emoji: string, active: boolean): Promise<CallResult> {
    return this.call((req_id) => ({
      type: "reaction.set",
      req_id,
      room_id: roomId,
      message_id: messageId,
      emoji,
      active,
    }));
  }

  /** No reply; a lost mark is covered by the next one. */
  markRead(roomId: string, messageId: string): void {
    this.sendFrame({ type: "read.mark", room_id: roomId, message_id: messageId });
  }

  private call(build: (reqId: string) => ClientFrame): Promise<CallResult> {
    if (!this.live) {
      return Promise.resolve({ ok: false, code: "offline", message: "not connected" });
    }
    this.nextReq += 1;
    const reqId = `c${this.nextReq}`;
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.calls.delete(reqId);
        resolve({ ok: false, code: "timeout", message: "no answer from the server" });
      }, this.options.ackTimeoutMs);
      this.calls.set(reqId, { resolve, timer });
      this.sendFrame(build(reqId));
    });
  }

  private settle(reqId: string, result: CallResult): boolean {
    const call = this.calls.get(reqId);
    if (!call) return false;
    clearTimeout(call.timer);
    this.calls.delete(reqId);
    call.resolve(result);
    return true;
  }

  typing(roomId: string): void {
    this.sendFrame({ type: "typing", room_id: roomId });
  }

  setPresence(state: PresenceState): void {
    this.presence = state;
    this.sendFrame({ type: "presence.set", state });
  }

  watch(userIds: string[]): void {
    this.watched = userIds;
    this.sendFrame({ type: "presence.watch", user_ids: userIds });
  }

  private connect(): void {
    this.setState(this.everConnected ? "reconnecting" : "connecting");
    const socket = new this.options.WebSocketImpl(this.options.url, SUBPROTOCOL);
    this.socket = socket;
    socket.onmessage = (event: MessageEvent) => {
      if (socket !== this.socket) return;
      this.resetIdle();
      let frame: ServerFrame;
      try {
        frame = JSON.parse(String(event.data)) as ServerFrame;
      } catch {
        return;
      }
      this.handle(frame);
    };
    socket.onclose = (event: CloseEvent) => {
      if (socket !== this.socket) return;
      this.onDisconnect(event.code);
    };
    // onerror is always followed by onclose; nothing to do there.
    socket.onerror = () => {};
    this.resetIdle();
  }

  private handle(frame: ServerFrame): void {
    switch (frame.type) {
      case "ping":
        this.sendFrame({ type: "pong" });
        return;
      case "hello": {
        const reconnected = this.everConnected;
        this.live = true;
        this.everConnected = true;
        this.attempt = 0;
        this.setState("live");
        if (this.watched.length > 0) this.sendFrame({ type: "presence.watch", user_ids: this.watched });
        if (this.presence !== "active") this.sendFrame({ type: "presence.set", state: this.presence });
        // Everything not yet acked goes again, with the same keys.
        for (const key of this.pending.keys()) {
          if (this.pending.get(key)?.status === "sending") this.transmit(key);
        }
        this.options.handlers.hello?.({ reconnected });
        return;
      }
      case "message.ack": {
        const key = this.requests.get(frame.req_id);
        this.requests.delete(frame.req_id);
        if (key && this.pending.delete(key)) {
          this.clearAckTimer(key);
          this.options.handlers.acked?.(key, frame.message);
        }
        return;
      }
      case "ok":
        this.settle(frame.req_id, { ok: true });
        return;
      case "error": {
        if (frame.req_id && this.settle(frame.req_id, { ok: false, code: frame.code, message: frame.message })) {
          return;
        }
        const key = frame.req_id ? this.requests.get(frame.req_id) : undefined;
        if (frame.req_id) this.requests.delete(frame.req_id);
        if (!key || !this.pending.has(key)) {
          this.options.handlers.frame?.(frame);
          return;
        }
        this.clearAckTimer(key);
        if (frame.code === "unavailable" || frame.code === "internal") {
          this.ackTimers.set(
            key,
            setTimeout(() => this.transmit(key), RETRY_AFTER_ERROR_MS),
          );
        } else {
          this.update(key, { status: "failed", error: frame.message });
        }
        return;
      }
      default:
        this.options.handlers.frame?.(frame);
    }
  }

  private transmit(key: string): void {
    const pending = this.pending.get(key);
    if (!pending || pending.status !== "sending" || !this.live) return;
    this.nextReq += 1;
    const reqId = `q${this.nextReq}`;
    this.requests.set(reqId, key);
    this.sendFrame({
      type: "message.send",
      req_id: reqId,
      room_id: pending.roomId,
      idempotency_key: key,
      content: pending.content,
    });
    this.clearAckTimer(key);
    this.ackTimers.set(
      key,
      setTimeout(() => this.transmit(key), this.options.ackTimeoutMs),
    );
  }

  private sendFrame(frame: ClientFrame): void {
    if (this.live && this.socket) this.socket.send(JSON.stringify(frame));
  }

  private onDisconnect(code: number): void {
    this.live = false;
    this.socket = null;
    this.clearTimers();
    // Whether these were applied is unknown; the caller may try again.
    for (const reqId of [...this.calls.keys()]) {
      this.settle(reqId, { ok: false, code: "offline", message: "the connection dropped" });
    }
    if (this.stopped) return;
    if (code === CLOSE_SESSION_ENDED) {
      this.setState("ended");
      return;
    }
    // Full jitter: spread reconnects so a restarted instance's clients don't
    // all hit the other one at the same moment.
    const ceiling = Math.min(MAX_BACKOFF_MS, MIN_BACKOFF_MS * 2 ** this.attempt);
    this.attempt += 1;
    this.setState("reconnecting");
    this.reconnectTimer = setTimeout(() => this.connect(), this.options.random() * ceiling);
  }

  private resetIdle(): void {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => {
      // A silent connection may be half-open: drop it and start over.
      const socket = this.socket;
      this.onDisconnect(4000);
      socket?.close(4000, "no heartbeat");
    }, this.options.idleTimeoutMs);
  }

  private update(key: string, changes: Partial<PendingSend>): void {
    const current = this.pending.get(key);
    if (!current) return;
    const next = { ...current, ...changes };
    this.pending.set(key, next);
    this.options.handlers.pending?.(next);
  }

  private setState(state: ConnectionState): void {
    if (this.state === state) return;
    this.state = state;
    this.options.handlers.state?.(state);
  }

  private clearAckTimer(key: string): void {
    const timer = this.ackTimers.get(key);
    if (timer) clearTimeout(timer);
    this.ackTimers.delete(key);
  }

  private clearTimers(): void {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.reconnectTimer = null;
    this.idleTimer = null;
    for (const timer of this.ackTimers.values()) clearTimeout(timer);
    this.ackTimers.clear();
  }
}

/** The `/ws` URL on the page's own origin. */
export function socketUrl(location: Location = window.location): string {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  return `${scheme}://${location.host}/ws`;
}
