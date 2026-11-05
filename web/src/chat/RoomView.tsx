import { type KeyboardEvent, useEffect, useLayoutEffect, useRef, useState } from "react";
import { ApiRequestError, api } from "../api";
import type { Member } from "../protocol/generated/Member";
import type { Me } from "../protocol/generated/Me";
import type { Message } from "../protocol/generated/Message";
import type { RoomInfo } from "../protocol/generated/RoomInfo";
import { REACTIONS } from "../protocol/generated/reactions";
import type { CallResult, PendingSend } from "../realtime/client";
import { compareIds } from "./messages";
import { PresenceDot, roomLabel } from "./ui";
import { type ChatState, myLatest, typers, visibleMessages } from "./state";
import type { useChat } from "./useChat";

type Actions = ReturnType<typeof useChat>["actions"];

/** Message content limit, in UTF-8 bytes (protocol::validate). */
export const MAX_CONTENT_BYTES = 4096;

export function RoomView({
  me,
  room,
  state,
  members,
  actions,
}: {
  me: Me;
  room: RoomInfo;
  state: ChatState;
  members: Member[];
  actions: Actions;
}) {
  const [showMembers, setShowMembers] = useState(false);
  const [invite, setInvite] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const { items, pending } = visibleMessages(state, room.id);
  const loaded = state.messages[room.id]?.loaded ?? false;
  const olderCursor = state.messages[room.id]?.olderCursor ?? null;
  const name = (id: string) => state.profiles[id]?.display_name ?? "Someone";
  const typing = typers(state, room.id, me.id, state.clock);
  const isOwner = room.role === "owner";
  const seen = useSeen(room, state, actions);

  const run = async (action: () => Promise<unknown>) => {
    setError(null);
    try {
      await action();
    } catch (err) {
      setError(err instanceof ApiRequestError ? err.body.message : String(err));
    }
  };

  return (
    <div className="room-view">
      <header className="room-head">
        <h1>
          {room.kind === "dm" && <PresenceDot status={room.dm_peer ? state.presence[room.dm_peer.id] : undefined} />}
          {roomLabel(room)}
        </h1>
        <div className="room-actions">
          {room.kind !== "dm" && (
            <button type="button" className="secondary small" onClick={() => setShowMembers(!showMembers)}>
              Members ({members.length})
            </button>
          )}
          {isOwner && room.kind === "private" && !me.is_guest && (
            <button
              type="button"
              className="secondary small"
              onClick={() =>
                void run(async () => {
                  const created = await api.createInvite(room.id);
                  setInvite(`${window.location.origin}/invite/${created.code}`);
                })
              }
            >
              Invite link
            </button>
          )}
          {room.kind !== "dm" && !isOwner && (
            <button
              type="button"
              className="secondary small"
              onClick={() =>
                void run(async () => {
                  await api.leaveRoom(room.id);
                  actions.left(room.id);
                })
              }
            >
              Leave
            </button>
          )}
          {isOwner && room.kind !== "dm" && (
            <button
              type="button"
              className="secondary small danger"
              onClick={() =>
                void run(async () => {
                  if (!window.confirm(`Delete ${room.name}? This can't be undone.`)) return;
                  await api.deleteRoom(room.id);
                  actions.left(room.id);
                })
              }
            >
              Delete
            </button>
          )}
        </div>
      </header>
      {invite && (
        <p className="notice">
          Invite link (valid 7 days): <code>{invite}</code>
        </p>
      )}
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}

      <div className="room-body">
        <MessageList
          roomId={room.id}
          items={items}
          pending={pending}
          loaded={loaded}
          hasOlder={olderCursor !== null}
          name={name}
          meId={me.id}
          lastReadId={room.last_read_id}
          seen={seen}
          actions={actions}
        />
        {showMembers && (
          <aside className="members" aria-label="Members">
            <h2>Members</h2>
            <ul>
              {members.map((m) => (
                <li key={m.user.id}>
                  <PresenceDot status={m.user.id === me.id ? "online" : state.presence[m.user.id]} />
                  <span>
                    {m.user.display_name} <span className="hint">@{m.user.handle}</span>
                    {m.role === "owner" && <span className="badge">owner</span>}
                  </span>
                  {isOwner && m.role !== "owner" && (
                    <button
                      type="button"
                      className="link small"
                      onClick={() =>
                        void run(async () => {
                          await api.removeMember(room.id, m.user.id);
                          await actions.refreshMembers(room.id);
                        })
                      }
                    >
                      Remove
                    </button>
                  )}
                </li>
              ))}
            </ul>
          </aside>
        )}
      </div>

      <p className="typing" aria-live="polite">
        {typing.length === 1 && `${name(typing[0] ?? "")} is typing…`}
        {typing.length > 1 && `${typing.length} people are typing…`}
      </p>
      <Composer roomId={room.id} actions={actions} label={`Message ${roomLabel(room)}`} />
    </div>
  );
}

/** "Seen" (DM) or "Seen by N" (group) for the caller's latest message. */
interface SeenInfo {
  messageId: string;
  /** Null while nobody has read it (yet, as far as this tab knows). */
  label: string | null;
  names: string;
  refresh: () => void;
}

function useSeen(room: RoomInfo, state: ChatState, actions: Actions): SeenInfo | null {
  const latest = myLatest(state, room.id);
  const latestId = latest?.id ?? null;
  const [receipt, setReceipt] = useState<{ messageId: string; count: number; names: string } | null>(null);
  const [asked, setAsked] = useState(0);

  // Fetched on demand: when the latest message changes, when the room opens,
  // and on hover. Group reads are never pushed (docs/design.md 9).
  useEffect(() => {
    if (!latestId) return;
    let cancelled = false;
    actions.receipts(room.id, latestId).then(
      (r) => {
        if (!cancelled) {
          setReceipt({ messageId: latestId, count: r.count, names: r.readers.map((u) => u.display_name).join(", ") });
        }
      },
      () => {},
    );
    return () => {
      cancelled = true;
    };
  }, [room.id, latestId, asked, actions]);

  if (!latestId) return null;
  const refresh = () => setAsked((n) => n + 1);
  const fetched = receipt?.messageId === latestId ? receipt : null;
  if (room.kind === "dm") {
    const peer = state.peerRead[room.id];
    const live = peer !== undefined && compareIds(peer, latestId) >= 0;
    const label = live || (fetched && fetched.count > 0) ? "Seen" : null;
    return { messageId: latestId, label, names: "", refresh };
  }
  const label = fetched && fetched.count > 0 ? `Seen by ${fetched.count}` : null;
  return { messageId: latestId, label, names: fetched?.names ?? "", refresh };
}

function MessageList({
  roomId,
  items,
  pending,
  loaded,
  hasOlder,
  name,
  meId,
  lastReadId,
  seen,
  actions,
}: {
  roomId: string;
  items: ChatState["messages"][string]["items"];
  pending: PendingSend[];
  loaded: boolean;
  hasOlder: boolean;
  name: (id: string) => string;
  meId: string;
  lastReadId: string;
  seen: SeenInfo | null;
  actions: Actions;
}) {
  const scroller = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);
  const [loadingOlder, setLoadingOlder] = useState(false);
  const newestId = items.at(-1)?.id ?? null;

  // Follow new messages only if the reader is already at the bottom.
  useLayoutEffect(() => {
    const el = scroller.current;
    if (el && stickToBottom.current) el.scrollTop = el.scrollHeight;
  }, [items.length, pending.length]);

  useEffect(() => {
    stickToBottom.current = true;
  }, [roomId]);

  // Read up to the newest message while it's on screen: the tab is visible
  // and the list is scrolled to the bottom.
  const markIfRead = () => {
    if (!newestId || !stickToBottom.current || document.visibilityState !== "visible") return;
    if (compareIds(newestId, lastReadId) > 0) actions.markRead(roomId, newestId);
  };
  const markRef = useRef(markIfRead);
  useEffect(() => {
    markRef.current = markIfRead;
  });
  useEffect(() => {
    markRef.current();
  }, [newestId, lastReadId]);
  useEffect(() => {
    const onVisible = () => markRef.current();
    document.addEventListener("visibilitychange", onVisible);
    return () => document.removeEventListener("visibilitychange", onVisible);
  }, []);

  const onScroll = () => {
    const el = scroller.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
    stickToBottom.current = atBottom;
    if (atBottom) markIfRead();
  };

  const loadOlder = async () => {
    setLoadingOlder(true);
    const el = scroller.current;
    const before = el ? el.scrollHeight - el.scrollTop : 0;
    try {
      await actions.loadOlder(roomId);
    } finally {
      setLoadingOlder(false);
      // Keep the reader's place when older messages are added above.
      requestAnimationFrame(() => {
        if (el) el.scrollTop = el.scrollHeight - before;
      });
    }
  };

  return (
    <div className="messages" ref={scroller} onScroll={onScroll} aria-label="Messages" role="log">
      {hasOlder && (
        <button type="button" className="link older" disabled={loadingOlder} onClick={() => void loadOlder()}>
          {loadingOlder ? "Loading…" : "Load older messages"}
        </button>
      )}
      {loaded && !hasOlder && items.length === 0 && pending.length === 0 && (
        <p className="hint centered-text">No messages yet. Say hello.</p>
      )}
      {items.map((m) => (
        <MessageItem
          key={m.id}
          message={m}
          mine={m.author_id === meId}
          name={name}
          seen={seen?.messageId === m.id ? seen : null}
          actions={actions}
        />
      ))}
      {pending.map((p) => (
        <article key={p.key} className="message mine pending" aria-busy={p.status === "sending"}>
          <header>
            <strong>{name(meId)}</strong>{" "}
            <span className="hint">{p.status === "sending" ? "sending…" : `not sent: ${p.error ?? "error"}`}</span>
          </header>
          <p className="content">{p.content}</p>
          {p.status === "failed" && (
            <div className="pending-actions">
              <button type="button" className="link small" onClick={() => actions.retry(p.key)}>
                Retry
              </button>
              <button type="button" className="link small" onClick={() => actions.discard(p.key)}>
                Discard
              </button>
            </div>
          )}
        </article>
      ))}
    </div>
  );
}

function failure(result: CallResult): string | null {
  if (result.ok) return null;
  return result.code === "offline" || result.code === "timeout"
    ? "Couldn't reach the server. Try again."
    : result.message;
}

function MessageItem({
  message: m,
  mine,
  name,
  seen,
  actions,
}: {
  message: Message;
  mine: boolean;
  name: (id: string) => string;
  seen: SeenInfo | null;
  actions: Actions;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(m.content);
  const [picking, setPicking] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const run = async (call: () => Promise<CallResult>, onDone?: () => void) => {
    setBusy(true);
    setError(null);
    const message = failure(await call());
    setBusy(false);
    if (message) setError(message);
    else onDone?.();
  };

  const saveEdit = () => {
    const content = draft.trim();
    if (content === "" || new TextEncoder().encode(content).length > MAX_CONTENT_BYTES) return;
    if (content === m.content) {
      setEditing(false);
      return;
    }
    void run(
      () => actions.edit(m.room_id, m.id, content),
      () => setEditing(false),
    );
  };

  const onEditKey = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Escape") {
      setEditing(false);
    } else if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
      event.preventDefault();
      saveEdit();
    }
  };

  const toggle = (emoji: string, active: boolean) => {
    setPicking(false);
    void run(() => actions.react(m.room_id, m.id, emoji, active));
  };

  return (
    // Hovering your latest message refreshes who has seen it.
    <article className={`message${mine ? " mine" : ""}`} data-message-id={m.id} onMouseEnter={seen?.refresh}>
      <header>
        <strong>{name(m.author_id)}</strong>{" "}
        <time dateTime={m.created_at} className="hint">
          {new Date(m.created_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
        </time>
        {m.edited_at && !m.deleted && <span className="hint"> (edited)</span>}
        {!m.deleted && !editing && (
          <span className="message-actions">
            <button
              type="button"
              className="link small"
              aria-label="Add reaction"
              aria-expanded={picking}
              onClick={() => setPicking(!picking)}
            >
              React
            </button>
            {mine && (
              <>
                <button
                  type="button"
                  className="link small"
                  onClick={() => {
                    setDraft(m.content);
                    setEditing(true);
                  }}
                >
                  Edit
                </button>
                <button
                  type="button"
                  className="link small"
                  disabled={busy}
                  onClick={() => {
                    if (window.confirm("Delete this message?")) void run(() => actions.remove(m.room_id, m.id));
                  }}
                >
                  Delete
                </button>
              </>
            )}
          </span>
        )}
      </header>
      {m.deleted ? (
        <p className="content deleted">Message deleted</p>
      ) : editing ? (
        <div className="edit">
          <textarea
            aria-label="Edit message"
            value={draft}
            rows={2}
            autoFocus
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={onEditKey}
          />
          <div className="edit-actions">
            <button type="button" className="small" disabled={busy || draft.trim() === ""} onClick={saveEdit}>
              Save
            </button>
            <button type="button" className="secondary small" onClick={() => setEditing(false)}>
              Cancel
            </button>
          </div>
        </div>
      ) : (
        // Plain text only: React escapes it, and bidi isolation keeps
        // direction-override characters from reordering the page.
        <p className="content">{m.content}</p>
      )}
      {picking && (
        <div className="picker" role="group" aria-label="Reactions">
          {REACTIONS.map((emoji) => {
            const mineAlready = m.reactions.some((r) => r.emoji === emoji && r.me);
            return (
              <button key={emoji} type="button" className="emoji" onClick={() => toggle(emoji, !mineAlready)}>
                {emoji}
              </button>
            );
          })}
        </div>
      )}
      {m.reactions.length > 0 && (
        <div className="reactions">
          {m.reactions.map((r) => (
            <button
              key={r.emoji}
              type="button"
              className={`reaction${r.me ? " me" : ""}`}
              aria-pressed={r.me}
              aria-label={`${r.emoji} ${r.count}`}
              disabled={busy}
              onClick={() => toggle(r.emoji, !r.me)}
            >
              {r.emoji} <span>{r.count}</span>
            </button>
          ))}
        </div>
      )}
      {error && (
        <p className="error small-text" role="alert">
          {error}
        </p>
      )}
      {seen?.label && (
        <p className="seen hint" title={seen.names || undefined}>
          {seen.label}
        </p>
      )}
    </article>
  );
}

function Composer({ roomId, actions, label }: { roomId: string; actions: Actions; label: string }) {
  const [text, setText] = useState("");
  const bytes = new TextEncoder().encode(text.trim()).length;
  const tooLong = bytes > MAX_CONTENT_BYTES;

  const send = () => {
    const content = text.trim();
    if (content === "" || tooLong) return;
    actions.send(roomId, content);
    setText("");
  };

  const onKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    // Enter sends; Shift+Enter is a new line.
    if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
      event.preventDefault();
      send();
    }
  };

  return (
    <form
      className="composer"
      onSubmit={(e) => {
        e.preventDefault();
        send();
      }}
    >
      <textarea
        aria-label={label}
        placeholder={label}
        value={text}
        rows={2}
        onChange={(e) => {
          setText(e.target.value);
          if (e.target.value.trim() !== "") actions.typing(roomId);
        }}
        onKeyDown={onKeyDown}
      />
      <div className="composer-side">
        {tooLong && <span className="error">{bytes}/{MAX_CONTENT_BYTES} bytes</span>}
        <button type="submit" disabled={text.trim() === "" || tooLong}>
          Send
        </button>
      </div>
    </form>
  );
}
