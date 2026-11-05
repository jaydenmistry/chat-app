import { type KeyboardEvent, useEffect, useLayoutEffect, useRef, useState } from "react";
import { ApiRequestError, api } from "../api";
import type { Member } from "../protocol/generated/Member";
import type { Me } from "../protocol/generated/Me";
import type { RoomInfo } from "../protocol/generated/RoomInfo";
import type { PendingSend } from "../realtime/client";
import { PresenceDot, roomLabel } from "./ui";
import { type ChatState, typers, visibleMessages } from "./state";
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

function MessageList({
  roomId,
  items,
  pending,
  loaded,
  hasOlder,
  name,
  meId,
  actions,
}: {
  roomId: string;
  items: ChatState["messages"][string]["items"];
  pending: PendingSend[];
  loaded: boolean;
  hasOlder: boolean;
  name: (id: string) => string;
  meId: string;
  actions: Actions;
}) {
  const scroller = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);
  const [loadingOlder, setLoadingOlder] = useState(false);

  // Follow new messages only if the reader is already at the bottom.
  useLayoutEffect(() => {
    const el = scroller.current;
    if (el && stickToBottom.current) el.scrollTop = el.scrollHeight;
  }, [items.length, pending.length]);

  useEffect(() => {
    stickToBottom.current = true;
  }, [roomId]);

  const onScroll = () => {
    const el = scroller.current;
    if (el) stickToBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
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
        <article key={m.id} className={`message${m.author_id === meId ? " mine" : ""}`} data-message-id={m.id}>
          <header>
            <strong>{name(m.author_id)}</strong>{" "}
            <time dateTime={m.created_at} className="hint">
              {new Date(m.created_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
            </time>
            {m.edited_at && <span className="hint"> (edited)</span>}
          </header>
          {m.deleted ? (
            <p className="content deleted">Message deleted</p>
          ) : (
            // Plain text only: React escapes it, and bidi isolation keeps
            // direction-override characters from reordering the page.
            <p className="content">{m.content}</p>
          )}
        </article>
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
