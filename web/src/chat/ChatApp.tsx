import { type FormEvent, type ReactNode, useEffect, useState } from "react";
import { Account } from "../Account";
import { ApiRequestError, api } from "../api";
import type { Me } from "../protocol/generated/Me";
import type { PublicRoom } from "../protocol/generated/PublicRoom";
import type { RoomInfo } from "../protocol/generated/RoomInfo";
import { RoomView } from "./RoomView";
import { PresenceDot, roomLabel } from "./ui";
import { useChat } from "./useChat";

type Dialog = "none" | "newRoom" | "browse" | "newDm";

export function ChatApp({ me, onSignedOut }: { me: Me; onSignedOut: () => void }) {
  const { state, members, actions } = useChat(me, onSignedOut);
  const [dialog, setDialog] = useState<Dialog>("none");
  const [showAccount, setShowAccount] = useState(false);

  if (showAccount) {
    return (
      <div>
        <button type="button" className="link back" onClick={() => setShowAccount(false)}>
          ← Back to chat
        </button>
        <Account me={me} onSignedOut={onSignedOut} />
      </div>
    );
  }

  const active = state.rooms.find((r) => r.id === state.activeRoomId) ?? null;
  const rooms = state.rooms.filter((r) => r.kind !== "dm");
  const dms = state.rooms.filter((r) => r.kind === "dm");

  return (
    <div className="chat">
      <aside className="sidebar" aria-label="Rooms">
        <div className="sidebar-head">
          <strong>Chat</strong>
          <ConnectionPill state={state.connection} />
        </div>

        <nav>
          <SidebarSection title="Rooms">
            {rooms.map((room) => (
              <RoomButton key={room.id} room={room} active={room.id === state.activeRoomId} onSelect={actions.select} />
            ))}
          </SidebarSection>
          <div className="sidebar-actions">
            {!me.is_guest && (
              <button type="button" className="secondary small" onClick={() => setDialog("newRoom")}>
                New room
              </button>
            )}
            <button type="button" className="secondary small" onClick={() => setDialog("browse")}>
              Browse rooms
            </button>
          </div>
          <SidebarSection title="Direct messages">
            {dms.map((room) => (
              <RoomButton key={room.id} room={room} active={room.id === state.activeRoomId} onSelect={actions.select}>
                <PresenceDot status={room.dm_peer ? state.presence[room.dm_peer.id] : undefined} />
              </RoomButton>
            ))}
          </SidebarSection>
          <div className="sidebar-actions">
            <button type="button" className="secondary small" onClick={() => setDialog("newDm")}>
              New message
            </button>
          </div>
        </nav>

        <div className="sidebar-foot">
          <button type="button" className="link" onClick={() => setShowAccount(true)}>
            {me.display_name}
          </button>
          <span className="hint">@{me.handle}</span>
        </div>
      </aside>

      <main className="room">
        {state.notice && (
          <div className="notice" role="status">
            {state.notice}
            <button type="button" className="link" onClick={actions.dismissNotice}>
              Dismiss
            </button>
          </div>
        )}
        {active ? (
          <RoomView key={active.id} me={me} room={active} state={state} members={members} actions={actions} />
        ) : (
          <div className="empty">
            <p>You're not in any rooms yet.</p>
            <button type="button" onClick={() => setDialog("browse")}>
              Browse public rooms
            </button>
          </div>
        )}
      </main>

      {dialog === "newRoom" && <NewRoomDialog onClose={() => setDialog("none")} onCreated={actions.joined} />}
      {dialog === "browse" && <BrowseDialog onClose={() => setDialog("none")} onJoined={actions.joined} />}
      {dialog === "newDm" && <NewDmDialog onClose={() => setDialog("none")} onOpened={actions.joined} />}
    </div>
  );
}

function ConnectionPill({ state }: { state: string }) {
  const label = state === "live" ? "Connected" : state === "ended" ? "Signed out" : "Reconnecting…";
  return (
    <span className={`pill pill-${state === "live" ? "ok" : "warn"}`} role="status" aria-live="polite">
      {label}
    </span>
  );
}

function SidebarSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section>
      <h2 className="sidebar-title">{title}</h2>
      <ul className="room-list">{children}</ul>
    </section>
  );
}

function RoomButton({
  room,
  active,
  onSelect,
  children,
}: {
  room: RoomInfo;
  active: boolean;
  onSelect: (id: string) => void;
  children?: ReactNode;
}) {
  return (
    <li>
      <button
        type="button"
        className={`room-button${active ? " active" : ""}`}
        aria-current={active ? "page" : undefined}
        onClick={() => onSelect(room.id)}
      >
        {children}
        <span className="room-name">{roomLabel(room)}</span>
      </button>
    </li>
  );
}

function Modal({ title, onClose, children }: { title: string; onClose: () => void; children: ReactNode }) {
  return (
    <div className="backdrop" onClick={onClose}>
      <div className="card modal" role="dialog" aria-modal="true" aria-label={title} onClick={(e) => e.stopPropagation()}>
        <h2>{title}</h2>
        {children}
        <button type="button" className="secondary" onClick={onClose}>
          Close
        </button>
      </div>
    </div>
  );
}

function useSubmit<T>(action: () => Promise<T>, onDone: (value: T) => void) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      onDone(await action());
    } catch (err) {
      setError(err instanceof ApiRequestError ? err.body.message : String(err));
    } finally {
      setBusy(false);
    }
  };
  return { submit, error, busy };
}

function NewRoomDialog({ onClose, onCreated }: { onClose: () => void; onCreated: (room: RoomInfo) => void }) {
  const [name, setName] = useState("");
  const [kind, setKind] = useState<"public" | "private">("public");
  const { submit, error, busy } = useSubmit(
    () => api.createRoom(name, kind),
    (room) => {
      onCreated(room);
      onClose();
    },
  );
  return (
    <Modal title="New room" onClose={onClose}>
      <form onSubmit={submit}>
        <div className="field">
          <label htmlFor="room-name">Name</label>
          <input id="room-name" value={name} onChange={(e) => setName(e.target.value)} maxLength={64} autoFocus />
        </div>
        <fieldset className="field">
          <legend>Who can join</legend>
          <label>
            <input type="radio" checked={kind === "public"} onChange={() => setKind("public")} /> Anyone (public)
          </label>
          <label>
            <input type="radio" checked={kind === "private"} onChange={() => setKind("private")} /> Invite only
            (private)
          </label>
        </fieldset>
        {error && <p className="error" role="alert">{error}</p>}
        <button type="submit" disabled={busy || name.trim() === ""}>
          Create room
        </button>
      </form>
    </Modal>
  );
}

function BrowseDialog({ onClose, onJoined }: { onClose: () => void; onJoined: (room: RoomInfo) => void }) {
  const [rooms, setRooms] = useState<PublicRoom[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    api.publicRooms().then(setRooms, (err: unknown) => setError(String(err)));
  }, []);
  const join = async (id: string) => {
    try {
      onJoined(await api.joinRoom(id));
      onClose();
    } catch (err) {
      setError(err instanceof ApiRequestError ? err.body.message : String(err));
    }
  };
  return (
    <Modal title="Public rooms" onClose={onClose}>
      {error && <p className="error" role="alert">{error}</p>}
      {rooms === null ? (
        <p>Loading…</p>
      ) : rooms.length === 0 ? (
        <p className="hint">No public rooms yet.</p>
      ) : (
        <ul className="directory">
          {rooms.map((room) => (
            <li key={room.id}>
              <span>
                # {room.name} <span className="hint">· {room.member_count} members</span>
              </span>
              {room.joined ? (
                <span className="hint">Joined</span>
              ) : (
                <button type="button" className="small" onClick={() => void join(room.id)}>
                  Join
                </button>
              )}
            </li>
          ))}
        </ul>
      )}
    </Modal>
  );
}

function NewDmDialog({ onClose, onOpened }: { onClose: () => void; onOpened: (room: RoomInfo) => void }) {
  const [handle, setHandle] = useState("");
  const { submit, error, busy } = useSubmit(
    () => api.openDm(handle),
    (room) => {
      onOpened(room);
      onClose();
    },
  );
  return (
    <Modal title="New message" onClose={onClose}>
      <form onSubmit={submit}>
        <div className="field">
          <label htmlFor="dm-handle">Their handle</label>
          <input id="dm-handle" value={handle} onChange={(e) => setHandle(e.target.value)} placeholder="@handle" autoFocus />
        </div>
        {error && <p className="error" role="alert">{error}</p>}
        <button type="submit" disabled={busy || handle.trim() === ""}>
          Open conversation
        </button>
      </form>
    </Modal>
  );
}
