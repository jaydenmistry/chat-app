// The chat UI's state, as a pure reducer: every server frame, REST response
// and user action becomes an action. Pure, so the merge and ordering rules
// are tested without a browser or a server.

import type { HistoryPage } from "../protocol/generated/HistoryPage";
import type { Message } from "../protocol/generated/Message";
import type { PresenceStatus } from "../protocol/generated/PresenceStatus";
import type { RoomInfo } from "../protocol/generated/RoomInfo";
import type { UserProfile } from "../protocol/generated/UserProfile";
import type { ConnectionState, PendingSend } from "../realtime/client";
import { mergeMessages } from "./messages";

/** How long "X is typing" shows after the last typing event. */
export const TYPING_SHOWN_MS = 5_000;

export interface RoomMessages {
  /** Oldest first. */
  items: Message[];
  /** Cursor for the next older page; null at the start of the room. */
  olderCursor: string | null;
  /** Whether the latest page has been loaded at least once. */
  loaded: boolean;
}

export interface ChatState {
  connection: ConnectionState;
  rooms: RoomInfo[];
  activeRoomId: string | null;
  messages: Record<string, RoomMessages>;
  /** Unacknowledged sends, in the order they were written. */
  pending: PendingSend[];
  /** room -> user -> time (ms) the indicator stops showing. */
  typing: Record<string, Record<string, number>>;
  presence: Record<string, PresenceStatus>;
  profiles: Record<string, UserProfile>;
  notice: string | null;
  /** Updated every second; render reads the time from here, not Date.now(). */
  clock: number;
}

export const initialState: ChatState = {
  connection: "connecting",
  rooms: [],
  activeRoomId: null,
  messages: {},
  pending: [],
  typing: {},
  presence: {},
  profiles: {},
  notice: null,
  clock: 0,
};

export type Action =
  | { type: "connection"; state: ConnectionState }
  | { type: "rooms"; rooms: RoomInfo[] }
  | { type: "roomJoined"; room: RoomInfo; select?: boolean }
  | { type: "roomLeft"; roomId: string; notice?: string }
  | { type: "select"; roomId: string }
  | { type: "history"; roomId: string; page: HistoryPage; replace: boolean }
  | { type: "messages"; roomId: string; messages: Message[] }
  | { type: "pending"; pending: PendingSend }
  | { type: "acked"; key: string; message: Message }
  | { type: "discard"; key: string }
  | { type: "typing"; roomId: string; userId: string; now: number }
  | { type: "expireTyping"; now: number }
  | { type: "presence"; userId: string; status: PresenceStatus }
  | { type: "profiles"; profiles: UserProfile[] }
  | { type: "notice"; notice: string | null };

const emptyRoom: RoomMessages = { items: [], olderCursor: null, loaded: false };

function withMessages(state: ChatState, roomId: string, incoming: Message[]): ChatState {
  const room = state.messages[roomId] ?? emptyRoom;
  const items = mergeMessages(room.items, incoming);
  if (items === room.items) return state;
  // A new message from someone ends their typing indicator.
  let typing = state.typing;
  const typers = typing[roomId];
  if (typers) {
    const authors = new Set(incoming.map((m) => m.author_id));
    const remaining = Object.fromEntries(Object.entries(typers).filter(([user]) => !authors.has(user)));
    typing = { ...typing, [roomId]: remaining };
  }
  return { ...state, typing, messages: { ...state.messages, [roomId]: { ...room, items } } };
}

function sortRooms(rooms: RoomInfo[]): RoomInfo[] {
  // Rooms by name, then DMs by the other person's name.
  const label = (r: RoomInfo) => (r.kind === "dm" ? `~${r.dm_peer?.display_name ?? ""}` : (r.name ?? ""));
  return [...rooms].sort((a, b) => label(a).localeCompare(label(b)));
}

export function reducer(state: ChatState, action: Action): ChatState {
  switch (action.type) {
    case "connection":
      return { ...state, connection: action.state };
    case "rooms": {
      const rooms = sortRooms(action.rooms);
      const ids = new Set(rooms.map((r) => r.id));
      const activeRoomId = state.activeRoomId && ids.has(state.activeRoomId) ? state.activeRoomId : (rooms[0]?.id ?? null);
      // Drop data for rooms we're no longer in.
      const messages = Object.fromEntries(Object.entries(state.messages).filter(([id]) => ids.has(id)));
      return { ...state, rooms, activeRoomId, messages };
    }
    case "roomJoined": {
      const rooms = sortRooms([...state.rooms.filter((r) => r.id !== action.room.id), action.room]);
      return {
        ...state,
        rooms,
        activeRoomId: action.select || !state.activeRoomId ? action.room.id : state.activeRoomId,
      };
    }
    case "roomLeft": {
      if (!state.rooms.some((r) => r.id === action.roomId)) return state;
      const rooms = state.rooms.filter((r) => r.id !== action.roomId);
      const messages = Object.fromEntries(Object.entries(state.messages).filter(([id]) => id !== action.roomId));
      return {
        ...state,
        rooms,
        messages,
        pending: state.pending.filter((p) => p.roomId !== action.roomId),
        activeRoomId: state.activeRoomId === action.roomId ? (rooms[0]?.id ?? null) : state.activeRoomId,
        notice: action.notice ?? state.notice,
      };
    }
    case "select":
      return { ...state, activeRoomId: action.roomId };
    case "history": {
      const room = state.messages[action.roomId] ?? emptyRoom;
      // `replace` reloads the latest page (after a sync too large to patch);
      // otherwise this is the first page or an older one. Either way the new
      // page's cursor is where the next older page starts.
      const items = mergeMessages(action.replace ? [] : room.items, action.page.messages);
      return {
        ...state,
        messages: {
          ...state.messages,
          [action.roomId]: { items, olderCursor: action.page.next_before, loaded: true },
        },
      };
    }
    case "messages":
      return withMessages(state, action.roomId, action.messages);
    case "pending": {
      const exists = state.pending.some((p) => p.key === action.pending.key);
      return {
        ...state,
        pending: exists
          ? state.pending.map((p) => (p.key === action.pending.key ? action.pending : p))
          : [...state.pending, action.pending],
      };
    }
    case "acked": {
      const next = withMessages(state, action.message.room_id, [action.message]);
      return { ...next, pending: next.pending.filter((p) => p.key !== action.key) };
    }
    case "discard":
      return { ...state, pending: state.pending.filter((p) => p.key !== action.key) };
    case "typing": {
      const room = state.typing[action.roomId] ?? {};
      return {
        ...state,
        typing: { ...state.typing, [action.roomId]: { ...room, [action.userId]: action.now + TYPING_SHOWN_MS } },
      };
    }
    case "expireTyping": {
      let changed = false;
      const typing: ChatState["typing"] = {};
      for (const [roomId, users] of Object.entries(state.typing)) {
        const live = Object.fromEntries(Object.entries(users).filter(([, until]) => until > action.now));
        if (Object.keys(live).length !== Object.keys(users).length) changed = true;
        typing[roomId] = live;
      }
      return { ...state, clock: action.now, typing: changed ? typing : state.typing };
    }
    case "presence":
      return { ...state, presence: { ...state.presence, [action.userId]: action.status } };
    case "profiles": {
      const profiles = { ...state.profiles };
      for (const p of action.profiles) profiles[p.id] = p;
      return { ...state, profiles };
    }
    case "notice":
      return { ...state, notice: action.notice };
  }
}

/** Who is typing in a room right now, excluding `me`. */
export function typers(state: ChatState, roomId: string, me: string, now: number): string[] {
  return Object.entries(state.typing[roomId] ?? {})
    .filter(([userId, until]) => userId !== me && until > now)
    .map(([userId]) => userId);
}

/** IDs of message authors we have no profile for yet. */
export function unknownAuthors(state: ChatState, messages: Message[]): string[] {
  return [...new Set(messages.map((m) => m.author_id))].filter((id) => !state.profiles[id]);
}

/** The newest loaded message per room, for sync. */
export function syncCursors(state: ChatState): { room_id: string; after: string }[] {
  return Object.entries(state.messages)
    .filter(([, room]) => room.loaded && room.items.length > 0)
    .flatMap(([roomId, room]) => {
      const last = room.items.at(-1);
      return last ? [{ room_id: roomId, after: last.id }] : [];
    });
}

/** Messages for display: stored ones by ID, then this room's pending sends. */
export function visibleMessages(state: ChatState, roomId: string) {
  const items = state.messages[roomId]?.items ?? [];
  return { items, pending: state.pending.filter((p) => p.roomId === roomId) };
}

