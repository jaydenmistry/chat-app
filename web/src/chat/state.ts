// The chat UI's state, as a pure reducer: every server frame, REST response
// and user action becomes an action. Pure, so the merge and ordering rules
// are tested without a browser or a server.

import type { HistoryPage } from "../protocol/generated/HistoryPage";
import type { Message } from "../protocol/generated/Message";
import type { PresenceStatus } from "../protocol/generated/PresenceStatus";
import type { ReactionCount } from "../protocol/generated/ReactionCount";
import type { RoomInfo } from "../protocol/generated/RoomInfo";
import type { UserProfile } from "../protocol/generated/UserProfile";
import { REACTIONS } from "../protocol/generated/reactions";
import type { ConnectionState, PendingSend } from "../realtime/client";
import { compareIds, mergeMessages } from "./messages";

/** How long "X is typing" shows after the last typing event. */
export const TYPING_SHOWN_MS = 5_000;
/** Unread counts above this show as "99+" (storage::messages::UNREAD_CAP). */
export const UNREAD_CAP = 99;
/**
 * Updates for a message not loaded yet are kept this long, in case its
 * `message.created` is still on the way (docs/design.md 16.5).
 */
export const EARLY_UPDATE_MS = 60_000;
const MAX_EARLY_UPDATES = 200;

export interface RoomMessages {
  /** Oldest first. */
  items: Message[];
  /** Cursor for the next older page; null at the start of the room. */
  olderCursor: string | null;
  /** Whether the latest page has been loaded at least once. */
  loaded: boolean;
  /** Changes after this point may be missing from `items`; sent as `changes_after`. */
  changeCursor: string | null;
}

/** A change to a message, from a frame. */
export type MessageChange =
  | { kind: "updated"; messageId: string; content: string; editedAt: string; version: number }
  | { kind: "deleted"; messageId: string; version: number }
  | {
      kind: "reaction";
      messageId: string;
      emoji: string;
      count: number;
      userId: string;
      active: boolean;
      changeId: string;
    };

interface EarlyUpdate {
  roomId: string;
  change: MessageChange;
  /** When it arrived (ms), for expiry. */
  at: number;
}

export interface ChatState {
  me: string;
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
  /** Changes that arrived before their message did. */
  early: EarlyUpdate[];
  /** `<message id> <emoji>` -> the newest reaction change applied. */
  reactionClock: Record<string, string>;
  /** In DMs: how far the other person has read. */
  peerRead: Record<string, string>;
  /** Counts `roomJoined` actions, so a stale room list can't undo a join. */
  joinSeq: number;
  /** room -> the `joinSeq` it was joined at. */
  joinedAt: Record<string, number>;
}

export function initialState(me = ""): ChatState {
  return {
    me,
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
    early: [],
    reactionClock: {},
    peerRead: {},
    joinSeq: 0,
    joinedAt: {},
  };
}

export type Action =
  | { type: "connection"; state: ConnectionState }
  /**
   * The full room list. `since` is the `joinSeq` when the request started:
   * rooms joined after that may be missing from it and are kept.
   */
  | { type: "rooms"; rooms: RoomInfo[]; since?: number }
  | { type: "roomJoined"; room: RoomInfo; select?: boolean }
  | { type: "roomLeft"; roomId: string; notice?: string }
  | { type: "select"; roomId: string }
  | { type: "history"; roomId: string; page: HistoryPage; replace: boolean }
  | { type: "messages"; roomId: string; messages: Message[] }
  /** A live `message.created`. `viewing`: the room is open in a visible tab. */
  | { type: "created"; message: Message; viewing: boolean }
  | { type: "synced"; roomId: string; messages: Message[]; changed: Message[]; changeCursor: string }
  | { type: "change"; roomId: string; change: MessageChange; now: number }
  /** The caller's read position moved (this tab or another). */
  | { type: "read"; roomId: string; messageId: string }
  | { type: "peerRead"; roomId: string; messageId: string }
  | { type: "pending"; pending: PendingSend }
  | { type: "acked"; key: string; message: Message }
  | { type: "discard"; key: string }
  | { type: "typing"; roomId: string; userId: string; now: number }
  | { type: "expireTyping"; now: number }
  | { type: "presence"; userId: string; status: PresenceStatus }
  | { type: "profiles"; profiles: UserProfile[] }
  | { type: "notice"; notice: string | null };

const emptyRoom: RoomMessages = { items: [], olderCursor: null, loaded: false, changeCursor: null };

/** The earlier of two cursors: what is known to be current from both. */
function earlier(a: string | null, b: string): string {
  return a === null || compareIds(b, a) < 0 ? b : a;
}

function withMessages(state: ChatState, roomId: string, incoming: Message[]): ChatState {
  const room = state.messages[roomId] ?? emptyRoom;
  const items = mergeMessages(room.items, incoming);
  if (items === room.items) return state;
  const next = settle(state, roomId, items, incoming);
  // A new message from someone ends their typing indicator.
  let typing = next.typing;
  const typers = typing[roomId];
  if (typers) {
    const authors = new Set(incoming.map((m) => m.author_id));
    const remaining = Object.fromEntries(Object.entries(typers).filter(([user]) => !authors.has(user)));
    typing = { ...typing, [roomId]: remaining };
  }
  return { ...next, typing };
}

/**
 * Stores a room's new item list, first applying any changes that were
 * waiting for one of the `arrived` messages.
 */
function settle(state: ChatState, roomId: string, merged: Message[], arrived: Message[]): ChatState {
  const room = state.messages[roomId] ?? emptyRoom;
  let items = merged;
  let next = state;
  const ids = new Set(arrived.map((m) => m.id));
  const ready = state.early.filter((e) => e.roomId === roomId && ids.has(e.change.messageId));
  if (ready.length > 0) {
    next = { ...next, early: state.early.filter((e) => !ready.includes(e)) };
    for (const e of ready) {
      const applied = applyChange(next, items, e.change);
      items = applied.items;
      next = applied.state;
    }
  }
  return { ...next, messages: { ...next.messages, [roomId]: { ...room, items } } };
}

function sortReactions(list: ReactionCount[]): ReactionCount[] {
  const rank = (emoji: string) => {
    const i = (REACTIONS as readonly string[]).indexOf(emoji);
    return i === -1 ? REACTIONS.length : i;
  };
  return [...list].sort((a, b) => rank(a.emoji) - rank(b.emoji));
}

/** Applies a change to a loaded message. Returns the items unchanged if it doesn't apply. */
function applyChange(
  state: ChatState,
  items: Message[],
  change: MessageChange,
): { items: Message[]; state: ChatState } {
  const index = items.findIndex((m) => m.id === change.messageId);
  const current = items[index];
  if (!current || current.deleted) return { items, state };
  let updated: Message = current;
  let next = state;
  switch (change.kind) {
    case "updated":
      if (change.version <= current.version) return { items, state };
      updated = { ...current, content: change.content, edited_at: change.editedAt, version: change.version };
      break;
    case "deleted":
      updated = { ...current, deleted: true, content: "", reactions: [], version: Math.max(current.version, change.version) };
      break;
    case "reaction": {
      const key = `${change.messageId} ${change.emoji}`;
      const seen = state.reactionClock[key];
      // Of two reaction frames, the higher change ID is newer.
      if (seen && compareIds(change.changeId, seen) <= 0) return { items, state };
      next = { ...state, reactionClock: { ...state.reactionClock, [key]: change.changeId } };
      const existing = current.reactions.find((r) => r.emoji === change.emoji);
      const me = change.userId === state.me ? change.active : (existing?.me ?? false);
      const others = current.reactions.filter((r) => r.emoji !== change.emoji);
      const reactions =
        change.count > 0 ? sortReactions([...others, { emoji: change.emoji, count: change.count, me }]) : others;
      updated = { ...current, reactions };
      break;
    }
  }
  const copy = [...items];
  copy[index] = updated;
  return { items: copy, state: next };
}

function sortRooms(rooms: RoomInfo[]): RoomInfo[] {
  // Rooms by name, then DMs by the other person's name.
  const label = (r: RoomInfo) => (r.kind === "dm" ? `~${r.dm_peer?.display_name ?? ""}` : (r.name ?? ""));
  return [...rooms].sort((a, b) => label(a).localeCompare(label(b)));
}

function updateRoom(state: ChatState, roomId: string, update: (room: RoomInfo) => RoomInfo): ChatState {
  let changed = false;
  const rooms = state.rooms.map((r) => {
    if (r.id !== roomId) return r;
    const next = update(r);
    if (next !== r) changed = true;
    return next;
  });
  return changed ? { ...state, rooms } : state;
}

/** Unread messages in what is loaded: from others, not deleted, after `readId`. */
function countUnread(state: ChatState, roomId: string, readId: string): number {
  const items = state.messages[roomId]?.items ?? [];
  return items.filter((m) => m.author_id !== state.me && !m.deleted && compareIds(m.id, readId) > 0).length;
}

export function reducer(state: ChatState, action: Action): ChatState {
  switch (action.type) {
    case "connection":
      return { ...state, connection: action.state };
    case "rooms": {
      const listed = new Set(action.rooms.map((r) => r.id));
      const since = action.since ?? state.joinSeq;
      const newer = state.rooms.filter((r) => !listed.has(r.id) && (state.joinedAt[r.id] ?? -1) > since);
      const rooms = sortRooms([...action.rooms, ...newer]);
      const ids = new Set(rooms.map((r) => r.id));
      const activeRoomId = state.activeRoomId && ids.has(state.activeRoomId) ? state.activeRoomId : (rooms[0]?.id ?? null);
      // Drop data for rooms we're no longer in.
      const messages = Object.fromEntries(Object.entries(state.messages).filter(([id]) => ids.has(id)));
      return { ...state, rooms, activeRoomId, messages };
    }
    case "roomJoined": {
      const rooms = sortRooms([...state.rooms.filter((r) => r.id !== action.room.id), action.room]);
      const joinSeq = state.joinSeq + 1;
      return {
        ...state,
        rooms,
        joinSeq,
        joinedAt: { ...state.joinedAt, [action.room.id]: joinSeq },
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
      // An older page is current as of now, the rest only as of the last
      // cursor: keep the earlier, so the next sync covers both.
      const changeCursor = action.replace
        ? action.page.change_cursor
        : earlier(room.changeCursor, action.page.change_cursor);
      const withPage = {
        ...state,
        messages: {
          ...state.messages,
          [action.roomId]: { ...room, olderCursor: action.page.next_before, loaded: true, changeCursor },
        },
      };
      return settle(withPage, action.roomId, items, action.page.messages);
    }
    case "messages":
      return withMessages(state, action.roomId, action.messages);
    case "created": {
      const { message } = action;
      const known = state.messages[message.room_id]?.items.some((m) => m.id === message.id) ?? false;
      const next = withMessages(state, message.room_id, [message]);
      if (known || action.viewing || message.author_id === state.me) return next;
      return updateRoom(next, message.room_id, (room) => {
        if (compareIds(message.id, room.last_read_id) <= 0) return room;
        const unread = Math.min(room.unread + 1, UNREAD_CAP);
        return { ...room, unread, unread_capped: room.unread_capped || room.unread + 1 > UNREAD_CAP };
      });
    }
    case "synced": {
      const room = state.messages[action.roomId] ?? emptyRoom;
      const next = withMessages(state, action.roomId, [...action.messages, ...action.changed]);
      const current = next.messages[action.roomId] ?? room;
      return {
        ...next,
        messages: { ...next.messages, [action.roomId]: { ...current, changeCursor: action.changeCursor } },
      };
    }
    case "change": {
      const room = state.messages[action.roomId];
      const loaded = room?.items.some((m) => m.id === action.change.messageId) ?? false;
      if (!room || !loaded) {
        // Not here yet: keep it for when the message arrives.
        const live = state.early.filter((e) => action.now - e.at < EARLY_UPDATE_MS);
        const early = [...live, { roomId: action.roomId, change: action.change, at: action.now }].slice(-MAX_EARLY_UPDATES);
        return { ...state, early };
      }
      const applied = applyChange(state, room.items, action.change);
      if (applied.items === room.items) return applied.state;
      return {
        ...applied.state,
        messages: { ...applied.state.messages, [action.roomId]: { ...room, items: applied.items } },
      };
    }
    case "read":
      return updateRoom(state, action.roomId, (room) => {
        if (compareIds(action.messageId, room.last_read_id) <= 0) return room;
        const unread = countUnread(state, action.roomId, action.messageId);
        return { ...room, last_read_id: action.messageId, unread, unread_capped: false };
      });
    case "peerRead": {
      const seen = state.peerRead[action.roomId];
      if (seen && compareIds(action.messageId, seen) <= 0) return state;
      return { ...state, peerRead: { ...state.peerRead, [action.roomId]: action.messageId } };
    }
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
      const early = state.early.filter((e) => action.now - e.at < EARLY_UPDATE_MS);
      return {
        ...state,
        clock: action.now,
        typing: changed ? typing : state.typing,
        early: early.length === state.early.length ? state.early : early,
      };
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

export interface SyncCursorOut {
  room_id: string;
  after: string;
  changes_after?: string;
}

/** The newest loaded message per room, and its change cursor, for sync. */
export function syncCursors(state: ChatState): SyncCursorOut[] {
  return Object.entries(state.messages)
    .filter(([, room]) => room.loaded && room.items.length > 0)
    .flatMap(([roomId, room]) => {
      const last = room.items.at(-1);
      if (!last) return [];
      const cursor: SyncCursorOut = { room_id: roomId, after: last.id };
      if (room.changeCursor) cursor.changes_after = room.changeCursor;
      return [cursor];
    });
}

/** Messages for display: stored ones by ID, then this room's pending sends. */
export function visibleMessages(state: ChatState, roomId: string) {
  const items = state.messages[roomId]?.items ?? [];
  return { items, pending: state.pending.filter((p) => p.roomId === roomId) };
}

/** The caller's newest message in a room that's still shown, for "Seen". */
export function myLatest(state: ChatState, roomId: string): Message | null {
  const items = state.messages[roomId]?.items ?? [];
  for (let i = items.length - 1; i >= 0; i--) {
    const m = items[i];
    if (m && m.author_id === state.me && !m.deleted) return m;
  }
  return null;
}

/** "3", or "99+" past the cap. */
export function unreadLabel(room: RoomInfo): string | null {
  if (room.unread === 0) return null;
  return room.unread_capped ? `${UNREAD_CAP}+` : String(room.unread);
}
