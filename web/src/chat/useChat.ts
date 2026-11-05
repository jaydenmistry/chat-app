// Connects the realtime client, the REST API and the reducer.

import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import { api } from "../api";
import type { Member } from "../protocol/generated/Member";
import type { Me } from "../protocol/generated/Me";
import type { Message } from "../protocol/generated/Message";
import type { RoomInfo } from "../protocol/generated/RoomInfo";
import type { ServerFrame } from "../protocol/generated/ServerFrame";
import { type CallResult, RealtimeClient, socketUrl } from "../realtime/client";
import { compareIds } from "./messages";
import { initialState, reducer, syncCursors, unknownAuthors } from "./state";

/** Typing events go out at most this often while the user types. */
const TYPING_EVERY_MS = 3_000;
/** No input for this long (or a hidden tab) reports the user as away. */
const IDLE_AFTER_MS = 5 * 60_000;
/** Read marks go out at most this often per room (docs/design.md 9). */
const READ_MARK_EVERY_MS = 2_000;

interface ReadMarkTimer {
  last: number;
  latest: string;
  timer: ReturnType<typeof setTimeout> | null;
}

function offline(): Promise<CallResult> {
  return Promise.resolve({ ok: false, code: "offline", message: "not connected" });
}

export function useChat(me: Me, onSessionEnded: () => void) {
  const [state, dispatch] = useReducer(reducer, me.id, initialState);
  const [membersByRoom, setMembersByRoom] = useState<Record<string, Member[]>>({});
  const membersRef = useRef(membersByRoom);
  useEffect(() => {
    membersRef.current = membersByRoom;
  });
  // Callbacks registered once (the socket handlers) read the latest state
  // through this ref instead of capturing a stale copy.
  const stateRef = useRef(state);
  useEffect(() => {
    stateRef.current = state;
  });
  const clientRef = useRef<RealtimeClient | null>(null);
  const lastTyping = useRef<Record<string, number>>({});
  const readMarks = useRef<Record<string, ReadMarkTimer>>({});

  const ensureUsers = useCallback((userIds: string[]) => {
    const ids = [...new Set(userIds)].filter((id) => !stateRef.current.profiles[id]);
    if (ids.length > 0) {
      api.profiles(ids).then((profiles) => dispatch({ type: "profiles", profiles }), () => {});
    }
  }, []);
  const ensureProfiles = useCallback(
    (messages: Message[]) => ensureUsers(unknownAuthors(stateRef.current, messages)),
    [ensureUsers],
  );
  // The member list of the open room is fetched when the room is opened.
  // Someone who joined since shows up first as an unknown author or typer;
  // refetch then, so their name and presence appear.
  const refreshIfStranger = useCallback((roomId: string, userId: string) => {
    const known = membersRef.current[roomId]?.some((m) => m.user.id === userId) ?? true;
    if (!known && roomId === stateRef.current.activeRoomId) {
      api.members(roomId).then((list) => setMembersByRoom((all) => ({ ...all, [roomId]: list })), () => {});
    }
  }, []);

  const loadRooms = useCallback(async () => {
    const rooms = await api.rooms();
    dispatch({ type: "rooms", rooms });
    const peers = rooms.flatMap((r) => (r.dm_peer ? [r.dm_peer] : []));
    dispatch({ type: "profiles", profiles: [...peers, { id: me.id, handle: me.handle, display_name: me.display_name, avatar_url: me.avatar_url, is_guest: me.is_guest }] });
  }, [me]);

  /** The sync procedure (docs/protocol.md): run after every `hello`. */
  const resync = useCallback(async () => {
    try {
      await loadRooms();
      const cursors = syncCursors(stateRef.current);
      if (cursors.length === 0) return;
      const response = await api.sync(cursors);
      for (const room of response.rooms) {
        if (!room.member) {
          dispatch({ type: "roomLeft", roomId: room.room_id });
        } else if (!room.complete) {
          // Too much to patch: reload the latest page instead.
          const page = await api.history(room.room_id);
          dispatch({ type: "history", roomId: room.room_id, page, replace: true });
          ensureProfiles(page.messages);
        } else {
          dispatch({
            type: "synced",
            roomId: room.room_id,
            messages: room.messages,
            changed: room.changed,
            changeCursor: room.change_cursor,
          });
          ensureProfiles([...room.messages, ...room.changed]);
        }
      }
    } catch {
      // The next hello or resync tries again.
    }
  }, [loadRooms, ensureProfiles]);

  const handleFrame = useCallback(
    (frame: ServerFrame) => {
      switch (frame.type) {
        case "message.created": {
          const viewing =
            frame.message.room_id === stateRef.current.activeRoomId && document.visibilityState === "visible";
          dispatch({ type: "created", message: frame.message, viewing });
          ensureProfiles([frame.message]);
          refreshIfStranger(frame.message.room_id, frame.message.author_id);
          break;
        }
        case "message.updated":
          dispatch({
            type: "change",
            roomId: frame.room_id,
            now: Date.now(),
            change: {
              kind: "updated",
              messageId: frame.message_id,
              content: frame.content,
              editedAt: frame.edited_at,
              version: frame.version,
            },
          });
          break;
        case "message.deleted":
          dispatch({
            type: "change",
            roomId: frame.room_id,
            now: Date.now(),
            change: { kind: "deleted", messageId: frame.message_id, version: frame.version },
          });
          break;
        case "reaction.updated":
          dispatch({
            type: "change",
            roomId: frame.room_id,
            now: Date.now(),
            change: {
              kind: "reaction",
              messageId: frame.message_id,
              emoji: frame.emoji,
              count: frame.count,
              userId: frame.user_id,
              active: frame.active,
              changeId: frame.change_id,
            },
          });
          break;
        case "read.updated":
          // Our own read from another tab, or the other person's in a DM.
          dispatch(
            frame.user_id === me.id
              ? { type: "read", roomId: frame.room_id, messageId: frame.message_id }
              : { type: "peerRead", roomId: frame.room_id, messageId: frame.message_id },
          );
          break;
        case "member.joined":
          setMembersByRoom((all) => {
            const list = all[frame.room_id];
            if (!list || list.some((m) => m.user.id === frame.member.user.id)) return all;
            return { ...all, [frame.room_id]: [...list, frame.member] };
          });
          dispatch({ type: "profiles", profiles: [frame.member.user] });
          break;
        case "member.left":
          setMembersByRoom((all) => {
            const list = all[frame.room_id];
            if (!list) return all;
            return { ...all, [frame.room_id]: list.filter((m) => m.user.id !== frame.user_id) };
          });
          break;
        case "typing":
          dispatch({ type: "typing", roomId: frame.room_id, userId: frame.user_id, now: Date.now() });
          ensureUsers([frame.user_id]);
          refreshIfStranger(frame.room_id, frame.user_id);
          break;
        case "presence":
          dispatch({ type: "presence", userId: frame.user_id, status: frame.status });
          break;
        case "room.joined":
          dispatch({ type: "roomJoined", room: frame.room });
          if (frame.room.dm_peer) dispatch({ type: "profiles", profiles: [frame.room.dm_peer] });
          break;
        case "room.left": {
          const room = stateRef.current.rooms.find((r) => r.id === frame.room_id);
          const name = room?.name ?? "a conversation";
          const notice =
            frame.reason === "removed"
              ? `You were removed from ${name}.`
              : frame.reason === "deleted"
                ? `${name} was deleted.`
                : undefined;
          dispatch({ type: "roomLeft", roomId: frame.room_id, notice });
          break;
        }
        case "resync":
          void resync();
          break;
        default:
          break;
      }
    },
    [ensureProfiles, ensureUsers, refreshIfStranger, resync, me.id],
  );

  // The socket, for the life of the signed-in view.
  useEffect(() => {
    const client = new RealtimeClient({
      url: socketUrl(),
      handlers: {
        state: (connection) => {
          dispatch({ type: "connection", state: connection });
          if (connection === "ended") onSessionEnded();
        },
        hello: () => void resync(),
        acked: (key, message) => dispatch({ type: "acked", key, message }),
        pending: (pending) => dispatch({ type: "pending", pending }),
        frame: handleFrame,
      },
    });
    clientRef.current = client;
    client.start();
    return () => {
      client.stop();
      clientRef.current = null;
    };
    // Registered once per mount; the handlers read state through refs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Accept an invite link (/invite/<code>) once, on load.
  useEffect(() => {
    const code = /^\/invite\/([A-Za-z0-9_-]+)$/.exec(window.location.pathname)?.[1];
    if (!code) return;
    window.history.replaceState(null, "", "/");
    api.acceptInvite(code).then(
      (room) => dispatch({ type: "roomJoined", room, select: true }),
      () => dispatch({ type: "notice", notice: "That invite link is invalid or has expired." }),
    );
  }, []);

  // The latest page of the room being viewed, the first time it's opened.
  const active = state.activeRoomId;
  const activeLoaded = active ? (state.messages[active]?.loaded ?? false) : true;
  useEffect(() => {
    if (!active || activeLoaded) return;
    api.history(active).then(
      (page) => {
        dispatch({ type: "history", roomId: active, page, replace: false });
        ensureProfiles(page.messages);
      },
      () => {},
    );
  }, [active, activeLoaded, ensureProfiles]);

  // Members of the room being viewed, fetched when it's opened.
  useEffect(() => {
    if (!active) return;
    api.members(active).then(
      (list) => {
        setMembersByRoom((all) => ({ ...all, [active]: list }));
        dispatch({ type: "profiles", profiles: list.map((m) => m.user) });
      },
      () => {},
    );
  }, [active]);
  const members = useMemo(() => (active ? (membersByRoom[active] ?? []) : []), [active, membersByRoom]);

  // Presence for DM partners and the visible member list.
  const watched = useMemo(() => {
    const ids = new Set<string>();
    for (const r of state.rooms) if (r.dm_peer) ids.add(r.dm_peer.id);
    for (const m of members) ids.add(m.user.id);
    ids.delete(me.id);
    return [...ids].sort();
  }, [state.rooms, members, me.id]);
  useEffect(() => {
    clientRef.current?.watch(watched);
  }, [watched]);

  // Typing indicators fade out on their own.
  useEffect(() => {
    const timer = setInterval(() => dispatch({ type: "expireTyping", now: Date.now() }), 1000);
    return () => clearInterval(timer);
  }, []);

  // Away after 5 minutes without input, or while the tab is hidden.
  useEffect(() => {
    let lastInput = Date.now();
    let idle = false;
    const report = () => {
      const nowIdle = document.hidden || Date.now() - lastInput > IDLE_AFTER_MS;
      if (nowIdle !== idle) {
        idle = nowIdle;
        clientRef.current?.setPresence(idle ? "idle" : "active");
      }
    };
    const onInput = () => {
      lastInput = Date.now();
      report();
    };
    const events = ["keydown", "pointerdown", "pointermove"] as const;
    for (const e of events) window.addEventListener(e, onInput, { passive: true });
    document.addEventListener("visibilitychange", report);
    const timer = setInterval(report, 30_000);
    return () => {
      for (const e of events) window.removeEventListener(e, onInput);
      document.removeEventListener("visibilitychange", report);
      clearInterval(timer);
    };
  }, []);

  const actions = useMemo(
    () => ({
      select: (roomId: string) => dispatch({ type: "select", roomId }),
      send: (roomId: string, content: string) => {
        lastTyping.current[roomId] = 0;
        clientRef.current?.send(roomId, content);
      },
      retry: (key: string) => clientRef.current?.retry(key),
      discard: (key: string) => {
        clientRef.current?.discard(key);
        dispatch({ type: "discard", key });
      },
      typing: (roomId: string) => {
        const now = Date.now();
        if (now - (lastTyping.current[roomId] ?? 0) < TYPING_EVERY_MS) return;
        lastTyping.current[roomId] = now;
        clientRef.current?.typing(roomId);
      },
      loadOlder: async (roomId: string) => {
        const cursor = stateRef.current.messages[roomId]?.olderCursor;
        if (!cursor) return;
        const page = await api.history(roomId, cursor);
        dispatch({ type: "history", roomId, page, replace: false });
        ensureProfiles(page.messages);
      },
      joined: (room: RoomInfo) => dispatch({ type: "roomJoined", room, select: true }),
      left: (roomId: string) => dispatch({ type: "roomLeft", roomId }),
      dismissNotice: () => dispatch({ type: "notice", notice: null }),
      refreshMembers: async (roomId: string) => {
        const list = await api.members(roomId);
        setMembersByRoom((all) => ({ ...all, [roomId]: list }));
      },
      edit: (roomId: string, messageId: string, content: string): Promise<CallResult> =>
        clientRef.current?.editMessage(roomId, messageId, content) ?? offline(),
      remove: (roomId: string, messageId: string): Promise<CallResult> =>
        clientRef.current?.deleteMessage(roomId, messageId) ?? offline(),
      react: (roomId: string, messageId: string, emoji: string, active: boolean): Promise<CallResult> =>
        clientRef.current?.setReaction(roomId, messageId, emoji, active) ?? offline(),
      /**
       * The reader has seen up to `messageId`: clears the badge now, and tells
       * the server at most every 2 s per room, always ending on the newest.
       */
      markRead: (roomId: string, messageId: string) => {
        dispatch({ type: "read", roomId, messageId });
        const marks = readMarks.current;
        const entry = marks[roomId] ?? { last: 0, latest: messageId, timer: null };
        marks[roomId] = entry;
        if (compareIds(messageId, entry.latest) > 0) entry.latest = messageId;
        if (entry.timer) return;
        const flush = () => {
          entry.timer = null;
          entry.last = Date.now();
          clientRef.current?.markRead(roomId, entry.latest);
        };
        const wait = entry.last + READ_MARK_EVERY_MS - Date.now();
        if (wait <= 0) flush();
        else entry.timer = setTimeout(flush, wait);
      },
      receipts: (roomId: string, messageId: string) => api.receipts(roomId, messageId),
      search: (q: string, before?: string) => api.search(q, { before }),
      knowAuthors: (messages: Message[]) => ensureProfiles(messages),
    }),
    [ensureProfiles],
  );

  // Pending read marks go out when the view goes away.
  useEffect(() => {
    const marks = readMarks.current;
    return () => {
      for (const [roomId, entry] of Object.entries(marks)) {
        if (entry.timer) {
          clearTimeout(entry.timer);
          clientRef.current?.markRead(roomId, entry.latest);
        }
      }
    };
  }, []);

  return { state, members, actions };
}
