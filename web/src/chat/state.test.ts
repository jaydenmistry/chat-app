import { describe, expect, it } from "vitest";
import { must } from "../test/must";
import type { Message } from "../protocol/generated/Message";
import type { RoomInfo } from "../protocol/generated/RoomInfo";
import { type Action, type ChatState, EARLY_UPDATE_MS, initialState, myLatest, reducer, syncCursors, TYPING_SHOWN_MS, typers, unreadLabel } from "./state";

function room(id: string, name: string): RoomInfo {
  return { id, kind: "public", name, role: "member", dm_peer: null, created_at: "2026-11-05T00:00:00Z", last_read_id: "0", unread: 0, unread_capped: false };
}

function msg(id: string, roomId = "r1", author = "bob"): Message {
  return {
    id,
    room_id: roomId,
    author_id: author,
    content: `m${id}`,
    created_at: "2026-11-05T00:00:00Z",
    edited_at: null,
    deleted: false,
    version: 1,
    reactions: [],
  };
}

const withRooms = reducer(initialState("me"), { type: "rooms", rooms: [room("r2", "beta"), room("r1", "alpha")] });

describe("chat state", () => {
  it("sorts rooms and selects the first", () => {
    expect(withRooms.rooms.map((r) => r.name)).toEqual(["alpha", "beta"]);
    expect(withRooms.activeRoomId).toBe("r1");
  });

  it("replaces a pending send with the acked message", () => {
    let s = reducer(withRooms, { type: "pending", pending: { key: "k", roomId: "r1", content: "hi", status: "sending" } });
    expect(s.pending).toHaveLength(1);
    s = reducer(s, { type: "acked", key: "k", message: msg("5", "r1", "me") });
    expect(s.pending).toHaveLength(0);
    expect(must(s.messages.r1).items.map((m) => m.id)).toEqual(["5"]);
    // The fan-out copy of the same message changes nothing.
    expect(reducer(s, { type: "messages", roomId: "r1", messages: [msg("5", "r1", "me")] }).messages.r1?.items).toHaveLength(1);
  });

  it("forgets a room on room.left and moves the selection", () => {
    let s = reducer(withRooms, { type: "messages", roomId: "r1", messages: [msg("1")] });
    s = reducer(s, { type: "roomLeft", roomId: "r1", notice: "You were removed from alpha" });
    expect(s.rooms.map((r) => r.id)).toEqual(["r2"]);
    expect(s.messages.r1).toBeUndefined();
    expect(s.activeRoomId).toBe("r2");
    expect(s.notice).toBe("You were removed from alpha");
  });

  it("drops an expired guest's messages but keeps everyone else's", () => {
    let s = reducer(withRooms, {
      type: "messages",
      roomId: "r1",
      messages: [msg("1", "r1", "bob"), msg("2", "r1", "me"), msg("3", "r1", "bob")],
    });
    s = reducer(s, { type: "userExpired", roomId: "r1", userId: "bob" });
    expect(must(s.messages.r1).items.map((m) => m.id)).toEqual(["2"]);
    // Nothing to drop in a room the guest wasn't in: no change.
    expect(reducer(s, { type: "userExpired", roomId: "r2", userId: "bob" })).toBe(s);
  });

  it("shows typing for a few seconds, and stops it when they send", () => {
    let s = reducer(withRooms, { type: "typing", roomId: "r1", userId: "bob", now: 1000 });
    expect(typers(s, "r1", "me", 1000)).toEqual(["bob"]);
    expect(typers(s, "r1", "bob", 1000)).toEqual([]);
    expect(typers(s, "r1", "me", 1000 + TYPING_SHOWN_MS)).toEqual([]);
    s = reducer(s, { type: "messages", roomId: "r1", messages: [msg("9", "r1", "bob")] });
    expect(typers(s, "r1", "me", 1001)).toEqual([]);
  });

  it("syncs only loaded rooms, from their newest message", () => {
    let s = reducer(withRooms, { type: "history", roomId: "r1", page: { messages: [msg("3"), msg("12")], next_before: null, change_cursor: "0" }, replace: false });
    s = reducer(s, { type: "messages", roomId: "r2", messages: [msg("40", "r2")] });
    expect(syncCursors(s)).toEqual([{ room_id: "r1", after: "12", changes_after: "0" }]);
  });

  it("a replacing page drops what it doesn't contain", () => {
    let s = reducer(withRooms, { type: "history", roomId: "r1", page: { messages: [msg("1"), msg("2")], next_before: null, change_cursor: "0" }, replace: false });
    s = reducer(s, { type: "history", roomId: "r1", page: { messages: [msg("900")], next_before: "899", change_cursor: "0" }, replace: true });
    expect(s.messages.r1).toEqual({ items: [msg("900")], olderCursor: "899", loaded: true, changeCursor: "0" });
  });
});

function page(messages: Message[], changeCursor: string): Action {
  return { type: "history", roomId: "r1", page: { messages, next_before: null, change_cursor: changeCursor }, replace: false };
}

const items = (s: ChatState, roomId = "r1") => s.messages[roomId]?.items ?? [];

function change(c: Extract<Action, { type: "change" }>["change"], now = 1000): Action {
  return { type: "change", roomId: "r1", change: c, now };
}

const reaction = (messageId: string, emoji: string, count: number, userId: string, active: boolean, changeId: string) =>
  change({ kind: "reaction", messageId, emoji, count, userId, active, changeId });

describe("room list", () => {
  it("a stale list doesn't undo a join made while it was loading", () => {
    const since = withRooms.joinSeq;
    let s = reducer(withRooms, { type: "roomJoined", room: room("r3", "gamma"), select: true });
    s = reducer(s, { type: "rooms", rooms: [room("r1", "alpha"), room("r2", "beta")], since });
    expect(s.rooms.map((r) => r.id)).toEqual(["r1", "r2", "r3"]);
    expect(s.activeRoomId).toBe("r3");
    // A list requested after the join is the truth: r3 is gone.
    s = reducer(s, { type: "rooms", rooms: [room("r1", "alpha")], since: s.joinSeq });
    expect(s.rooms.map((r) => r.id)).toEqual(["r1"]);
  });
});

describe("edits and deletes", () => {
  const loaded = reducer(withRooms, page([msg("5")], "100"));

  it("applies an edit only if its version is newer", () => {
    let s = reducer(loaded, change({ kind: "updated", messageId: "5", content: "edited", editedAt: "t", version: 3 }));
    expect(must(items(s)[0]).content).toBe("edited");
    s = reducer(s, change({ kind: "updated", messageId: "5", content: "older", editedAt: "t", version: 2 }));
    expect(must(items(s)[0]).content).toBe("edited");
  });

  it("is final once deleted", () => {
    let s = reducer(loaded, change({ kind: "deleted", messageId: "5", version: 3 }));
    expect(items(s)[0]).toMatchObject({ deleted: true, content: "" });
    s = reducer(s, change({ kind: "updated", messageId: "5", content: "back", editedAt: "t", version: 9 }));
    s = reducer(s, { type: "messages", roomId: "r1", messages: [{ ...msg("5"), version: 9 }] });
    expect(items(s)[0]).toMatchObject({ deleted: true, content: "" });
  });

  it("holds an update that beats its message, and applies it on arrival", () => {
    let s = reducer(loaded, change({ kind: "updated", messageId: "8", content: "edited first", editedAt: "t", version: 5 }));
    expect(items(s).map((m) => m.id)).toEqual(["5"]);
    s = reducer(s, { type: "created", message: msg("8"), viewing: true });
    expect(must(items(s)[1]).content).toBe("edited first");
    expect(s.early).toEqual([]);
  });

  it("drops held updates after a minute", () => {
    let s = reducer(loaded, change({ kind: "deleted", messageId: "8", version: 5 }, 1000));
    s = reducer(s, { type: "expireTyping", now: 1000 + EARLY_UPDATE_MS });
    expect(s.early).toEqual([]);
    s = reducer(s, { type: "created", message: msg("8"), viewing: true });
    expect(must(items(s)[1]).deleted).toBe(false);
  });
});

describe("reactions", () => {
  const loaded = reducer(withRooms, page([msg("5")], "100"));

  it("takes counts from the server and `me` from my own changes", () => {
    let s = reducer(loaded, reaction("5", "🎉", 1, "bob", true, "200"));
    s = reducer(s, reaction("5", "👍", 2, "me", true, "201"));
    expect(must(items(s)[0]).reactions).toEqual([
      { emoji: "👍", count: 2, me: true },
      { emoji: "🎉", count: 1, me: false },
    ]);
    s = reducer(s, reaction("5", "👍", 1, "me", false, "202"));
    s = reducer(s, reaction("5", "🎉", 0, "bob", false, "203"));
    expect(must(items(s)[0]).reactions).toEqual([{ emoji: "👍", count: 1, me: false }]);
  });

  it("ignores a reaction frame older than one already applied", () => {
    let s = reducer(loaded, reaction("5", "👍", 2, "bob", true, "300"));
    s = reducer(s, reaction("5", "👍", 1, "bob", false, "299"));
    expect(must(items(s)[0]).reactions).toEqual([{ emoji: "👍", count: 2, me: false }]);
  });

  it("takes a fresher copy of the same version from a sync", () => {
    const s = reducer(loaded, {
      type: "synced",
      roomId: "r1",
      messages: [],
      changed: [{ ...msg("5"), reactions: [{ emoji: "🔥", count: 4, me: true }] }],
      changeCursor: "400",
    });
    expect(must(items(s)[0]).reactions).toEqual([{ emoji: "🔥", count: 4, me: true }]);
    expect(syncCursors(s)).toEqual([{ room_id: "r1", after: "5", changes_after: "400" }]);
  });
});

describe("change cursors", () => {
  it("keep the earlier cursor when an older page loads", () => {
    let s = reducer(withRooms, page([msg("50")], "500"));
    s = reducer(s, page([msg("10")], "900"));
    expect(s.messages.r1?.changeCursor).toBe("500");
  });
});

describe("unread and read state", () => {
  const loaded = reducer(withRooms, page([msg("5")], "100"));
  const unread = (s: ChatState, id = "r1") => {
    const r = must(s.rooms.find((x) => x.id === id));
    return [r.unread, r.unread_capped, unreadLabel(r)];
  };

  it("counts others' new messages in rooms I'm not viewing", () => {
    let s = reducer(loaded, { type: "created", message: msg("6"), viewing: false });
    s = reducer(s, { type: "created", message: msg("7", "r1", "me"), viewing: false });
    s = reducer(s, { type: "created", message: msg("8"), viewing: true });
    // The same message twice (a sync overlap) counts once.
    s = reducer(s, { type: "created", message: msg("6"), viewing: false });
    expect(unread(s)).toEqual([1, false, "1"]);
  });

  it("caps at 99+", () => {
    let s = loaded;
    for (let i = 10; i < 120; i++) s = reducer(s, { type: "created", message: msg(String(i)), viewing: false });
    expect(unread(s)).toEqual([99, true, "99+"]);
  });

  it("recounts from what is loaded when the read position moves, and only forward", () => {
    let s = loaded;
    for (const id of ["6", "7", "8"]) s = reducer(s, { type: "created", message: msg(id), viewing: false });
    s = reducer(s, { type: "read", roomId: "r1", messageId: "7" });
    expect(unread(s)).toEqual([1, false, "1"]);
    s = reducer(s, { type: "read", roomId: "r1", messageId: "6" });
    expect(must(s.rooms.find((r) => r.id === "r1")).last_read_id).toBe("7");
  });

  it("tracks the other person's reads, forward only", () => {
    let s = reducer(loaded, { type: "peerRead", roomId: "r1", messageId: "9" });
    s = reducer(s, { type: "peerRead", roomId: "r1", messageId: "8" });
    expect(s.peerRead.r1).toBe("9");
  });

  it("finds my latest message that is still shown", () => {
    let s = reducer(loaded, { type: "messages", roomId: "r1", messages: [msg("6", "r1", "me"), msg("7", "r1", "me")] });
    s = reducer(s, change({ kind: "deleted", messageId: "7", version: 2 }));
    expect(myLatest(s, "r1")?.id).toBe("6");
  });
});
