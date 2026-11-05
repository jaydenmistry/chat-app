import { describe, expect, it } from "vitest";
import { must } from "../test/must";
import type { Message } from "../protocol/generated/Message";
import type { RoomInfo } from "../protocol/generated/RoomInfo";
import { initialState, reducer, syncCursors, TYPING_SHOWN_MS, typers } from "./state";

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

const withRooms = reducer(initialState, { type: "rooms", rooms: [room("r2", "beta"), room("r1", "alpha")] });

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
    expect(syncCursors(s)).toEqual([{ room_id: "r1", after: "12" }]);
  });

  it("a replacing page drops what it doesn't contain", () => {
    let s = reducer(withRooms, { type: "history", roomId: "r1", page: { messages: [msg("1"), msg("2")], next_before: null, change_cursor: "0" }, replace: false });
    s = reducer(s, { type: "history", roomId: "r1", page: { messages: [msg("900")], next_before: "899", change_cursor: "0" }, replace: true });
    expect(s.messages.r1).toEqual({ items: [msg("900")], olderCursor: "899", loaded: true });
  });
});
