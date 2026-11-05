import { describe, expect, it } from "vitest";
import { must } from "../test/must";
import type { Message } from "../protocol/generated/Message";
import { compareIds, lastId, mergeMessages, newerCopy } from "./messages";

function msg(id: string, content = `m${id}`, version = 1, deleted = false): Message {
  return {
    id,
    room_id: "r",
    author_id: "a",
    content,
    created_at: "2026-11-05T00:00:00Z",
    edited_at: null,
    deleted,
    version,
  };
}

describe("compareIds", () => {
  it("orders by numeric value, not text", () => {
    expect(compareIds("9", "10")).toBeLessThan(0);
    expect(compareIds("100", "99")).toBeGreaterThan(0);
    expect(compareIds("123", "123")).toBe(0);
    // Values past 2^53, where Number would lose precision.
    expect(compareIds("9007199254740993", "9007199254740992")).toBeGreaterThan(0);
    expect(compareIds("9223372036854775807", "9223372036854775806")).toBeGreaterThan(0);
  });
});

describe("mergeMessages", () => {
  it("deduplicates by id", () => {
    const merged = mergeMessages([msg("1"), msg("2")], [msg("2"), msg("3"), msg("3")]);
    expect(merged.map((m) => m.id)).toEqual(["1", "2", "3"]);
  });

  it("puts a late message with a smaller id in its place", () => {
    // Design 16.1: an instance with a slower clock commits after a message
    // with a bigger ID has been shown.
    const merged = mergeMessages([msg("10"), msg("30")], [msg("20")]);
    expect(merged.map((m) => m.id)).toEqual(["10", "20", "30"]);
  });

  it("keeps the higher version of the same message", () => {
    const edited = msg("5", "edited", 2);
    expect(mergeMessages([msg("5", "original", 1)], [edited])[0]).toBe(edited);
    // An older copy arriving later (a sync overlap) doesn't undo the edit.
    expect(mergeMessages([edited], [msg("5", "original", 1)])[0]).toBe(edited);
  });

  it("never undeletes", () => {
    const deleted = msg("7", "", 2, true);
    expect(newerCopy(deleted, msg("7", "back?", 9))).toBe(deleted);
    expect(must(mergeMessages([deleted], [msg("7", "back?", 9)])[0]).deleted).toBe(true);
  });

  it("returns the same array when there is nothing new", () => {
    const existing = [msg("1")];
    expect(mergeMessages(existing, [])).toBe(existing);
  });

  it("finds the last id", () => {
    expect(lastId([])).toBeNull();
    expect(lastId(mergeMessages([], [msg("12"), msg("3")]))).toBe("12");
  });
});
