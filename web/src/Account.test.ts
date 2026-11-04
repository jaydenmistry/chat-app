import { describe, expect, it } from "vitest";
import { describeAgent } from "./Account";

describe("describeAgent", () => {
  it.each([
    ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36", "Chrome on Windows"],
    ["Mozilla/5.0 (Macintosh; Intel Mac OS X 15_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/19.0 Safari/605.1.15", "Safari on macOS"],
    ["Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0", "Firefox on Linux"],
    ["Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36 Edg/140.0", "Edge on Windows"],
    [null, "Unknown device"],
  ])("%s", (agent, label) => {
    expect(describeAgent(agent)).toBe(label);
  });
});
