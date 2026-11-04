import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiRequestError, api } from "./api";

function respond(status: number, body?: unknown) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(body === undefined ? null : JSON.stringify(body), { status })),
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("api", () => {
  it("returns the parsed body on success", async () => {
    respond(200, { github: true });
    await expect(api.authConfig()).resolves.toEqual({ github: true });
  });

  it("sends JSON with the session cookie", async () => {
    respond(200, { id: "1" });
    await api.login({ email: "a@example.com", password: "pw" });
    expect(fetch).toHaveBeenCalledTimes(1);
    const [url, init] = vi.mocked(fetch).mock.calls[0] ?? [];
    expect(url).toBe("/api/auth/login");
    expect(init).toMatchObject({
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: "a@example.com", password: "pw" }),
    });
  });

  it("returns undefined for 204", async () => {
    respond(204);
    await expect(api.logout()).resolves.toBeUndefined();
  });

  it("throws the server's error body", async () => {
    respond(409, { code: "conflict", message: "this handle is taken", field: "handle" });
    const error = await api.guest().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ApiRequestError);
    expect((error as ApiRequestError).status).toBe(409);
    expect((error as ApiRequestError).body).toEqual({
      code: "conflict",
      message: "this handle is taken",
      field: "handle",
    });
  });

  it("wraps a body that isn't an ApiError", async () => {
    respond(502, "Bad Gateway");
    const error = (await api.me().catch((e: unknown) => e)) as ApiRequestError;
    expect(error.status).toBe(502);
    expect(error.body.code).toBe("internal");
  });

  it("reports a network failure as unavailable", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Promise.reject(new TypeError("Failed to fetch"))));
    const error = (await api.me().catch((e: unknown) => e)) as ApiRequestError;
    expect(error.status).toBe(0);
    expect(error.body.code).toBe("unavailable");
  });
});
