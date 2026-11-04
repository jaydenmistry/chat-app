import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SignIn } from "./SignIn";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

/** Answers each request URL with a canned response. */
function routes(table: Record<string, [number, unknown]>) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      const [status, body] = table[url] ?? [404, { code: "not_found", message: "not found" }];
      return new Response(JSON.stringify(body), { status });
    }),
  );
}

describe("SignIn", () => {
  it("shows a field error from the server next to that field", async () => {
    routes({
      "/api/auth/config": [200, { github: false }],
      "/api/auth/signup": [409, { code: "conflict", message: "this handle is taken", field: "handle" }],
    });
    render(<SignIn onSignedIn={() => {}} />);
    fireEvent.click(screen.getByRole("button", { name: "Create one" }));
    fireEvent.click(screen.getByRole("button", { name: "Create account" }));

    const message = await screen.findByText("this handle is taken");
    const handle = screen.getByLabelText("Handle");
    expect(handle.getAttribute("aria-invalid")).toBe("true");
    expect(handle.getAttribute("aria-describedby")).toBe(message.id);
  });

  it("offers GitHub only when the server has it configured", async () => {
    routes({ "/api/auth/config": [200, { github: true }] });
    render(<SignIn onSignedIn={() => {}} />);
    const link = await screen.findByRole("link", { name: "Sign in with GitHub" });
    expect(link.getAttribute("href")).toBe("/api/auth/github/start");
  });

  it("signs in as a guest", async () => {
    const guest = { id: "g", handle: "guest_abc123", display_name: "Guest abc123", avatar_url: null, is_guest: true, expires_at: "2026-11-05T12:00:00Z" };
    routes({ "/api/auth/config": [200, { github: false }], "/api/auth/guest": [201, guest] });
    const onSignedIn = vi.fn();
    render(<SignIn onSignedIn={onSignedIn} />);
    fireEvent.click(screen.getByRole("button", { name: "Try as guest" }));
    await waitFor(() => expect(onSignedIn).toHaveBeenCalledWith(guest));
  });
});
