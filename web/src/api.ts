// Typed wrappers around the REST API. Request and response types are
// generated from the Rust protocol crate, so a server change that breaks the
// client fails `tsc`.

import type { ApiError } from "./protocol/generated/ApiError";
import type { AuthConfig } from "./protocol/generated/AuthConfig";
import type { LoginRequest } from "./protocol/generated/LoginRequest";
import type { Me } from "./protocol/generated/Me";
import type { SessionInfo } from "./protocol/generated/SessionInfo";
import type { SignupRequest } from "./protocol/generated/SignupRequest";

/** A non-2xx response, carrying the server's error body. */
export class ApiRequestError extends Error {
  readonly status: number;
  readonly body: ApiError;

  constructor(status: number, body: ApiError) {
    super(body.message);
    this.status = status;
    this.body = body;
  }
}

function isApiError(value: unknown): value is ApiError {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { code?: unknown }).code === "string" &&
    typeof (value as { message?: unknown }).message === "string"
  );
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`/api${path}`, {
      method,
      // Same-origin requests send the session cookie; the browser adds the
      // Origin header the server checks on POST and DELETE.
      credentials: "same-origin",
      headers: body === undefined ? {} : { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch {
    throw new ApiRequestError(0, { code: "unavailable", message: "can't reach the server" });
  }
  if (response.status === 204) {
    return undefined as T;
  }
  const data: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    throw new ApiRequestError(
      response.status,
      isApiError(data) ? data : { code: "internal", message: `unexpected response (${response.status})` },
    );
  }
  return data as T;
}

export const api = {
  authConfig: () => request<AuthConfig>("GET", "/auth/config"),
  me: () => request<Me>("GET", "/me"),
  signup: (body: SignupRequest) => request<Me>("POST", "/auth/signup", body),
  login: (body: LoginRequest) => request<Me>("POST", "/auth/login", body),
  guest: () => request<Me>("POST", "/auth/guest"),
  logout: () => request<undefined>("POST", "/auth/logout"),
  sessions: () => request<SessionInfo[]>("GET", "/sessions"),
  revokeSession: (id: string) => request<undefined>("DELETE", `/sessions/${encodeURIComponent(id)}`),
};

/** Where the browser goes to start GitHub sign-in (a full-page redirect). */
export const GITHUB_START_URL = "/api/auth/github/start";
