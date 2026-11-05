// Typed wrappers around the REST API. Request and response types are
// generated from the Rust protocol crate, so a server change that breaks the
// client fails `tsc`.

import type { ApiError } from "./protocol/generated/ApiError";
import type { AuthConfig } from "./protocol/generated/AuthConfig";
import type { HistoryPage } from "./protocol/generated/HistoryPage";
import type { Invite } from "./protocol/generated/Invite";
import type { LoginRequest } from "./protocol/generated/LoginRequest";
import type { Me } from "./protocol/generated/Me";
import type { Member } from "./protocol/generated/Member";
import type { PublicRoom } from "./protocol/generated/PublicRoom";
import type { Receipts } from "./protocol/generated/Receipts";
import type { RoomInfo } from "./protocol/generated/RoomInfo";
import type { SearchResults } from "./protocol/generated/SearchResults";
import type { SessionInfo } from "./protocol/generated/SessionInfo";
import type { SignupRequest } from "./protocol/generated/SignupRequest";
import type { SyncCursor } from "./protocol/generated/SyncCursor";
import type { SyncResponse } from "./protocol/generated/SyncResponse";
import type { UserProfile } from "./protocol/generated/UserProfile";

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

  rooms: () => request<RoomInfo[]>("GET", "/rooms"),
  publicRooms: () => request<PublicRoom[]>("GET", "/rooms/public"),
  createRoom: (name: string, kind: "public" | "private") => request<RoomInfo>("POST", "/rooms", { name, kind }),
  joinRoom: (id: string) => request<RoomInfo>("POST", `/rooms/${id}/join`),
  leaveRoom: (id: string) => request<undefined>("POST", `/rooms/${id}/leave`),
  deleteRoom: (id: string) => request<undefined>("DELETE", `/rooms/${id}`),
  members: (id: string) => request<Member[]>("GET", `/rooms/${id}/members`),
  removeMember: (roomId: string, userId: string) => request<undefined>("DELETE", `/rooms/${roomId}/members/${userId}`),
  createInvite: (roomId: string) => request<Invite>("POST", `/rooms/${roomId}/invites`),
  acceptInvite: (code: string) => request<RoomInfo>("POST", `/invites/${encodeURIComponent(code)}/accept`),
  openDm: (handle: string) => request<RoomInfo>("POST", "/dms", { handle }),
  profiles: (ids: string[]) => request<UserProfile[]>("GET", `/users?ids=${ids.map(encodeURIComponent).join(",")}`),
  history: (roomId: string, before?: string) =>
    request<HistoryPage>("GET", `/rooms/${roomId}/messages${before ? `?before=${before}` : ""}`),
  sync: (rooms: SyncCursor[]) => request<SyncResponse>("POST", "/sync", { rooms }),
  receipts: (roomId: string, messageId: string) =>
    request<Receipts>("GET", `/rooms/${roomId}/receipts?message_id=${messageId}`),
  search: (q: string, options: { roomId?: string; before?: string } = {}) => {
    const params = new URLSearchParams({ q });
    if (options.roomId) params.set("room_id", options.roomId);
    if (options.before) params.set("before", options.before);
    return request<SearchResults>("GET", `/search?${params.toString()}`);
  },
};

/** Where the browser goes to start GitHub sign-in (a full-page redirect). */
export const GITHUB_START_URL = "/api/auth/github/start";
