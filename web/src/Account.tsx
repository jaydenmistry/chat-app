import { useCallback, useEffect, useState } from "react";
import { api } from "./api";
import type { Me } from "./protocol/generated/Me";
import type { SessionInfo } from "./protocol/generated/SessionInfo";

/**
 * The signed-in view for Phase 1: who you are and where you're signed in.
 * The chat UI replaces most of this in Phase 2.
 */
export function Account({ me, onSignedOut }: { me: Me; onSignedOut: () => void }) {
  const [sessions, setSessions] = useState<SessionInfo[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadSessions = useCallback(() => {
    api.sessions().then(setSessions, (err: unknown) => setError(String(err)));
  }, []);
  useEffect(loadSessions, [loadSessions]);

  async function signOut() {
    try {
      await api.logout();
      onSignedOut();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function revoke(session: SessionInfo) {
    try {
      await api.revokeSession(session.id);
      if (session.current) {
        onSignedOut();
      } else {
        loadSessions();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <main className="centered">
      <section className="card" aria-labelledby="account-title">
        <h1 id="account-title">{me.display_name}</h1>
        <p className="handle">@{me.handle}</p>
        {me.is_guest && me.expires_at && (
          <p className="hint">This guest account and its messages are deleted at {formatTime(me.expires_at)}.</p>
        )}
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}

        <h2>Where you're signed in</h2>
        {sessions === null ? (
          <p>Loading…</p>
        ) : (
          <ul className="sessions">
            {sessions.map((session) => (
              <li key={session.id}>
                <div>
                  <strong>{describeAgent(session.user_agent)}</strong>
                  {session.current && <span className="badge">this device</span>}
                  <div className="hint">
                    {session.ip ?? "unknown address"} · last active {formatTime(session.last_seen_at)}
                  </div>
                </div>
                <button type="button" className="secondary" onClick={() => void revoke(session)}>
                  {session.current ? "Sign out" : "Revoke"}
                </button>
              </li>
            ))}
          </ul>
        )}

        <button type="button" onClick={() => void signOut()}>
          Sign out
        </button>
      </section>
    </main>
  );
}

function formatTime(iso: string): string {
  return new Date(iso).toLocaleString();
}

/** A short label for a user agent string. Good enough for a session list. */
export function describeAgent(userAgent: string | null): string {
  if (!userAgent) return "Unknown device";
  const browser = /Firefox\//.test(userAgent)
    ? "Firefox"
    : /Edg\//.test(userAgent)
      ? "Edge"
      : /Chrome\//.test(userAgent)
        ? "Chrome"
        : /Safari\//.test(userAgent)
          ? "Safari"
          : "Browser";
  const os = /Windows/.test(userAgent)
    ? "Windows"
    : /Mac OS X/.test(userAgent)
      ? "macOS"
      : /Android/.test(userAgent)
        ? "Android"
        : /iPhone|iPad/.test(userAgent)
          ? "iOS"
          : /Linux/.test(userAgent)
            ? "Linux"
            : "unknown OS";
  return `${browser} on ${os}`;
}
