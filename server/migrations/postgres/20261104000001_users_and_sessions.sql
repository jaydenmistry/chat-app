-- Accounts and session metadata. The hot copy of each session lives in Redis;
-- this table is the durable record used to list and revoke sessions.

CREATE TABLE users (
    id            uuid PRIMARY KEY,
    handle        text NOT NULL,
    display_name  text NOT NULL,
    email         text,
    password_hash text,
    github_id     bigint,
    avatar_url    text,
    is_guest      boolean NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now(),
    expires_at    timestamptz,
    banned_at     timestamptz,

    -- Handles and emails are stored normalized (lowercase), so plain unique
    -- indexes give case-insensitive uniqueness.
    CONSTRAINT users_handle_format CHECK (handle ~ '^[a-z0-9_]{3,32}$'),
    CONSTRAINT users_email_lowercase CHECK (email = lower(email)),
    CONSTRAINT users_display_name_length CHECK (char_length(display_name) BETWEEN 1 AND 64),
    -- Guests, and only guests, expire.
    CONSTRAINT users_guest_expiry CHECK (is_guest = (expires_at IS NOT NULL)),
    -- Guests have no way to sign in again.
    CONSTRAINT users_guest_no_login CHECK (
        NOT is_guest OR (email IS NULL AND password_hash IS NULL AND github_id IS NULL)
    ),
    CONSTRAINT users_password_needs_email CHECK (password_hash IS NULL OR email IS NOT NULL)
);

CREATE UNIQUE INDEX users_handle_key ON users (handle);
CREATE UNIQUE INDEX users_email_key ON users (email);
CREATE UNIQUE INDEX users_github_id_key ON users (github_id);

CREATE TABLE sessions (
    id           uuid PRIMARY KEY,
    user_id      uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    -- SHA-256 of the cookie token. The token itself is never stored.
    token_hash   bytea NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    user_agent   text,
    ip           inet,
    revoked_at   timestamptz,

    CONSTRAINT sessions_token_hash_length CHECK (octet_length(token_hash) = 32)
);

CREATE UNIQUE INDEX sessions_token_hash_key ON sessions (token_hash);
CREATE INDEX sessions_active_by_user ON sessions (user_id) WHERE revoked_at IS NULL;
