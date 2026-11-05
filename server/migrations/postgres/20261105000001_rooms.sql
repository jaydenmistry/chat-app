-- Rooms, memberships and invites. Messages live in Scylla; this is the
-- relational side: who can see which room, enforced by constraints.

CREATE TABLE rooms (
    id                uuid PRIMARY KEY,
    kind              text NOT NULL,
    -- Null for DMs, which are shown under the other member's name.
    name              text,
    created_by        uuid REFERENCES users (id) ON DELETE SET NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    is_demo           boolean NOT NULL DEFAULT false,
    -- '<smaller user id>:<larger user id>' for DMs, so the unique index makes
    -- "one DM per pair of users" a database guarantee.
    dm_key            text,
    -- When set, every message written to the room expires then (DMs with a
    -- guest; demo rooms between resets). docs/design.md 11.3.
    message_expiry_at timestamptz,
    -- History and search never go below this message ID (demo resets).
    history_floor     bigint NOT NULL DEFAULT 0,

    CONSTRAINT rooms_kind_valid CHECK (kind IN ('public', 'private', 'dm')),
    CONSTRAINT rooms_dm_key_iff_dm CHECK ((kind = 'dm') = (dm_key IS NOT NULL)),
    CONSTRAINT rooms_name_iff_not_dm CHECK ((kind = 'dm') = (name IS NULL)),
    CONSTRAINT rooms_name_length CHECK (name IS NULL OR char_length(name) BETWEEN 1 AND 64)
);

CREATE UNIQUE INDEX rooms_dm_key_key ON rooms (dm_key);
CREATE INDEX rooms_public ON rooms (created_at) WHERE kind = 'public';

CREATE TABLE memberships (
    room_id      uuid NOT NULL REFERENCES rooms (id) ON DELETE CASCADE,
    user_id      uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    role         text NOT NULL,
    joined_at    timestamptz NOT NULL DEFAULT now(),
    last_read_id bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (room_id, user_id),

    CONSTRAINT memberships_role_valid CHECK (role IN ('owner', 'member'))
);

-- "My rooms".
CREATE INDEX memberships_by_user ON memberships (user_id);
-- A room has at most one owner.
CREATE UNIQUE INDEX memberships_one_owner ON memberships (room_id) WHERE role = 'owner';

CREATE TABLE invites (
    -- 128 random bits, base64url.
    code       text PRIMARY KEY,
    room_id    uuid NOT NULL REFERENCES rooms (id) ON DELETE CASCADE,
    created_by uuid REFERENCES users (id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    max_uses   integer NOT NULL,
    uses       integer NOT NULL DEFAULT 0,

    CONSTRAINT invites_max_uses_positive CHECK (max_uses > 0),
    CONSTRAINT invites_uses_bounded CHECK (uses BETWEEN 0 AND max_uses)
);
