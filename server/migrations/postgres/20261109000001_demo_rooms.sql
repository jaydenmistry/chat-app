-- The system user and the seeded demo rooms (docs/design.md 11.4). The demo
-- reset job owns their content: it sets each room's message_expiry_at and
-- history_floor and writes the seed messages. This migration only creates the
-- owner and the rooms, with fixed IDs so re-running is a no-op.

-- A real (non-guest) account with no way to sign in. It owns the demo rooms
-- and authors their seed messages. storage::users::SYSTEM_USER_ID must match.
INSERT INTO users (id, handle, display_name, is_guest, created_at)
VALUES ('00000000-0000-0000-0000-000000000001', 'system', 'System', false, now())
ON CONFLICT (id) DO NOTHING;

INSERT INTO rooms (id, kind, name, created_by, is_demo) VALUES
    ('00000000-0000-0000-0000-00000000a001', 'public', 'general', '00000000-0000-0000-0000-000000000001', true),
    ('00000000-0000-0000-0000-00000000a002', 'public', 'rust',    '00000000-0000-0000-0000-000000000001', true),
    ('00000000-0000-0000-0000-00000000a003', 'public', 'random',  '00000000-0000-0000-0000-000000000001', true)
ON CONFLICT (id) DO NOTHING;

INSERT INTO memberships (room_id, user_id, role) VALUES
    ('00000000-0000-0000-0000-00000000a001', '00000000-0000-0000-0000-000000000001', 'owner'),
    ('00000000-0000-0000-0000-00000000a002', '00000000-0000-0000-0000-000000000001', 'owner'),
    ('00000000-0000-0000-0000-00000000a003', '00000000-0000-0000-0000-000000000001', 'owner')
ON CONFLICT DO NOTHING;
