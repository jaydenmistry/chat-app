-- Messages, reactions and the change feed. See docs/design.md section 4.2.
-- Every statement must be safe to run twice (IF NOT EXISTS): Scylla has no
-- transactional DDL, so a migration interrupted halfway is simply re-run.

-- One partition per room per hour. bucket = hours since the ID epoch,
-- derived from message_id, so a message is addressable from (room_id, id).
CREATE TABLE IF NOT EXISTS messages (
    room_id    uuid,
    bucket     int,
    message_id bigint,
    author_id  uuid,
    content    text,
    edited_at  timestamp,
    deleted    boolean,
    version    bigint,
    PRIMARY KEY ((room_id, bucket), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

-- The non-empty buckets of each room, so pagination skips empty hours and
-- knows where the room starts.
CREATE TABLE IF NOT EXISTS room_buckets (
    room_id uuid,
    bucket  int,
    PRIMARY KEY (room_id, bucket)
) WITH CLUSTERING ORDER BY (bucket DESC);

-- One row per (message, emoji, user). Removing a reaction sets active = false
-- instead of deleting the row, which would write a tombstone.
CREATE TABLE IF NOT EXISTS reactions (
    room_id    uuid,
    bucket     int,
    message_id bigint,
    emoji      text,
    user_id    uuid,
    active     boolean,
    PRIMARY KEY ((room_id, bucket), message_id, emoji, user_id)
);

-- Edits, deletes and reaction changes, so a reconnecting client can catch up
-- on changes to messages it already has. Rows expire after 7 days; one
-- partition per room per day keeps expired data in whole SSTables that
-- time-window compaction can drop.
CREATE TABLE IF NOT EXISTS room_changes (
    room_id    uuid,
    day        int,
    change_id  bigint,
    message_id bigint,
    kind       tinyint,
    PRIMARY KEY ((room_id, day), change_id)
) WITH default_time_to_live = 604800
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 1
  };
