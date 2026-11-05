-- Read positions and the search index (docs/design.md 9 and 10).

-- "Seen by N": members whose read position is at or after a message.
CREATE INDEX memberships_read_position ON memberships (room_id, last_read_id);

-- Full-text index over message content. Holds only the tsvector, not the
-- text: results are re-read from Scylla, so deleted content is never copied
-- into a second database and never shown even if this index lags.
CREATE TABLE message_search (
    message_id bigint PRIMARY KEY,
    room_id    uuid NOT NULL,
    author_id  uuid NOT NULL,
    tsv        tsvector NOT NULL,
    deleted    boolean NOT NULL DEFAULT false,
    -- The message's version when indexed. An update only applies if newer,
    -- so out-of-order or repeated index events can't regress the index.
    version    bigint NOT NULL,
    -- Guest messages and expiring rooms: hidden from search after this.
    expires_at timestamptz
);

CREATE INDEX message_search_tsv ON message_search USING gin (tsv);
CREATE INDEX message_search_room ON message_search (room_id, message_id);
