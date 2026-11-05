//! Messages in Scylla, and history reads across hour buckets.
//!
//! A room's messages are split into one partition per hour
//! (`(room_id, bucket)`, docs/design.md 4.2). `room_buckets` lists the hours
//! that have messages, so reads jump from one non-empty hour to the next
//! instead of probing every hour back to the room's creation.
//!
//! The read algorithms ([`page_back`], [`read_forward`]) are written against
//! the [`HistorySource`] trait, so the unit tests below can run them against
//! an in-memory store with hand-built gaps.

use std::{collections::HashSet, future::Future, sync::Mutex, time::Duration};

use protocol::ids::MessageId;
use scylla::{client::session::Session, statement::prepared::PreparedStatement};
use time::OffsetDateTime;
use uuid::Uuid;

/// A history request reads at most this many partitions, so a run of empty
/// (expired) buckets can't make one request arbitrarily expensive. The client
/// gets a cursor and asks again.
pub const MAX_BUCKETS_PER_REQUEST: usize = 32;
/// Forward reads (sync) look at most this many buckets ahead.
const MAX_FORWARD_BUCKETS: usize = 64;
/// The in-process "buckets already recorded" cache is cleared when it grows
/// past this. Clearing only costs a few repeated (idempotent) index writes.
const BUCKET_CACHE_LIMIT: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMessage {
    pub id: MessageId,
    pub author_id: Uuid,
    /// Empty for deleted messages: the storage layer never returns the
    /// content of a deleted message.
    pub content: String,
    pub edited_at: Option<OffsetDateTime>,
    pub deleted: bool,
    pub version: i64,
}

pub struct NewMessage<'a> {
    pub room_id: Uuid,
    pub id: MessageId,
    pub author_id: Uuid,
    pub content: &'a str,
    /// For guest messages and expiring rooms. `None` keeps the row forever.
    pub ttl: Option<Duration>,
}

/// The driver's errors, boxed and type-erased: callers only report them.
#[derive(Debug, thiserror::Error)]
#[error("scylla: {0}")]
pub struct StoreError(Box<dyn std::error::Error + Send + Sync>);

impl StoreError {
    fn new(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self(Box::new(err))
    }
}

/// What the read algorithms need from storage.
pub trait HistorySource {
    /// Messages in one bucket with `after < id < before`, newest first.
    fn newest_in_bucket(
        &self,
        room_id: Uuid,
        bucket: i32,
        before: i64,
        after: i64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredMessage>, StoreError>> + Send;

    /// Recorded buckets below `below` (all of them if `None`) and at or
    /// above `floor`, newest first.
    fn buckets_below(
        &self,
        room_id: Uuid,
        below: Option<i32>,
        floor: i32,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<i32>, StoreError>> + Send;

    /// Messages in one bucket with `id > after`, oldest first.
    fn oldest_in_bucket(
        &self,
        room_id: Uuid,
        bucket: i32,
        after: i64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredMessage>, StoreError>> + Send;

    /// Recorded buckets at or above `from`, oldest first.
    fn buckets_from(
        &self,
        room_id: Uuid,
        from: i32,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<i32>, StoreError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    /// Newest first.
    pub messages: Vec<StoredMessage>,
    /// Where the next older page starts; `None` at the start of the room.
    pub next_before: Option<MessageId>,
}

/// One page of history older than `before` (the newest messages if `None`),
/// never going below `floor`. The floor is inclusive: it is the lowest
/// visible ID (after a demo reset, the first seed message can have exactly
/// that ID).
pub async fn page_back<S: HistorySource>(
    source: &S,
    room_id: Uuid,
    before: Option<MessageId>,
    floor: MessageId,
    limit: usize,
) -> Result<Page, StoreError> {
    let limit = limit.max(1);
    if before.is_some_and(|b| b <= floor) {
        return Ok(Page {
            messages: Vec::new(),
            next_before: None,
        });
    }
    let floor_bucket = floor.bucket();
    // The source's `after` bound is exclusive.
    let below_floor = floor.as_i64() - 1;
    let mut upper = before.map_or(i64::MAX, MessageId::as_i64);
    let mut messages: Vec<StoredMessage> = Vec::with_capacity(limit);

    // Buckets still to read, newest first. The cursor's own bucket goes
    // first: it may hold older messages below the cursor. Reading it when it
    // isn't in the index costs one empty query.
    let mut queue: std::collections::VecDeque<i32> =
        before.map(MessageId::bucket).into_iter().collect();
    let mut index_cursor = before.map(MessageId::bucket);
    let mut index_exhausted = false;
    let mut scanned = 0;

    loop {
        if queue.is_empty() && !index_exhausted {
            const BATCH: usize = 16;
            let next = source
                .buckets_below(room_id, index_cursor, floor_bucket, BATCH)
                .await?;
            index_exhausted = next.len() < BATCH;
            if let Some(&oldest) = next.last() {
                index_cursor = Some(oldest);
            }
            queue.extend(next);
        }
        let Some(bucket) = queue.pop_front() else {
            // No older bucket: this is the start of the room (or the floor).
            return Ok(Page {
                messages,
                next_before: None,
            });
        };
        if scanned == MAX_BUCKETS_PER_REQUEST {
            // Stop before reading `bucket`; the next request resumes there.
            let next_before = match messages.last() {
                Some(last) => last.id,
                None => protocol::ids::bucket_start(bucket + 1),
            };
            return Ok(Page {
                messages,
                next_before: Some(next_before),
            });
        }
        scanned += 1;
        let rows = source
            .newest_in_bucket(room_id, bucket, upper, below_floor, limit - messages.len())
            .await?;
        messages.extend(rows);
        if messages.len() >= limit {
            messages.truncate(limit);
            let next_before = messages.last().map(|m| m.id);
            return Ok(Page {
                messages,
                next_before,
            });
        }
        // Every later bucket is entirely older than the cursor.
        upper = i64::MAX;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Forward {
    /// Oldest first.
    pub messages: Vec<StoredMessage>,
    /// False if there were more than `limit` messages (or more buckets than
    /// one read covers) after `after`.
    pub complete: bool,
}

/// Messages newer than `after` (and at or above `floor`), oldest first: what
/// a reconnecting client missed.
pub async fn read_forward<S: HistorySource>(
    source: &S,
    room_id: Uuid,
    after: MessageId,
    floor: MessageId,
    limit: usize,
) -> Result<Forward, StoreError> {
    // Both bounds as one exclusive lower bound.
    let after = after.as_i64().max(floor.as_i64() - 1);
    let first_bucket = MessageId::from_i64(after.max(0)).map_or(0, MessageId::bucket);
    let buckets = source
        .buckets_from(room_id, first_bucket, MAX_FORWARD_BUCKETS)
        .await?;
    let mut messages = Vec::new();
    for &bucket in &buckets {
        // One more than needed, to learn whether there are more.
        let wanted = limit + 1 - messages.len();
        messages.extend(
            source
                .oldest_in_bucket(room_id, bucket, after, wanted)
                .await?,
        );
        if messages.len() > limit {
            messages.truncate(limit);
            return Ok(Forward {
                messages,
                complete: false,
            });
        }
    }
    Ok(Forward {
        messages,
        complete: buckets.len() < MAX_FORWARD_BUCKETS,
    })
}

/// The Scylla implementation, with every statement prepared once.
pub struct MessageStore {
    session: Session,
    insert_bucket: PreparedStatement,
    insert_message: PreparedStatement,
    get_message: PreparedStatement,
    get_with_ttl: PreparedStatement,
    newest_in_bucket: PreparedStatement,
    buckets_below_all: PreparedStatement,
    buckets_below: PreparedStatement,
    oldest_in_bucket: PreparedStatement,
    buckets_from: PreparedStatement,
    edit_message: PreparedStatement,
    delete_message: PreparedStatement,
    get_many: PreparedStatement,
    insert_change: PreparedStatement,
    changes_in_day: PreparedStatement,
    set_reaction: PreparedStatement,
    reactions_in_range: PreparedStatement,
    reactions_for_emoji: PreparedStatement,
    /// `(room, bucket)` pairs this process has already written to
    /// `room_buckets`, so each is written about once per room per hour.
    recorded_buckets: Mutex<HashSet<(Uuid, i32)>>,
}

const MESSAGE_COLUMNS: &str = "message_id, author_id, content, edited_at, deleted, version";
type MessageRow = (
    i64,
    Uuid,
    Option<String>,
    Option<OffsetDateTime>,
    Option<bool>,
    Option<i64>,
);

fn from_row(
    (id, author_id, content, edited_at, deleted, version): MessageRow,
) -> Option<StoredMessage> {
    let id = MessageId::from_i64(id)?;
    let deleted = deleted.unwrap_or(false);
    Some(StoredMessage {
        id,
        author_id,
        content: if deleted {
            String::new()
        } else {
            content.unwrap_or_default()
        },
        edited_at,
        deleted,
        version: version.unwrap_or_else(|| id.write_timestamp_us()),
    })
}

impl MessageStore {
    /// `session` must already be using the application keyspace.
    pub async fn new(session: Session) -> Result<Self, StoreError> {
        let prepare = |cql: String| {
            let session = &session;
            async move { session.prepare(cql).await.map_err(StoreError::new) }
        };
        Ok(Self {
            insert_bucket: prepare("INSERT INTO room_buckets (room_id, bucket) VALUES (?, ?)".into()).await?,
            insert_message: prepare(
                "INSERT INTO messages (room_id, bucket, message_id, author_id, content, version) \
                 VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ? AND TTL ?"
                    .into(),
            )
            .await?,
            get_message: prepare(format!(
                "SELECT {MESSAGE_COLUMNS} FROM messages WHERE room_id = ? AND bucket = ? AND message_id = ?"
            ))
            .await?,
            // `author_id` is written once, by the insert, so its TTL is the
            // message's remaining lifetime.
            get_with_ttl: prepare(format!(
                "SELECT {MESSAGE_COLUMNS}, TTL(author_id) FROM messages \
                 WHERE room_id = ? AND bucket = ? AND message_id = ?"
            ))
            .await?,
            newest_in_bucket: prepare(format!(
                "SELECT {MESSAGE_COLUMNS} FROM messages \
                 WHERE room_id = ? AND bucket = ? AND message_id < ? AND message_id > ? LIMIT ?"
            ))
            .await?,
            buckets_below_all: prepare(
                "SELECT bucket FROM room_buckets WHERE room_id = ? AND bucket >= ? LIMIT ?".into(),
            )
            .await?,
            buckets_below: prepare(
                "SELECT bucket FROM room_buckets WHERE room_id = ? AND bucket < ? AND bucket >= ? LIMIT ?"
                    .into(),
            )
            .await?,
            oldest_in_bucket: prepare(format!(
                "SELECT {MESSAGE_COLUMNS} FROM messages \
                 WHERE room_id = ? AND bucket = ? AND message_id > ? ORDER BY message_id ASC LIMIT ?"
            ))
            .await?,
            buckets_from: prepare(
                "SELECT bucket FROM room_buckets WHERE room_id = ? AND bucket >= ? ORDER BY bucket ASC LIMIT ?"
                    .into(),
            )
            .await?,
            edit_message: prepare(
                "UPDATE messages USING TIMESTAMP ? AND TTL ? SET content = ?, edited_at = ?, version = ? \
                 WHERE room_id = ? AND bucket = ? AND message_id = ?"
                    .into(),
            )
            .await?,
            // Clearing to '' instead of null: setting a column to null writes
            // a tombstone (docs/design.md 4.2).
            delete_message: prepare(
                "UPDATE messages USING TIMESTAMP ? AND TTL ? SET deleted = true, content = '', version = ? \
                 WHERE room_id = ? AND bucket = ? AND message_id = ?"
                    .into(),
            )
            .await?,
            get_many: prepare(format!(
                "SELECT {MESSAGE_COLUMNS} FROM messages WHERE room_id = ? AND bucket = ? AND message_id IN ?"
            ))
            .await?,
            insert_change: prepare(
                "INSERT INTO room_changes (room_id, day, change_id, message_id, kind) VALUES (?, ?, ?, ?, ?) \
                 USING TTL ?"
                    .into(),
            )
            .await?,
            changes_in_day: prepare(
                "SELECT change_id, message_id FROM room_changes \
                 WHERE room_id = ? AND day = ? AND change_id > ? LIMIT ?"
                    .into(),
            )
            .await?,
            set_reaction: prepare(
                "UPDATE reactions USING TIMESTAMP ? AND TTL ? SET active = ? \
                 WHERE room_id = ? AND bucket = ? AND message_id = ? AND emoji = ? AND user_id = ?"
                    .into(),
            )
            .await?,
            reactions_in_range: prepare(
                "SELECT message_id, emoji, user_id, active FROM reactions \
                 WHERE room_id = ? AND bucket = ? AND message_id >= ? AND message_id <= ?"
                    .into(),
            )
            .await?,
            reactions_for_emoji: prepare(
                "SELECT active FROM reactions \
                 WHERE room_id = ? AND bucket = ? AND message_id = ? AND emoji = ?"
                    .into(),
            )
            .await?,
            recorded_buckets: Mutex::new(HashSet::new()),
            session,
        })
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Stores a message. Safe to repeat with the same arguments: the same
    /// cells are written with the same write timestamp (the one inside the
    /// ID), so a repeat changes nothing, and it can never overwrite a later
    /// edit or delete, whose timestamps are higher.
    pub async fn insert(&self, new: &NewMessage<'_>) -> Result<(), StoreError> {
        let bucket = new.id.bucket();
        let key = (new.room_id, bucket);
        let recorded = self
            .recorded_buckets
            .lock()
            .expect("bucket cache lock poisoned")
            .contains(&key);
        if !recorded {
            // The index entry goes first. If the message write below then
            // fails, the bucket is merely empty, which pagination handles.
            // The other order could hide a stored message from pagination.
            self.session
                .execute_unpaged(&self.insert_bucket, (new.room_id, bucket))
                .await
                .map_err(StoreError::new)?;
            let mut cache = self
                .recorded_buckets
                .lock()
                .expect("bucket cache lock poisoned");
            if cache.len() >= BUCKET_CACHE_LIMIT {
                cache.clear();
            }
            cache.insert(key);
        }
        // TTL 0 means "no TTL" in CQL.
        let ttl = new.ttl.map_or(0, |ttl| {
            i32::try_from(ttl.as_secs().max(1)).unwrap_or(i32::MAX)
        });
        let write_ts = new.id.write_timestamp_us();
        self.session
            .execute_unpaged(
                &self.insert_message,
                (
                    new.room_id,
                    bucket,
                    new.id.as_i64(),
                    new.author_id,
                    new.content,
                    write_ts,
                    write_ts,
                    ttl,
                ),
            )
            .await
            .map_err(StoreError::new)?;
        Ok(())
    }

    pub async fn get(
        &self,
        room_id: Uuid,
        id: MessageId,
    ) -> Result<Option<StoredMessage>, StoreError> {
        let result = self
            .session
            .execute_unpaged(&self.get_message, (room_id, id.bucket(), id.as_i64()))
            .await
            .map_err(StoreError::new)?;
        let rows = result.into_rows_result().map_err(StoreError::new)?;
        let row = rows
            .maybe_first_row::<MessageRow>()
            .map_err(StoreError::new)?;
        Ok(row.and_then(from_row))
    }

    /// A message and its remaining TTL (`None`: it never expires). Changes
    /// to a message write their cells with this TTL, so no cell outlives
    /// the row (docs/design.md 11.3).
    pub async fn get_with_ttl(
        &self,
        room_id: Uuid,
        id: MessageId,
    ) -> Result<Option<(StoredMessage, Option<Duration>)>, StoreError> {
        type Row = (
            i64,
            Uuid,
            Option<String>,
            Option<OffsetDateTime>,
            Option<bool>,
            Option<i64>,
            Option<i32>,
        );
        let result = self
            .session
            .execute_unpaged(&self.get_with_ttl, (room_id, id.bucket(), id.as_i64()))
            .await
            .map_err(StoreError::new)?;
        let rows = result.into_rows_result().map_err(StoreError::new)?;
        let Some((id, author, content, edited_at, deleted, version, ttl)) =
            rows.maybe_first_row::<Row>().map_err(StoreError::new)?
        else {
            return Ok(None);
        };
        let ttl = ttl.map(|secs| Duration::from_secs(u64::try_from(secs).unwrap_or(0)));
        Ok(from_row((id, author, content, edited_at, deleted, version)).map(|m| (m, ttl)))
    }

    async fn messages(
        &self,
        statement: &PreparedStatement,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<Vec<StoredMessage>, StoreError> {
        let result = self
            .session
            .execute_unpaged(statement, values)
            .await
            .map_err(StoreError::new)?;
        let rows = result.into_rows_result().map_err(StoreError::new)?;
        let mut out = Vec::new();
        for row in rows.rows::<MessageRow>().map_err(StoreError::new)? {
            out.extend(from_row(row.map_err(StoreError::new)?));
        }
        Ok(out)
    }

    async fn buckets(
        &self,
        statement: &PreparedStatement,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<Vec<i32>, StoreError> {
        let result = self
            .session
            .execute_unpaged(statement, values)
            .await
            .map_err(StoreError::new)?;
        let rows = result.into_rows_result().map_err(StoreError::new)?;
        rows.rows::<(i32,)>()
            .map_err(StoreError::new)?
            .map(|row| row.map(|(bucket,)| bucket).map_err(StoreError::new))
            .collect()
    }
}

/// Why a message appears in the change feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum ChangeKind {
    Edit = 1,
    Delete = 2,
    Reaction = 3,
    /// A message acknowledged long after its ID was assigned (a retry of a
    /// slow first attempt): a client that resumed past its ID learns of it
    /// from the change feed (docs/design.md 16.1).
    LateInsert = 4,
}

/// Change rows are kept this long; an older change cursor means a full
/// reload.
pub const CHANGE_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);
const MAX_CHANGES: usize = 500;

/// One emoji's reactions on one message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactionSummary {
    pub emoji: String,
    pub count: u32,
    /// Whether `viewer` (the caller) is one of them.
    pub me: bool,
}

fn ttl_secs(ttl: Option<Duration>) -> i32 {
    // CQL TTL 0 means "no TTL".
    ttl.map_or(0, |ttl| {
        i32::try_from(ttl.as_secs().max(1)).unwrap_or(i32::MAX)
    })
}

impl MessageStore {
    /// Replaces a message's content. `write_ts` must be above the message's
    /// current version (callers use `max(now, version + 1)`), so this edit
    /// wins over any replay of the original insert, whose timestamp is the
    /// ID's. Callers check the message exists first: an UPDATE of a missing
    /// row would create one.
    pub async fn edit(
        &self,
        room_id: Uuid,
        id: MessageId,
        content: &str,
        edited_at: OffsetDateTime,
        write_ts: i64,
        ttl: Option<Duration>,
    ) -> Result<(), StoreError> {
        self.session
            .execute_unpaged(
                &self.edit_message,
                (
                    write_ts,
                    ttl_secs(ttl),
                    content,
                    edited_at,
                    write_ts,
                    room_id,
                    id.bucket(),
                    id.as_i64(),
                ),
            )
            .await
            .map(|_| ())
            .map_err(StoreError::new)
    }

    /// Soft-deletes a message: a flag and cleared content, not a row delete
    /// (docs/design.md 4.2).
    pub async fn delete(
        &self,
        room_id: Uuid,
        id: MessageId,
        write_ts: i64,
        ttl: Option<Duration>,
    ) -> Result<(), StoreError> {
        self.session
            .execute_unpaged(
                &self.delete_message,
                (
                    write_ts,
                    ttl_secs(ttl),
                    write_ts,
                    room_id,
                    id.bucket(),
                    id.as_i64(),
                ),
            )
            .await
            .map(|_| ())
            .map_err(StoreError::new)
    }

    /// Current state of several messages, one query per bucket.
    pub async fn get_many(
        &self,
        room_id: Uuid,
        ids: &[MessageId],
    ) -> Result<Vec<StoredMessage>, StoreError> {
        let mut by_bucket: std::collections::BTreeMap<i32, Vec<i64>> =
            std::collections::BTreeMap::new();
        for id in ids {
            by_bucket.entry(id.bucket()).or_default().push(id.as_i64());
        }
        let mut out = Vec::with_capacity(ids.len());
        for (bucket, ids) in by_bucket {
            out.extend(
                self.messages(&self.get_many, (room_id, bucket, ids))
                    .await?,
            );
        }
        out.sort_unstable_by_key(|m| m.id);
        Ok(out)
    }

    /// Records that a message changed, for the change feed. Written before
    /// the change itself: if the change then fails, the feed points at an
    /// unchanged message, which is harmless; the other order could lose a
    /// change from the feed.
    pub async fn record_change(
        &self,
        room_id: Uuid,
        change_id: MessageId,
        message_id: MessageId,
        kind: ChangeKind,
        ttl: Option<Duration>,
    ) -> Result<(), StoreError> {
        let ttl = ttl.map_or(CHANGE_RETENTION, |t| t.min(CHANGE_RETENTION));
        self.session
            .execute_unpaged(
                &self.insert_change,
                (
                    room_id,
                    change_id.day(),
                    change_id.as_i64(),
                    message_id.as_i64(),
                    kind as i8,
                    ttl_secs(Some(ttl)),
                ),
            )
            .await
            .map(|_| ())
            .map_err(StoreError::new)
    }

    /// Messages changed after `after` (exclusive), up to `now`: their IDs
    /// without duplicates, and false if the feed can't answer completely
    /// (the cursor is older than its retention, or there are too many).
    pub async fn changes_since(
        &self,
        room_id: Uuid,
        after: MessageId,
        now: MessageId,
    ) -> Result<(Vec<MessageId>, bool), StoreError> {
        let retention_days = (CHANGE_RETENTION.as_secs() / 86_400) as i32;
        if now.day() - after.day() >= retention_days {
            return Ok((Vec::new(), false));
        }
        let mut seen = HashSet::new();
        let mut ids = Vec::new();
        for day in after.day()..=now.day() {
            let result = self
                .session
                .execute_unpaged(
                    &self.changes_in_day,
                    (room_id, day, after.as_i64(), cql_limit(MAX_CHANGES + 1)),
                )
                .await
                .map_err(StoreError::new)?;
            let rows = result.into_rows_result().map_err(StoreError::new)?;
            for row in rows.rows::<(i64, i64)>().map_err(StoreError::new)? {
                let (_, message_id) = row.map_err(StoreError::new)?;
                if let Some(id) = MessageId::from_i64(message_id)
                    && seen.insert(id)
                {
                    ids.push(id);
                }
            }
            if ids.len() > MAX_CHANGES {
                ids.truncate(MAX_CHANGES);
                return Ok((ids, false));
            }
        }
        Ok((ids, true))
    }

    /// Adds or removes one user's reaction. A soft toggle (`active`), never a
    /// row delete, for the same tombstone reason as messages. Last write wins
    /// between two toggles of the same reaction.
    #[allow(clippy::too_many_arguments)]
    pub async fn set_reaction(
        &self,
        room_id: Uuid,
        id: MessageId,
        emoji: &str,
        user_id: Uuid,
        active: bool,
        write_ts: i64,
        ttl: Option<Duration>,
    ) -> Result<(), StoreError> {
        self.session
            .execute_unpaged(
                &self.set_reaction,
                (
                    write_ts,
                    ttl_secs(ttl),
                    active,
                    room_id,
                    id.bucket(),
                    id.as_i64(),
                    emoji,
                    user_id,
                ),
            )
            .await
            .map(|_| ())
            .map_err(StoreError::new)
    }

    /// How many users currently have this reaction on this message.
    pub async fn reaction_count(
        &self,
        room_id: Uuid,
        id: MessageId,
        emoji: &str,
    ) -> Result<u32, StoreError> {
        let result = self
            .session
            .execute_unpaged(
                &self.reactions_for_emoji,
                (room_id, id.bucket(), id.as_i64(), emoji),
            )
            .await
            .map_err(StoreError::new)?;
        let rows = result.into_rows_result().map_err(StoreError::new)?;
        let mut count = 0;
        for row in rows.rows::<(Option<bool>,)>().map_err(StoreError::new)? {
            if row.map_err(StoreError::new)?.0 == Some(true) {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Reactions for a set of messages (one page, typically), as seen by
    /// `viewer`. One range query per bucket the messages span.
    pub async fn reactions_for(
        &self,
        room_id: Uuid,
        ids: &[MessageId],
        viewer: Uuid,
    ) -> Result<std::collections::HashMap<MessageId, Vec<ReactionSummary>>, StoreError> {
        let mut ranges: std::collections::BTreeMap<i32, (i64, i64)> =
            std::collections::BTreeMap::new();
        for id in ids {
            let range = ranges.entry(id.bucket()).or_insert((i64::MAX, i64::MIN));
            range.0 = range.0.min(id.as_i64());
            range.1 = range.1.max(id.as_i64());
        }
        let wanted: HashSet<i64> = ids.iter().map(|id| id.as_i64()).collect();
        // (message, emoji) -> (count, me)
        let mut totals: std::collections::HashMap<(i64, String), (u32, bool)> =
            std::collections::HashMap::new();
        for (bucket, (low, high)) in ranges {
            let result = self
                .session
                .execute_unpaged(&self.reactions_in_range, (room_id, bucket, low, high))
                .await
                .map_err(StoreError::new)?;
            let rows = result.into_rows_result().map_err(StoreError::new)?;
            for row in rows
                .rows::<(i64, String, Uuid, Option<bool>)>()
                .map_err(StoreError::new)?
            {
                let (message_id, emoji, user_id, active) = row.map_err(StoreError::new)?;
                if active != Some(true) || !wanted.contains(&message_id) {
                    continue;
                }
                let entry = totals.entry((message_id, emoji)).or_default();
                entry.0 += 1;
                entry.1 |= user_id == viewer;
            }
        }
        let mut out: std::collections::HashMap<MessageId, Vec<ReactionSummary>> =
            std::collections::HashMap::new();
        for ((message_id, emoji), (count, me)) in totals {
            if let Some(id) = MessageId::from_i64(message_id) {
                out.entry(id)
                    .or_default()
                    .push(ReactionSummary { emoji, count, me });
            }
        }
        // A stable order: the order the app offers them in.
        let rank = |emoji: &str| {
            protocol::reactions::ALLOWED
                .iter()
                .position(|e| *e == emoji)
                .unwrap_or(usize::MAX)
        };
        for list in out.values_mut() {
            list.sort_by_key(|r| rank(&r.emoji));
        }
        Ok(out)
    }
}

/// Unread counts are exact up to this many and shown as "99+" beyond.
pub const UNREAD_CAP: usize = 99;

/// Messages from others after `after`, not deleted: (count, capped). Reads
/// at most `UNREAD_CAP + 1` messages, whatever the room's size, so the cost
/// is bounded per reader (docs/design.md 9).
///
/// Capped means "at least 100 messages since your read position", which can
/// include some of your own; the count is then reported as `UNREAD_CAP` and
/// shown as "99+". Exact below the cap.
pub async fn count_unread<S: HistorySource>(
    source: &S,
    room_id: Uuid,
    after: MessageId,
    floor: MessageId,
    viewer: Uuid,
) -> Result<(u32, bool), StoreError> {
    let forward = read_forward(source, room_id, after, floor, UNREAD_CAP + 1).await?;
    let count = forward
        .messages
        .iter()
        .filter(|m| m.author_id != viewer && !m.deleted && m.id > after)
        .count();
    let capped = count > UNREAD_CAP || !forward.complete;
    let count = if capped { UNREAD_CAP } else { count };
    Ok((count as u32, capped))
}

fn cql_limit(limit: usize) -> i32 {
    i32::try_from(limit).unwrap_or(i32::MAX)
}

impl HistorySource for MessageStore {
    async fn newest_in_bucket(
        &self,
        room_id: Uuid,
        bucket: i32,
        before: i64,
        after: i64,
        limit: usize,
    ) -> Result<Vec<StoredMessage>, StoreError> {
        self.messages(
            &self.newest_in_bucket,
            (room_id, bucket, before, after, cql_limit(limit)),
        )
        .await
    }

    async fn buckets_below(
        &self,
        room_id: Uuid,
        below: Option<i32>,
        floor: i32,
        limit: usize,
    ) -> Result<Vec<i32>, StoreError> {
        match below {
            Some(below) => {
                self.buckets(
                    &self.buckets_below,
                    (room_id, below, floor, cql_limit(limit)),
                )
                .await
            }
            None => {
                self.buckets(&self.buckets_below_all, (room_id, floor, cql_limit(limit)))
                    .await
            }
        }
    }

    async fn oldest_in_bucket(
        &self,
        room_id: Uuid,
        bucket: i32,
        after: i64,
        limit: usize,
    ) -> Result<Vec<StoredMessage>, StoreError> {
        self.messages(
            &self.oldest_in_bucket,
            (room_id, bucket, after, cql_limit(limit)),
        )
        .await
    }

    async fn buckets_from(
        &self,
        room_id: Uuid,
        from: i32,
        limit: usize,
    ) -> Result<Vec<i32>, StoreError> {
        self.buckets(&self.buckets_from, (room_id, from, cql_limit(limit)))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use protocol::ids::BUCKET_MS;

    use super::*;

    /// Messages per bucket, plus an index that can disagree with them, as the
    /// real one can (a failed write, or every row expired).
    #[derive(Default)]
    struct Fake {
        buckets: BTreeMap<i32, BTreeMap<i64, StoredMessage>>,
        index: BTreeSet<i32>,
        reads: std::sync::atomic::AtomicUsize,
    }

    fn msg(hour: u64, n: u16) -> StoredMessage {
        let id = MessageId::from_parts(hour * BUCKET_MS + u64::from(n), 0, n);
        StoredMessage {
            id,
            author_id: Uuid::nil(),
            content: format!("{hour}/{n}"),
            edited_at: None,
            deleted: false,
            version: id.write_timestamp_us(),
        }
    }

    impl Fake {
        fn add(&mut self, m: StoredMessage) {
            let bucket = m.id.bucket();
            self.index.insert(bucket);
            self.buckets
                .entry(bucket)
                .or_default()
                .insert(m.id.as_i64(), m);
        }

        /// Messages are gone but the index still lists the bucket.
        fn expire_bucket(&mut self, bucket: i32) {
            self.buckets.remove(&bucket);
            self.index.insert(bucket);
        }

        fn all_newest_first(&self) -> Vec<MessageId> {
            let mut ids: Vec<_> = self
                .buckets
                .values()
                .flat_map(|b| b.values().map(|m| m.id))
                .collect();
            ids.sort_unstable_by(|a, b| b.cmp(a));
            ids
        }
    }

    impl HistorySource for Fake {
        async fn newest_in_bucket(
            &self,
            _room: Uuid,
            bucket: i32,
            before: i64,
            after: i64,
            limit: usize,
        ) -> Result<Vec<StoredMessage>, StoreError> {
            self.reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(self
                .buckets
                .get(&bucket)
                .map(|b| {
                    b.range(after + 1..before)
                        .rev()
                        .take(limit)
                        .map(|(_, m)| m.clone())
                        .collect()
                })
                .unwrap_or_default())
        }

        async fn buckets_below(
            &self,
            _room: Uuid,
            below: Option<i32>,
            floor: i32,
            limit: usize,
        ) -> Result<Vec<i32>, StoreError> {
            let upper = below.unwrap_or(i32::MAX);
            Ok(self
                .index
                .range(floor..upper)
                .rev()
                .take(limit)
                .copied()
                .collect())
        }

        async fn oldest_in_bucket(
            &self,
            _room: Uuid,
            bucket: i32,
            after: i64,
            limit: usize,
        ) -> Result<Vec<StoredMessage>, StoreError> {
            Ok(self
                .buckets
                .get(&bucket)
                .map(|b| {
                    b.range(after + 1..)
                        .take(limit)
                        .map(|(_, m)| m.clone())
                        .collect()
                })
                .unwrap_or_default())
        }

        async fn buckets_from(
            &self,
            _room: Uuid,
            from: i32,
            limit: usize,
        ) -> Result<Vec<i32>, StoreError> {
            Ok(self.index.range(from..).take(limit).copied().collect())
        }
    }

    const ROOM: Uuid = Uuid::nil();
    /// The lowest possible ID: no floor.
    fn no_floor() -> MessageId {
        MessageId::from_parts(0, 0, 0)
    }

    /// Pages through the whole room, checking each page is strictly older
    /// than the last, and returns every ID seen.
    async fn walk(fake: &Fake, floor: MessageId, limit: usize) -> (Vec<MessageId>, usize) {
        let mut seen = Vec::new();
        let mut before = None;
        let mut pages = 0;
        loop {
            let page = page_back(fake, ROOM, before, floor, limit).await.unwrap();
            pages += 1;
            assert!(page.messages.len() <= limit);
            for m in &page.messages {
                if let Some(&prev) = seen.last() {
                    assert!(
                        m.id < prev,
                        "not strictly descending: {} after {prev}",
                        m.id
                    );
                }
                seen.push(m.id);
            }
            match page.next_before {
                Some(next) => before = Some(next),
                None => return (seen, pages),
            }
            assert!(pages < 10_000, "pagination does not terminate");
        }
    }

    #[tokio::test]
    async fn empty_room() {
        let fake = Fake::default();
        let page = page_back(&fake, ROOM, None, no_floor(), 50).await.unwrap();
        assert!(page.messages.is_empty());
        assert_eq!(page.next_before, None);
    }

    #[tokio::test]
    async fn pages_within_one_bucket() {
        let mut fake = Fake::default();
        for n in 0..120 {
            fake.add(msg(5, n));
        }
        let first = page_back(&fake, ROOM, None, no_floor(), 50).await.unwrap();
        assert_eq!(first.messages.len(), 50);
        assert_eq!(first.messages[0].id, msg(5, 119).id);
        assert_eq!(first.next_before, Some(msg(5, 70).id));
        let (seen, _) = walk(&fake, no_floor(), 50).await;
        assert_eq!(seen, fake.all_newest_first());
    }

    #[tokio::test]
    async fn crosses_buckets_and_skips_unrecorded_hours() {
        let mut fake = Fake::default();
        // Hours 0, 1, 7 and 900 have messages; everything between is empty
        // and not in the index.
        for (hour, count) in [(0, 3), (1, 1), (7, 60), (900, 2)] {
            for n in 0..count {
                fake.add(msg(hour, n));
            }
        }
        let (seen, _) = walk(&fake, no_floor(), 25).await;
        assert_eq!(seen, fake.all_newest_first(), "no gaps, no duplicates");
        // Exactly the four recorded buckets were read, plus the cursor's own
        // bucket once per page: nothing like 900 probes.
        assert!(fake.reads.load(std::sync::atomic::Ordering::Relaxed) < 10);
    }

    #[tokio::test]
    async fn recorded_but_empty_buckets_are_skipped() {
        let mut fake = Fake::default();
        fake.add(msg(1, 0));
        for hour in 2..10 {
            fake.expire_bucket(hour);
        }
        fake.add(msg(10, 0));
        let (seen, _) = walk(&fake, no_floor(), 50).await;
        assert_eq!(seen, [msg(10, 0).id, msg(1, 0).id]);
    }

    /// The newest hours are all expired (a room whose recent messages were
    /// guests'); older history must still be reachable from the first page.
    #[tokio::test]
    async fn empty_newest_buckets_dont_hide_older_history() {
        let mut fake = Fake::default();
        fake.add(msg(1, 0));
        fake.add(msg(1, 1));
        for hour in 2..6 {
            fake.expire_bucket(hour);
        }
        let first = page_back(&fake, ROOM, None, no_floor(), 50).await.unwrap();
        let ids: Vec<_> = first.messages.iter().map(|m| m.id).collect();
        assert_eq!(ids, [msg(1, 1).id, msg(1, 0).id]);
        assert_eq!(first.next_before, None);
    }

    #[tokio::test]
    async fn long_runs_of_empty_buckets_return_a_cursor_not_a_huge_scan() {
        let mut fake = Fake::default();
        fake.add(msg(0, 0));
        for hour in 1..=100 {
            fake.expire_bucket(hour);
        }
        fake.add(msg(101, 0));

        let first = page_back(&fake, ROOM, None, no_floor(), 50).await.unwrap();
        // Bucket 101 plus 31 empty ones, then a cursor.
        assert_eq!(first.messages.len(), 1);
        assert!(first.next_before.is_some());
        let (seen, pages) = walk(&fake, no_floor(), 50).await;
        assert_eq!(seen, [msg(101, 0).id, msg(0, 0).id]);
        assert!(
            pages >= 4,
            "102 buckets need at least 4 requests of 32, took {pages}"
        );
    }

    #[tokio::test]
    async fn stops_at_the_floor() {
        let mut fake = Fake::default();
        for hour in 0..5 {
            for n in 0..3 {
                fake.add(msg(hour, n));
            }
        }
        // A demo reset at the start of hour 3.
        let floor = protocol::ids::bucket_start(3);
        let (seen, _) = walk(&fake, floor, 2).await;
        assert!(seen.iter().all(|id| *id >= floor));
        assert!(seen.contains(&floor), "the floor itself is visible");
        assert_eq!(seen.len(), 6);
        // A cursor at or below the floor returns nothing.
        let page = page_back(&fake, ROOM, Some(floor), floor, 10)
            .await
            .unwrap();
        assert!(page.messages.is_empty() && page.next_before.is_none());
    }

    #[tokio::test]
    async fn cursor_in_the_middle_of_a_bucket() {
        let mut fake = Fake::default();
        for n in 0..10 {
            fake.add(msg(3, n));
            fake.add(msg(4, n));
        }
        let page = page_back(&fake, ROOM, Some(msg(4, 2).id), no_floor(), 5)
            .await
            .unwrap();
        let ids: Vec<_> = page.messages.iter().map(|m| m.id).collect();
        assert_eq!(
            ids,
            [
                msg(4, 1).id,
                msg(4, 0).id,
                msg(3, 9).id,
                msg(3, 8).id,
                msg(3, 7).id
            ]
        );
    }

    #[tokio::test]
    async fn randomized_layouts_page_completely() {
        // A deterministic generator: storage doesn't depend on `rand`.
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = move |modulo: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % modulo
        };
        for _ in 0..50 {
            let mut fake = Fake::default();
            for _ in 0..next(300) {
                fake.add(msg(next(500), next(4000) as u16));
            }
            for _ in 0..next(80) {
                let hour = next(500) as i32;
                if !fake.buckets.contains_key(&hour) {
                    fake.expire_bucket(hour);
                }
            }
            let limit = 1 + next(60) as usize;
            let (seen, _) = walk(&fake, no_floor(), limit).await;
            assert_eq!(seen, fake.all_newest_first(), "limit {limit}");
        }
    }

    #[tokio::test]
    async fn unread_counts_others_messages_only_and_caps() {
        let me = Uuid::from_u128(7);
        let mut fake = Fake::default();
        for n in 0..5 {
            fake.add(msg(1, n));
        }
        let mut mine = msg(1, 10);
        mine.author_id = me;
        fake.add(mine);
        let mut deleted = msg(1, 11);
        deleted.deleted = true;
        fake.add(deleted);
        // Read up to the second message: three others' messages remain.
        let (count, capped) = count_unread(&fake, ROOM, msg(1, 1).id, no_floor(), me)
            .await
            .unwrap();
        assert_eq!((count, capped), (3, false));

        for n in 100..300 {
            fake.add(msg(2, n));
        }
        let (count, capped) = count_unread(&fake, ROOM, msg(1, 1).id, no_floor(), me)
            .await
            .unwrap();
        assert_eq!((count, capped), (99, true));
    }

    #[tokio::test]
    async fn forward_reads_cross_buckets_oldest_first() {
        let mut fake = Fake::default();
        for n in 0..5 {
            fake.add(msg(2, n));
            fake.add(msg(6, n));
        }
        let fwd = read_forward(&fake, ROOM, msg(2, 2).id, no_floor(), 100)
            .await
            .unwrap();
        let ids: Vec<_> = fwd.messages.iter().map(|m| m.id).collect();
        let mut expected = vec![msg(2, 3).id, msg(2, 4).id];
        expected.extend((0..5).map(|n| msg(6, n).id));
        assert_eq!(ids, expected);
        assert!(fwd.complete);

        let capped = read_forward(&fake, ROOM, no_floor(), no_floor(), 4)
            .await
            .unwrap();
        assert_eq!(capped.messages.len(), 4);
        assert!(!capped.complete);
    }
}
