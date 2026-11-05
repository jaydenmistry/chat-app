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
    newest_in_bucket: PreparedStatement,
    buckets_below_all: PreparedStatement,
    buckets_below: PreparedStatement,
    oldest_in_bucket: PreparedStatement,
    buckets_from: PreparedStatement,
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
