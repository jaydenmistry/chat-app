//! Message IDs: 64-bit, time-sortable, Snowflake-style.
//!
//! ```text
//!  bit 63      62 ............ 22   21 ...... 12   11 ...... 0
//!  [ 0 ]  [ ms since 2026-01-01 ]  [ worker id ]  [ sequence ]
//!            41 bits (~69 years)      10 bits        12 bits
//! ```
//!
//! The top bit is always zero, so an ID fits a signed `bigint` in both
//! Postgres and Scylla. IDs sort by creation time, and the time inside an ID
//! gives its Scylla bucket, so a message is addressable from `(room, id)`.
//!
//! In JSON an ID is a decimal string: JavaScript numbers lose precision above
//! 2^53, and these IDs are larger than that.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use ts_rs::TS;

/// 2026-01-01T00:00:00Z in Unix milliseconds.
pub const EPOCH_UNIX_MS: u64 = 1_767_225_600_000;

pub const WORKER_BITS: u32 = 10;
pub const SEQUENCE_BITS: u32 = 12;
pub const TIMESTAMP_BITS: u32 = 41;

pub const MAX_WORKER: u16 = (1 << WORKER_BITS) - 1;
pub const MAX_SEQUENCE: u16 = (1 << SEQUENCE_BITS) - 1;
pub const MAX_TIMESTAMP_MS: u64 = (1 << TIMESTAMP_BITS) - 1;

/// One Scylla partition per room per hour (docs/design.md 4.2).
pub const BUCKET_MS: u64 = 3_600_000;
/// The change feed is partitioned per room per day.
pub const DAY_MS: u64 = 86_400_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TS)]
#[ts(type = "string")]
pub struct MessageId(i64);

impl MessageId {
    /// Assembles an ID. Panics in debug builds if a part is out of range;
    /// the generator never produces such parts.
    pub fn from_parts(ms_since_epoch: u64, worker: u16, sequence: u16) -> Self {
        debug_assert!(ms_since_epoch <= MAX_TIMESTAMP_MS);
        debug_assert!(worker <= MAX_WORKER);
        debug_assert!(sequence <= MAX_SEQUENCE);
        let raw = (ms_since_epoch << (WORKER_BITS + SEQUENCE_BITS))
            | (u64::from(worker) << SEQUENCE_BITS)
            | u64::from(sequence);
        Self(raw as i64)
    }

    /// The smallest ID that can exist at this millisecond. Used as a cursor
    /// ("everything from this time on") rather than as a real message ID.
    pub fn first_at_ms(ms_since_epoch: u64) -> Self {
        Self::from_parts(ms_since_epoch.min(MAX_TIMESTAMP_MS), 0, 0)
    }

    /// The smallest ID at a Unix time. Times before the epoch clamp to it.
    pub fn first_at_unix_ms(unix_ms: u64) -> Self {
        Self::first_at_ms(unix_ms.saturating_sub(EPOCH_UNIX_MS))
    }

    /// `None` for negative values, which no valid ID has.
    pub fn from_i64(raw: i64) -> Option<Self> {
        (raw >= 0).then_some(Self(raw))
    }

    pub fn as_i64(self) -> i64 {
        self.0
    }

    pub fn ms_since_epoch(self) -> u64 {
        (self.0 as u64) >> (WORKER_BITS + SEQUENCE_BITS)
    }

    pub fn unix_ms(self) -> u64 {
        self.ms_since_epoch() + EPOCH_UNIX_MS
    }

    pub fn worker(self) -> u16 {
        (((self.0 as u64) >> SEQUENCE_BITS) & u64::from(MAX_WORKER)) as u16
    }

    pub fn sequence(self) -> u16 {
        ((self.0 as u64) & u64::from(MAX_SEQUENCE)) as u16
    }

    /// The hour bucket the message is stored in.
    pub fn bucket(self) -> i32 {
        bucket_of_ms(self.ms_since_epoch())
    }

    /// The day partition of the change feed, for change IDs.
    pub fn day(self) -> i32 {
        (self.ms_since_epoch() / DAY_MS) as i32
    }

    /// The Scylla write timestamp (microseconds since the Unix epoch) used
    /// for the message's first insert. Every replay of that insert uses the
    /// same timestamp, so a replay can never overwrite a later edit
    /// (docs/design.md 4.2).
    pub fn write_timestamp_us(self) -> i64 {
        (self.unix_ms() * 1000) as i64
    }

    /// The first ID `ms` milliseconds earlier: the start of the resume
    /// overlap window (docs/design.md 16.1).
    pub fn minus_ms(self, ms: u64) -> Self {
        Self::first_at_ms(self.ms_since_epoch().saturating_sub(ms))
    }
}

/// The hour bucket containing a time, in milliseconds since the ID epoch.
pub fn bucket_of_ms(ms_since_epoch: u64) -> i32 {
    (ms_since_epoch / BUCKET_MS) as i32
}

/// The first ID of an hour bucket.
pub fn bucket_start(bucket: i32) -> MessageId {
    MessageId::first_at_ms(u64::try_from(bucket).unwrap_or(0) * BUCKET_MS)
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseIdError;

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not a message ID")
    }
}

impl std::error::Error for ParseIdError {}

impl FromStr for MessageId {
    type Err = ParseIdError;

    /// Digits only: no sign, no whitespace.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ParseIdError);
        }
        s.parse::<i64>().map(Self).map_err(|_| ParseIdError)
    }
}

impl Serialize for MessageId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MessageId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parts_round_trip() {
        let id = MessageId::from_parts(123_456_789, 513, 4095);
        assert_eq!(id.ms_since_epoch(), 123_456_789);
        assert_eq!(id.worker(), 513);
        assert_eq!(id.sequence(), 4095);
        assert_eq!(id.unix_ms(), EPOCH_UNIX_MS + 123_456_789);
    }

    #[test]
    fn extreme_values_stay_non_negative() {
        let max = MessageId::from_parts(MAX_TIMESTAMP_MS, MAX_WORKER, MAX_SEQUENCE);
        assert_eq!(max.as_i64(), i64::MAX);
        assert_eq!(MessageId::from_parts(0, 0, 0).as_i64(), 0);
        assert_eq!(MessageId::from_i64(-1), None);
    }

    #[test]
    fn ids_sort_by_time_then_worker_then_sequence() {
        let a = MessageId::from_parts(1000, 1023, 4095);
        let b = MessageId::from_parts(1001, 0, 0);
        let c = MessageId::from_parts(1001, 0, 1);
        let d = MessageId::from_parts(1001, 1, 0);
        assert!(a < b && b < c && c < d);
    }

    #[test]
    fn buckets_are_hours_since_the_epoch() {
        assert_eq!(MessageId::from_parts(0, 0, 0).bucket(), 0);
        assert_eq!(MessageId::from_parts(BUCKET_MS - 1, 9, 9).bucket(), 0);
        assert_eq!(MessageId::from_parts(BUCKET_MS, 0, 0).bucket(), 1);
        assert_eq!(bucket_start(1), MessageId::from_parts(BUCKET_MS, 0, 0));
        // Every ID in a bucket is at or after the bucket's first ID.
        let last_in_0 = MessageId::from_parts(BUCKET_MS - 1, MAX_WORKER, MAX_SEQUENCE);
        assert!(last_in_0 < bucket_start(1));
        assert_eq!(MessageId::from_parts(DAY_MS * 3 + 5, 0, 0).day(), 3);
    }

    #[test]
    fn unix_time_conversions() {
        // 2026-01-01T01:00:00Z is the first instant of bucket 1.
        let id = MessageId::first_at_unix_ms(EPOCH_UNIX_MS + BUCKET_MS);
        assert_eq!(id.bucket(), 1);
        assert_eq!(
            id.write_timestamp_us(),
            ((EPOCH_UNIX_MS + BUCKET_MS) * 1000) as i64
        );
        assert_eq!(
            MessageId::first_at_unix_ms(0),
            MessageId::from_parts(0, 0, 0)
        );
    }

    #[test]
    fn overlap_window_start() {
        let id = MessageId::from_parts(10_000, 7, 7);
        assert_eq!(id.minus_ms(5_000), MessageId::from_parts(5_000, 0, 0));
        assert_eq!(id.minus_ms(50_000), MessageId::from_parts(0, 0, 0));
    }

    #[test]
    fn json_is_a_decimal_string() {
        let id = MessageId::from_parts(MAX_TIMESTAMP_MS, MAX_WORKER, MAX_SEQUENCE);
        assert_eq!(
            serde_json::to_string(&id).unwrap(),
            "\"9223372036854775807\""
        );
        assert_eq!(
            serde_json::from_str::<MessageId>("\"42\"")
                .unwrap()
                .as_i64(),
            42
        );
        for bad in [
            "42",
            "\"-1\"",
            "\" 42\"",
            "\"\"",
            "\"4x\"",
            "\"9223372036854775808\"",
        ] {
            assert!(serde_json::from_str::<MessageId>(bad).is_err(), "{bad}");
        }
    }
}
