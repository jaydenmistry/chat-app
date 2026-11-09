//! Message ID generation and worker-ID leases (docs/design.md section 3).
//!
//! Scylla inserts are upserts, so two messages with the same ID in the same
//! room would silently overwrite each other. IDs are unique because each
//! running instance holds a different 10-bit worker ID, leased from Redis:
//!
//! 1. Acquire `worker:<n>` with `SET NX PX`, starting from a random `n`.
//! 2. Wait one lease period before issuing IDs (quarantine), so a previous
//!    holder that lost its key in a Redis restart notices first. The lease is
//!    renewed throughout.
//! 3. Renew every few seconds, only if the key still holds our instance ID.
//! 4. Treat the lease as valid only until `renewal sent + TTL - 1 s`, by the
//!    local monotonic clock. Redis starts its countdown after receiving the
//!    request, so our deadline always passes first.
//! 5. Record the last millisecond used (`worker_hwm:<n>`); a new holder of
//!    `n` starts after it, even if its clock is behind ours.
//!
//! Each renewal also compares our clock with Redis `TIME`. More than 500 ms
//! apart and we stop issuing IDs, which bounds the skew between any two
//! instances to 1 s, which bounds how late a message can commit relative to
//! its ID, which is what makes resume correct (design 16.1).

use std::{
    sync::{LazyLock, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use protocol::ids::{EPOCH_UNIX_MS, MAX_SEQUENCE, MAX_WORKER, MessageId};
use rand::RngExt;
use redis::{Script, aio::ConnectionManager};
use tokio::sync::watch;
use uuid::Uuid;

/// The logical clock may run at most this far ahead of the wall clock: it
/// absorbs small backward clock steps and sequence overflow. Beyond it, the
/// generator refuses (the clock jumped back, or someone is issuing over 4096
/// IDs per millisecond).
const MAX_AHEAD_MS: u64 = 10;

#[derive(Debug, Clone, Copy)]
pub struct LeaseTimings {
    pub ttl: Duration,
    pub renew_every: Duration,
    pub quarantine: Duration,
    pub max_clock_skew: Duration,
}

impl Default for LeaseTimings {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(15),
            renew_every: Duration::from_secs(5),
            quarantine: Duration::from_secs(15),
            max_clock_skew: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    #[error("no valid worker-ID lease")]
    NoLease,
    #[error("clock is {0} ms behind the last issued ID")]
    ClockBehind(u64),
}

/// The generator's state. Pure: callers pass in the time, so the tests can
/// drive it through any sequence of clock readings.
#[derive(Debug)]
struct State {
    worker: Option<u16>,
    valid_until: Option<Instant>,
    /// The last (millisecond, sequence) issued, if any.
    last: Option<(u64, u16)>,
    /// No ID may use a millisecond below this (the previous holder's high
    /// water mark, plus one).
    min_ms: u64,
}

impl State {
    fn next(&mut self, now_ms: u64, now: Instant) -> Result<MessageId, IdError> {
        let worker = match (self.worker, self.valid_until) {
            (Some(worker), Some(until)) if now < until => worker,
            _ => return Err(IdError::NoLease),
        };
        let mut ms = now_ms.max(self.min_ms);
        let sequence = match self.last {
            Some((last_ms, last_seq)) if ms <= last_ms => {
                if last_seq < MAX_SEQUENCE {
                    ms = last_ms;
                    last_seq + 1
                } else {
                    ms = last_ms + 1;
                    0
                }
            }
            _ => 0,
        };
        if ms > now_ms.max(self.min_ms) + MAX_AHEAD_MS {
            return Err(IdError::ClockBehind(ms - now_ms));
        }
        self.last = Some((ms, sequence));
        Ok(MessageId::from_parts(ms, worker, sequence))
    }

    fn last_ms(&self) -> u64 {
        self.last.map_or(self.min_ms, |(ms, _)| ms)
    }
}

pub struct IdGenerator {
    state: Mutex<State>,
    /// Shifts this instance's clock, so tests can run instances with
    /// different clocks. Always 0 outside tests.
    clock_offset_ms: i64,
}

impl IdGenerator {
    pub fn new(clock_offset_ms: i64) -> Self {
        Self {
            state: Mutex::new(State {
                worker: None,
                valid_until: None,
                last: None,
                min_ms: 0,
            }),
            clock_offset_ms,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // Nothing panics while holding this lock, so poisoning would be a bug.
        self.state.lock().expect("id generator lock poisoned")
    }

    /// Unix time in milliseconds, as this instance sees it.
    pub fn unix_ms(&self) -> u64 {
        let real = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        real.saturating_add_signed(self.clock_offset_ms)
    }

    /// Milliseconds since the ID epoch.
    pub fn now_ms(&self) -> u64 {
        self.unix_ms().saturating_sub(EPOCH_UNIX_MS)
    }

    pub fn next_id(&self) -> Result<MessageId, IdError> {
        let now_ms = self.now_ms();
        self.lock().next(now_ms, Instant::now())
    }

    /// Whether IDs can be issued right now (for `/readyz`).
    pub fn is_ready(&self) -> bool {
        let state = self.lock();
        state.worker.is_some()
            && state
                .valid_until
                .is_some_and(|until| Instant::now() < until)
    }

    pub fn worker(&self) -> Option<u16> {
        self.lock().worker
    }

    fn last_ms(&self) -> u64 {
        self.lock().last_ms()
    }

    /// Starts using a newly leased worker ID, after `hwm` (the last
    /// millisecond any previous holder used).
    fn install(&self, worker: u16, hwm: Option<u64>, valid_until: Instant) {
        let mut state = self.lock();
        let previous_last = state.last_ms();
        state.worker = Some(worker);
        state.valid_until = Some(valid_until);
        state.min_ms = hwm.map_or(0, |h| h + 1).max(previous_last);
        state.last = None;
    }

    fn extend(&self, valid_until: Instant) {
        self.lock().valid_until = Some(valid_until);
    }

    /// Stops issuing IDs until a renewal succeeds (clock skew).
    fn suspend(&self) {
        self.lock().valid_until = None;
    }

    /// Forgets the worker ID entirely (lease lost or released).
    fn drop_lease(&self) {
        let mut state = self.lock();
        let last = state.last_ms();
        state.worker = None;
        state.valid_until = None;
        state.min_ms = last;
        state.last = None;
    }
}

/// Extends the lease and records the high-water mark, only if the key still
/// holds our instance ID. Returns `{ok, redis seconds, redis microseconds}`:
/// Redis `TIME` comes back in the same round trip, for the skew check.
static RENEW: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
        local t = redis.call('TIME')
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then
          return {0, t[1], t[2]}
        end
        redis.call('PEXPIRE', KEYS[1], ARGV[2])
        redis.call('SET', KEYS[2], ARGV[3], 'EX', 3600)
        return {1, t[1], t[2]}
        ",
    )
});

/// Deletes the lease only if it is still ours.
static RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
        if redis.call('GET', KEYS[1]) == ARGV[1] then
          redis.call('SET', KEYS[2], ARGV[2], 'EX', 3600)
          return redis.call('DEL', KEYS[1])
        end
        return 0
        ",
    )
});

fn lease_key(worker: u16) -> String {
    format!("worker:{worker}")
}

fn hwm_key(worker: u16) -> String {
    format!("worker_hwm:{worker}")
}

/// Keeps a worker-ID lease for the life of the process: acquires one,
/// renews it, re-acquires if it is lost, and releases it when `shutdown`
/// turns true.
pub async fn run_lease(
    generator: std::sync::Arc<IdGenerator>,
    redis: ConnectionManager,
    instance_id: Uuid,
    timings: LeaseTimings,
    mut shutdown: watch::Receiver<bool>,
) {
    let instance = instance_id.to_string();
    'acquire: loop {
        let worker = loop {
            match acquire(&mut redis.clone(), &instance, timings.ttl).await {
                Ok(Some(worker)) => break worker,
                Ok(None) => tracing::error!("all 1024 worker IDs are leased; retrying"),
                Err(err) => tracing::warn!(error = %err, "acquiring a worker ID failed"),
            }
            if wait_or_shutdown(&mut shutdown, Duration::from_secs(1)).await {
                return;
            }
        };
        let hwm: Option<u64> = redis::cmd("GET")
            .arg(hwm_key(worker))
            .query_async(&mut redis.clone())
            .await
            .unwrap_or(None);
        tracing::info!(worker, "acquired worker ID; in quarantine");
        // Not issuing IDs until quarantine clears.
        crate::observe::worker_id_fenced(true);
        // The lease is renewed from the start; quarantine only delays using
        // it. (Waiting out the quarantine without renewing would let the key
        // expire, since the quarantine is as long as the TTL.)
        let usable_from = Instant::now() + timings.quarantine;

        let mut installed = false;
        loop {
            let sent = Instant::now();
            let sent_unix_ms = generator.unix_ms();
            let result: redis::RedisResult<(i64, i64, i64)> = RENEW
                .key(lease_key(worker))
                .key(hwm_key(worker))
                .arg(&instance)
                .arg(timings.ttl.as_millis() as u64)
                .arg(generator.last_ms())
                .invoke_async(&mut redis.clone())
                .await;
            match result {
                Ok((0, _, _)) => {
                    tracing::warn!(worker, "worker ID lease lost; stopped issuing IDs");
                    generator.drop_lease();
                    crate::observe::worker_id_fenced(true);
                    continue 'acquire;
                }
                Ok((_, seconds, micros)) => {
                    let rtt = sent.elapsed();
                    let redis_ms = (seconds * 1000 + micros / 1000) as i128;
                    let local_ms = sent_unix_ms as i128 + (rtt.as_millis() / 2) as i128;
                    let skew_ms = (redis_ms - local_ms).unsigned_abs();
                    let valid_until = sent + timings.ttl.saturating_sub(Duration::from_secs(1));
                    if skew_ms > timings.max_clock_skew.as_millis() {
                        tracing::error!(
                            skew_ms,
                            "clock differs from Redis TIME; stopped issuing IDs"
                        );
                        generator.suspend();
                        crate::observe::worker_id_fenced(true);
                    } else if installed {
                        generator.extend(valid_until);
                        crate::observe::worker_id_fenced(false);
                    } else if Instant::now() >= usable_from {
                        generator.install(worker, hwm, valid_until);
                        installed = true;
                        crate::observe::worker_id_fenced(false);
                        tracing::info!(worker, "issuing message IDs");
                    }
                }
                // The deadline computed at the last success still stands and
                // lapses on its own if Redis stays unreachable.
                Err(err) => tracing::warn!(error = %err, worker, "renewing the worker ID failed"),
            }
            if wait_or_shutdown(&mut shutdown, timings.renew_every).await {
                release(&mut redis.clone(), worker, &instance, &generator).await;
                return;
            }
        }
    }
}

/// Tries every worker ID once, starting at a random one.
async fn acquire(
    redis: &mut ConnectionManager,
    instance: &str,
    ttl: Duration,
) -> redis::RedisResult<Option<u16>> {
    let start: u16 = rand::rng().random_range(0..=MAX_WORKER);
    for offset in 0..=MAX_WORKER {
        let worker = (start + offset) % (MAX_WORKER + 1);
        let acquired: Option<String> = redis::cmd("SET")
            .arg(lease_key(worker))
            .arg(instance)
            .arg("NX")
            .arg("PX")
            .arg(ttl.as_millis() as u64)
            .query_async(redis)
            .await?;
        if acquired.is_some() {
            return Ok(Some(worker));
        }
    }
    Ok(None)
}

async fn release(
    redis: &mut ConnectionManager,
    worker: u16,
    instance: &str,
    generator: &IdGenerator,
) {
    let last_ms = generator.last_ms();
    generator.drop_lease();
    let released: redis::RedisResult<i64> = RELEASE
        .key(lease_key(worker))
        .key(hwm_key(worker))
        .arg(instance)
        .arg(last_ms)
        .invoke_async(redis)
        .await;
    match released {
        Ok(1) => tracing::info!(worker, "released worker ID"),
        Ok(_) => tracing::warn!(worker, "worker ID was no longer ours at release"),
        Err(err) => {
            tracing::warn!(error = %err, worker, "releasing worker ID failed; it expires on its own")
        }
    }
}

/// Sleeps for `duration`; returns true early if shutdown was signalled.
async fn wait_or_shutdown(shutdown: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    tokio::select! {
        () = tokio::time::sleep(duration) => false,
        _ = shutdown.changed() => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leased(worker: u16) -> (State, Instant) {
        let now = Instant::now();
        let state = State {
            worker: Some(worker),
            valid_until: Some(now + Duration::from_secs(10)),
            last: None,
            min_ms: 0,
        };
        (state, now)
    }

    #[test]
    fn ids_increase_and_carry_the_worker() {
        let (mut state, now) = leased(7);
        let mut previous = None;
        for i in 0..10_000u64 {
            // The clock advances every 3 calls.
            let id = state.next(1_000 + i / 3, now).unwrap();
            assert_eq!(id.worker(), 7);
            if let Some(p) = previous {
                assert!(id > p);
            }
            previous = Some(id);
        }
    }

    #[test]
    fn sequence_overflow_borrows_the_next_millisecond() {
        let (mut state, now) = leased(1);
        for seq in 0..=MAX_SEQUENCE {
            let id = state.next(500, now).unwrap();
            assert_eq!((id.ms_since_epoch(), id.sequence()), (500, seq));
        }
        let id = state.next(500, now).unwrap();
        assert_eq!((id.ms_since_epoch(), id.sequence()), (501, 0));
    }

    #[test]
    fn small_backward_steps_are_absorbed_large_ones_refused() {
        let (mut state, now) = leased(1);
        let a = state.next(10_000, now).unwrap();
        // The clock steps back 5 ms: keep counting on the last millisecond.
        let b = state.next(9_995, now).unwrap();
        assert!(b > a);
        assert_eq!(b.ms_since_epoch(), 10_000);
        // 50 ms back: refuse rather than drift far ahead of real time.
        assert_eq!(state.next(9_950, now), Err(IdError::ClockBehind(50)));
        // Once the clock catches up, IDs flow again.
        assert!(state.next(10_001, now).unwrap() > b);
    }

    #[test]
    fn no_ids_without_a_valid_lease() {
        let now = Instant::now();
        let mut state = State {
            worker: None,
            valid_until: None,
            last: None,
            min_ms: 0,
        };
        assert_eq!(state.next(1, now), Err(IdError::NoLease));
        state.worker = Some(3);
        state.valid_until = Some(now);
        // The deadline has passed (now is not before it).
        assert_eq!(state.next(1, now), Err(IdError::NoLease));
        state.valid_until = Some(now + Duration::from_millis(1));
        assert!(state.next(1, now).is_ok());
    }

    #[test]
    fn a_new_holder_starts_after_the_high_water_mark() {
        let generator = IdGenerator::new(0);
        let far_future_hwm = generator.now_ms() + 5;
        generator.install(
            9,
            Some(far_future_hwm),
            Instant::now() + Duration::from_secs(10),
        );
        let id = generator.next_id().unwrap();
        assert!(id.ms_since_epoch() > far_future_hwm);
        assert_eq!(id.worker(), 9);
    }

    #[test]
    fn dropping_the_lease_stops_ids_but_keeps_time_monotonic() {
        let generator = IdGenerator::new(0);
        generator.install(1, None, Instant::now() + Duration::from_secs(10));
        let before = generator.next_id().unwrap();
        generator.drop_lease();
        assert_eq!(generator.next_id(), Err(IdError::NoLease));
        assert!(!generator.is_ready());
        generator.install(2, None, Instant::now() + Duration::from_secs(10));
        assert!(generator.next_id().unwrap().ms_since_epoch() >= before.ms_since_epoch());
    }

    #[test]
    fn clock_offset_shifts_time() {
        let behind = IdGenerator::new(-300);
        let normal = IdGenerator::new(0);
        let gap = normal.now_ms() as i64 - behind.now_ms() as i64;
        assert!((295..=305).contains(&gap), "{gap}");
    }
}
