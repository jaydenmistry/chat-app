//! Token-bucket rate limiting (docs/design.md 11.1).
//!
//! Every limit is a token bucket held in Redis, so all instances share one
//! bucket per subject and one clock (Redis `TIME`). A bucket has a capacity
//! (the burst) and a refill rate (tokens per second); each allowed request
//! costs one token. The maths lives once, in [`BUCKET_LUA`], and is run inside
//! a Redis script so the check-and-consume can't interleave with another
//! instance.
//!
//! A limit that applies to several subjects at once (a send is limited per
//! user, per IP and per room) is all-or-nothing: the script consumes a token
//! from every bucket only if every bucket has one, so a request rejected by
//! one limit doesn't drain the others.
//!
//! The send limit is folded into the idempotency-claim script
//! (`realtime::send`) instead of called separately, so a message is never
//! both rate-limited and claimed, and a retry of an already-claimed message
//! is never charged again. Everything else uses [`enforce`].

use std::sync::LazyLock;

use redis::{Script, aio::ConnectionManager};

use crate::http::error::AppError;

/// A single token bucket: `capacity` is the burst, `refill_per_sec` the
/// steady rate. Both are config variables (one `RATE_*` per limit).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket {
    pub capacity: f64,
    pub refill_per_sec: f64,
}

impl Bucket {
    /// A burst of `capacity`, refilling at `per_sec` tokens a second.
    pub const fn per_second(capacity: f64, per_sec: f64) -> Self {
        Self {
            capacity,
            refill_per_sec: per_sec,
        }
    }

    /// `count` actions per `period_secs`: capacity `count`, refilled so the
    /// bucket is full again one period after it empties.
    pub fn per_period(count: f64, period_secs: f64) -> Self {
        Self {
            capacity: count,
            refill_per_sec: count / period_secs,
        }
    }
}

/// Every limit in the system, built once from the environment. Defaults are
/// the starting values in docs/design.md 11.1.
#[derive(Debug, Clone)]
pub struct RateLimits {
    pub send_user: Bucket,
    pub send_ip: Bucket,
    pub send_room: Bucket,
    pub mutate_user: Bucket,
    pub signup_ip: Bucket,
    pub guest_ip: Bucket,
    pub login_ip: Bucket,
    pub login_account: Bucket,
    pub room_create_user: Bucket,
    pub join_user: Bucket,
    pub join_guest: Bucket,
}

/// Consumes one token from each of several buckets, atomically, iff every
/// bucket has at least one. Parallel arrays `keys`, `caps`, `refills`; `now`
/// is the current time in seconds (a float). Returns true when it consumed.
///
/// Each bucket is a hash of `t` (tokens left) and `s` (the second `t` was
/// computed at). A missing bucket starts full. The key expires once it would
/// refill to full, so idle subjects leave nothing behind.
const BUCKET_LUA: &str = r"
local function rl_take(keys, caps, refills, now)
  local toks = {}
  for i = 1, #keys do
    local d = redis.call('HMGET', keys[i], 't', 's')
    local tk = tonumber(d[1])
    local ts = tonumber(d[2])
    if tk == nil then tk = caps[i]; ts = now end
    tk = math.min(caps[i], tk + (now - ts) * refills[i])
    if tk < 1 then return false end
    toks[i] = tk
  end
  for i = 1, #keys do
    local tk = toks[i] - 1
    redis.call('HSET', keys[i], 't', tk, 's', now)
    redis.call('PEXPIRE', keys[i], math.ceil((caps[i] - tk) / refills[i]) * 1000 + 1000)
  end
  return true
end
";

/// The generic limiter: KEYS are the bucket keys, ARGV the `capacity, refill`
/// pair for each, in the same order. Returns 1 if allowed, 0 if not.
static LIMIT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(&format!(
        "{BUCKET_LUA}
        local t = redis.call('TIME')
        local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
        local keys, caps, refills = {{}}, {{}}, {{}}
        for i = 1, #KEYS do
          keys[i] = KEYS[i]
          caps[i] = tonumber(ARGV[(i - 1) * 2 + 1])
          refills[i] = tonumber(ARGV[(i - 1) * 2 + 2])
        end
        if rl_take(keys, caps, refills, now) then return 1 else return 0 end
        "
    ))
});

/// The Lua `rl_take` definition, for scripts that embed the token bucket (the
/// send path's claim script).
pub(crate) const TAKE_DEF: &str = BUCKET_LUA;

/// Checks (and, if allowed, consumes) the given buckets together. On a Redis
/// error the caller decides; most callers fail closed.
pub async fn check(
    redis: &mut ConnectionManager,
    buckets: &[(String, Bucket)],
) -> redis::RedisResult<bool> {
    if buckets.is_empty() {
        return Ok(true);
    }
    let mut invocation = LIMIT.prepare_invoke();
    for (key, _) in buckets {
        invocation.key(key);
    }
    for (_, bucket) in buckets {
        invocation.arg(bucket.capacity).arg(bucket.refill_per_sec);
    }
    let allowed: i64 = invocation.invoke_async(redis).await?;
    Ok(allowed == 1)
}

/// Checks `buckets` and turns a rejection into a 429 for REST handlers. A
/// Redis failure becomes `unavailable` (fail closed): the limiter is part of
/// the same Redis the rest of the request needs anyway.
pub async fn enforce(
    redis: &mut ConnectionManager,
    action: &'static str,
    message: &'static str,
    buckets: &[(String, Bucket)],
) -> Result<(), AppError> {
    if check(redis, buckets).await.map_err(AppError::from)? {
        Ok(())
    } else {
        record_rejection(action);
        Err(AppError::TooManyRequests(message))
    }
}

/// Counts a rejection for metrics (docs/design.md 13:
/// `rate_limit_rejections_total{action}`). The Prometheus recorder is wired in
/// the metrics piece of Phase 4; until then this records the rejection in the
/// logs and keeps every call site in place.
pub fn record_rejection(action: &str) {
    tracing::debug!(action, "rate limited");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_period_refills_over_the_window() {
        let b = Bucket::per_period(10.0, 3600.0);
        assert_eq!(b.capacity, 10.0);
        // Ten tokens an hour.
        assert!((b.refill_per_sec - 10.0 / 3600.0).abs() < f64::EPSILON);
    }

    #[test]
    fn per_second_keeps_burst_and_rate() {
        let b = Bucket::per_second(30.0, 3.0);
        assert_eq!(b.capacity, 30.0);
        assert_eq!(b.refill_per_sec, 3.0);
    }
}
