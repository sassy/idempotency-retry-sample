//! Client-side retry helper: exponential backoff + jitter, with a maximum
//! attempt count and an overall deadline.
//!
//! Policy rules demonstrated here:
//! - Retry only when the caller's `is_retryable` predicate says so
//!   (e.g. timeouts and 5xx: retry; 4xx: do not retry — the request itself
//!   is wrong, so sending it again changes nothing).
//! - Wait between attempts with exponential backoff (base, 2×base, 4×base…),
//!   capped at `max_delay`, with jitter applied so that many clients do not
//!   retry in lock-step (thundering herd).
//! - Give up when either `max_attempts` is reached or the next sleep would
//!   push the total elapsed time past `deadline`.
//!
//! Retrying is only safe because the request carries an `Idempotency-Key`:
//! if the first attempt actually succeeded on the server but its response
//! was lost, the retry replays the stored result instead of charging twice.

use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total number of attempts, including the first one.
    pub max_attempts: usize,
    /// Delay before the first retry; doubled on each subsequent retry.
    pub base_delay: Duration,
    /// Upper bound for a single (pre-jitter) delay.
    pub max_delay: Duration,
    /// Overall budget from the first attempt.
    pub deadline: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
            deadline: Duration::from_secs(10),
        }
    }
}

impl RetryPolicy {
    /// Backoff for the retry that follows attempt `attempt` (1-based):
    /// base_delay * 2^(attempt-1), capped at max_delay.
    fn backoff(&self, attempt: usize) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1) as u32);
        std::cmp::min(self.base_delay.saturating_mul(factor), self.max_delay)
    }
}

/// A simple "full jitter"-style function: returns a value in
/// `[delay/2, delay]`, derived from the current time. Tests do not use this;
/// they inject a deterministic jitter instead (see `retry_with`).
pub fn simple_jitter(delay: Duration, _attempt: usize) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let half = delay / 2;
    if delay.is_zero() {
        return delay;
    }
    half + Duration::from_nanos(nanos % (half.as_nanos() as u64 + 1))
}

/// Run `operation` until it succeeds, a non-retryable error occurs,
/// `max_attempts` is exhausted, or the deadline would be exceeded.
///
/// - `operation(attempt)` is called with the 1-based attempt number.
/// - `is_retryable(&error)` decides whether another attempt is worthwhile.
/// - `jitter(delay, attempt)` transforms each backoff delay; tests pass a
///   deterministic function (e.g. identity), production code can pass
///   [`simple_jitter`].
pub async fn retry_with<T, E, Op, Fut, P, J>(
    policy: &RetryPolicy,
    is_retryable: P,
    mut jitter: J,
    mut operation: Op,
) -> Result<T, E>
where
    Op: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: Fn(&E) -> bool,
    J: FnMut(Duration, usize) -> Duration,
{
    let started = Instant::now();
    let mut attempt = 1usize;
    loop {
        match operation(attempt).await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt >= policy.max_attempts || !is_retryable(&err) {
                    return Err(err);
                }
                let delay = jitter(policy.backoff(attempt), attempt);
                // Respect the overall deadline: do not sleep past it.
                if started.elapsed() + delay > policy.deadline {
                    return Err(err);
                }
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

/// Convenience wrapper using [`simple_jitter`].
pub async fn retry<T, E, Op, Fut, P>(
    policy: &RetryPolicy,
    is_retryable: P,
    operation: Op,
) -> Result<T, E>
where
    Op: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: Fn(&E) -> bool,
{
    retry_with(policy, is_retryable, simple_jitter, operation).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Tiny, deterministic policy so tests run in milliseconds.
    fn fast_policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 4,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(4),
            deadline: Duration::from_secs(5),
        }
    }

    fn identity_jitter(delay: Duration, _attempt: usize) -> Duration {
        delay
    }

    /// Fake HTTP-ish error: 5xx and timeouts are retryable, 4xx is not.
    #[derive(Debug, PartialEq)]
    enum FakeError {
        Status(u16),
        Timeout,
    }

    fn is_retryable(err: &FakeError) -> bool {
        matches!(err, FakeError::Timeout) || matches!(err, FakeError::Status(500..=599))
    }

    #[tokio::test]
    async fn retries_retryable_errors_and_eventually_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_op = calls.clone();
        let result = retry_with(&fast_policy(), is_retryable, identity_jitter, move |_attempt| {
            let calls = calls_in_op.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    Err(FakeError::Status(503))
                } else {
                    Ok("ok")
                }
            }
        })
        .await;
        assert_eq!(result, Ok("ok"));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_retry_non_retryable_errors() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_op = calls.clone();
        let result: Result<(), FakeError> =
            retry_with(&fast_policy(), is_retryable, identity_jitter, move |_attempt| {
                let calls = calls_in_op.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(FakeError::Status(422))
                }
            })
            .await;
        assert_eq!(result, Err(FakeError::Status(422)));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "4xx must not be retried");
    }

    #[tokio::test]
    async fn stops_at_max_attempts() {
        let policy = RetryPolicy {
            max_attempts: 3,
            ..fast_policy()
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_op = calls.clone();
        let result: Result<(), FakeError> =
            retry_with(&policy, is_retryable, identity_jitter, move |_attempt| {
                let calls = calls_in_op.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(FakeError::Timeout)
                }
            })
            .await;
        assert_eq!(result, Err(FakeError::Timeout));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
