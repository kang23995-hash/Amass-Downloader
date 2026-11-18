use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Async rate limiter based on GCRA (virtual-time leaky bucket).
///
/// Each `acquire(n)` reserves `n` bytes of capacity in lock order and sleeps
/// until its reservation is due. This caps the long-term aggregate rate
/// exactly and is fair between competing workers. A single shared instance
/// enforces a global cap across all tasks; per-task instances are independent.
pub struct RateLimiter {
    state: Mutex<State>,
}

struct State {
    rate_bps: u64,
    burst_bytes: u64,
    tat: Instant,
}

impl RateLimiter {
    /// `rate_bps == 0` means unlimited.
    pub fn new(rate_bps: u64) -> Self {
        Self {
            state: Mutex::new(State {
                rate_bps,
                burst_bytes: burst_for(rate_bps),
                tat: Instant::now(),
            }),
        }
    }

    pub fn set_rate(&self, rate_bps: u64) {
        let mut s = match self.state.lock() {
            Ok(s) => s,
            Err(p) => p.into_inner(),
        };
        if s.rate_bps == 0 || rate_bps == 0 {
            s.tat = Instant::now();
        }
        s.rate_bps = rate_bps;
        s.burst_bytes = burst_for(rate_bps);
    }

    pub fn rate(&self) -> u64 {
        self.state.lock().map(|s| s.rate_bps).unwrap_or(0)
    }

    /// Wait until `n` bytes may be consumed. Cancellation-safe: dropping the
    /// future forfeits the reservation, which only relaxes the cap slightly.
    pub async fn acquire(&self, n: u64) {
        if n == 0 {
            return;
        }
        let wait = {
            let mut s = match self.state.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            if s.rate_bps == 0 {
                return;
            }
            let now = Instant::now();
            let tat = s.tat.max(now);
            let increment = duration_for_bytes(n, s.rate_bps);
            let tolerance = duration_for_bytes(s.burst_bytes, s.rate_bps);
            let new_tat = tat + increment;
            s.tat = new_tat;
            let allow_at = new_tat.checked_sub(tolerance).unwrap_or(now);
            allow_at.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

fn burst_for(rate_bps: u64) -> u64 {
    // 100ms worth of traffic, at least 64 KiB so single chunks flow smoothly.
    (rate_bps / 10).max(64 * 1024)
}

fn duration_for_bytes(bytes: u64, rate_bps: u64) -> Duration {
    // nanos = bytes * 1e9 / rate, computed in u128 to avoid overflow.
    let nanos = (bytes as u128 * 1_000_000_000u128) / rate_bps as u128;
    Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unlimited_is_instant() {
        let l = RateLimiter::new(0);
        let start = Instant::now();
        for _ in 0..100 {
            l.acquire(1024 * 1024).await;
        }
        assert!(start.elapsed() < Duration::from_millis(200));
    }

    #[tokio::test]
    async fn caps_aggregate_rate() {
        let rate = 200 * 1024u64; // 200 KiB/s
        let l = RateLimiter::new(rate);
        let total = 400 * 1024u64;
        let start = Instant::now();
        let mut done = 0;
        while done < total {
            let n = 16 * 1024;
            l.acquire(n).await;
            done += n;
        }
        let elapsed = start.elapsed();
        // (total - burst) / rate is the theoretical minimum.
        let min = Duration::from_millis(1500);
        assert!(elapsed >= min, "too fast: {elapsed:?}");
        assert!(elapsed < Duration::from_secs(5), "too slow: {elapsed:?}");
    }

    #[tokio::test]
    async fn shared_instance_caps_two_consumers() {
        let rate = 200 * 1024u64;
        let l = std::sync::Arc::new(RateLimiter::new(rate));
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..2 {
            let l = l.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..16 {
                    l.acquire(16 * 1024).await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // 512 KiB total at 200 KiB/s must take ~2s, not ~1s.
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(1900),
            "global cap leaked: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn dynamic_rate_change() {
        let l = RateLimiter::new(64 * 1024);
        l.acquire(64 * 1024).await;
        l.set_rate(0);
        let start = Instant::now();
        for _ in 0..50 {
            l.acquire(64 * 1024).await;
        }
        assert!(start.elapsed() < Duration::from_millis(200));
        assert_eq!(l.rate(), 0);
    }
}
