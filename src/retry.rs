use std::time::Duration;

pub const MAX_ATTEMPTS: u32 = 8;

/// Exponential pause with jitter. Attempt is the number of failures so far, starting at 1.
pub fn backoff_delay(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(1).min(6);
    let base_ms = (1_000u64 << exp).min(60_000);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or(1);
    let mix = nanos
        .wrapping_mul(1_664_525)
        .wrapping_add(attempt.wrapping_mul(1_013_904_223));
    let span = (base_ms / 5).max(1);
    let jitter = u64::from(mix) % (span * 2 + 1);
    let ms = base_ms - span + jitter;
    Duration::from_millis(ms.min(72_000))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_grows_then_caps() {
        let first = backoff_delay(1);
        assert!(first >= Duration::from_millis(700) && first <= Duration::from_millis(1_300));
        let later = backoff_delay(20);
        assert!(later <= Duration::from_millis(72_000));
        assert!(later >= Duration::from_secs(40));
    }
}
