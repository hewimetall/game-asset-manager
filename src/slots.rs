use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use tokio::sync::Notify;

#[derive(Debug)]
struct Inner {
    max: usize,
    inflight: usize,
    files: HashMap<String, usize>,
    net_fails: u32,
    oks: u32,
    stopped: bool,
}

pub struct Slots {
    inner: Mutex<Inner>,
    notify: Notify,
}

pub struct Permit {
    slots: std::sync::Arc<Slots>,
    rel: String,
    armed: bool,
}

impl Permit {
    pub fn release_ok(mut self) {
        self.slots.note_ok();
        self.slots.release_only(&self.rel);
        self.armed = false;
    }

    pub fn release_network(mut self) {
        self.slots.note_network_error();
        self.slots.release_only(&self.rel);
        self.armed = false;
    }

    pub fn release_neutral(mut self) {
        self.slots.release_only(&self.rel);
        self.armed = false;
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.armed {
            self.slots.release_only(&self.rel);
        }
    }
}

impl Slots {
    pub fn new() -> Self {
        Self::with_max(2)
    }

    pub fn with_max(max: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                max: max.clamp(1, 2),
                inflight: 0,
                files: HashMap::new(),
                net_fails: 0,
                oks: 0,
                stopped: false,
            }),
            notify: Notify::new(),
        }
    }

    pub fn max(&self) -> usize {
        self.lock().max
    }

    pub fn inflight(&self) -> usize {
        self.lock().inflight
    }

    pub fn stopped(&self) -> bool {
        self.lock().stopped
    }

    pub fn stop(&self) {
        self.lock().stopped = true;
        self.notify.notify_waiters();
    }

    pub fn try_acquire(self: &std::sync::Arc<Self>, rel: &str) -> Result<Option<Permit>, ()> {
        let mut guard = self.lock();
        if guard.stopped {
            return Err(());
        }
        let max = guard.max.clamp(1, 2);
        let this = guard.files.get(rel).copied().unwrap_or(0);
        let active = guard.files.values().filter(|count| **count > 0).count();
        let others = active - usize::from(this > 0);
        if this == 0 && others >= 2 {
            return Ok(None);
        }
        let per_file = if others == 0 { max } else { 1 };
        if this >= per_file || guard.inflight >= max {
            return Ok(None);
        }
        *guard.files.entry(rel.to_string()).or_insert(0) += 1;
        guard.inflight += 1;
        drop(guard);
        Ok(Some(Permit {
            slots: std::sync::Arc::clone(self),
            rel: rel.to_string(),
            armed: true,
        }))
    }

    pub async fn acquire(self: &std::sync::Arc<Self>, rel: &str) -> Result<Permit, ()> {
        loop {
            if let Some(permit) = self.try_acquire(rel)? {
                return Ok(permit);
            }
            self.notify.notified().await;
        }
    }

    fn note_ok(&self) {
        let mut guard = self.lock();
        guard.net_fails = 0;
        guard.oks = guard.oks.saturating_add(1);
        if guard.oks >= 8 && guard.max == 1 {
            guard.max = 2;
            guard.oks = 0;
            tracing::info!(from = 1, to = 2, "параллелизм восстановлен до 2");
        }
    }

    pub fn observe_network_error(&self) {
        self.note_network_error();
    }

    fn note_network_error(&self) {
        let mut guard = self.lock();
        guard.oks = 0;
        guard.net_fails = guard.net_fails.saturating_add(1);
        if guard.net_fails >= 3 && guard.max == 2 {
            guard.max = 1;
            guard.net_fails = 0;
            tracing::info!(from = 2, to = 1, "параллелизм снижен до 1");
        }
    }

    fn release_only(&self, rel: &str) {
        let mut guard = self.lock();
        if let Some(count) = guard.files.get_mut(rel) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                guard.files.remove(rel);
            }
        }
        guard.inflight = guard.inflight.saturating_sub(1);
        drop(guard);
        self.notify.notify_waiters();
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|err| err.into_inner())
    }
}

impl Default for Slots {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn one_file_can_take_both_slots() {
        let slots = Arc::new(Slots::new());
        let first = slots.try_acquire("a").unwrap().unwrap();
        let second = slots.try_acquire("a").unwrap().unwrap();
        assert!(slots.try_acquire("a").unwrap().is_none());
        assert!(slots.try_acquire("b").unwrap().is_none());
        assert_eq!(slots.inflight(), 2);
        drop(first);
        drop(second);
    }

    #[test]
    fn two_files_get_one_slot_each_and_a_third_waits() {
        let slots = Arc::new(Slots::new());
        let a = slots.try_acquire("a").unwrap().unwrap();
        let b = slots.try_acquire("b").unwrap().unwrap();
        assert!(slots.try_acquire("a").unwrap().is_none());
        assert!(slots.try_acquire("b").unwrap().is_none());
        assert!(slots.try_acquire("c").unwrap().is_none());
        a.release_ok();
        assert!(slots.try_acquire("c").unwrap().is_some());
        let _ = b;
    }

    #[test]
    fn network_errors_drop_to_one_then_recover() {
        let slots = Arc::new(Slots::new());
        for _ in 0..3 {
            let permit = slots.try_acquire("a").unwrap().unwrap();
            permit.release_network();
        }
        assert_eq!(slots.max(), 1);
        let permit = slots.try_acquire("a").unwrap().unwrap();
        assert!(slots.try_acquire("b").unwrap().is_none());
        permit.release_neutral();
        for _ in 0..8 {
            let permit = slots.try_acquire("a").unwrap().unwrap();
            permit.release_ok();
        }
        assert_eq!(slots.max(), 2);
        assert!(slots.try_acquire("a").unwrap().is_some());
        assert!(slots.try_acquire("b").unwrap().is_some());
    }
}
