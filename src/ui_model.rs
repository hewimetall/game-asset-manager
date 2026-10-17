use std::collections::{HashMap, HashSet};
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Idle,
    Scanning,
    Transferring,
    Done,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Upload,
    Download,
}

impl Direction {
    pub fn label(self) -> &'static str {
        match self {
            Self::Upload => "Отправка",
            Self::Download => "Скачивание",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentFile {
    pub rel: String,
    pub direction: Direction,
    pub done: u64,
    pub total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictNote {
    pub rel: String,
    pub reason: String,
}

#[derive(Clone, Debug)]
pub struct UiModel {
    pub phase: Phase,
    pub stand: String,
    pub files_done: u32,
    pub files_total: u32,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub current: Vec<CurrentFile>,
    pub conflicts: Vec<ConflictNote>,
    pub status: String,
    pub waiting_until: Option<Instant>,
    pub finalized: bool,
    pub syncing: bool,
    pub degraded: bool,
    pub keys_ready: bool,
}

impl UiModel {
    pub fn new(stand: impl Into<String>) -> Self {
        Self {
            phase: Phase::Idle,
            stand: stand.into(),
            files_done: 0,
            files_total: 0,
            bytes_done: 0,
            bytes_total: 0,
            current: Vec::new(),
            conflicts: Vec::new(),
            status: String::new(),
            waiting_until: None,
            finalized: false,
            syncing: false,
            degraded: false,
            keys_ready: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct Ledger {
    bytes_done: u64,
    per_file: HashMap<String, u64>,
    seen: HashSet<(String, i32)>,
}

impl Ledger {
    /// Counts each `(rel, token)` once. A retried part uses the same token and does not add bytes again.
    pub fn confirm(&mut self, rel: &str, token: i32, bytes: u64) -> (u64, u64) {
        if self.seen.insert((rel.to_string(), token)) {
            self.bytes_done = self.bytes_done.saturating_add(bytes);
            let file = self.per_file.entry(rel.to_string()).or_insert(0);
            *file = file.saturating_add(bytes);
        }
        (
            self.bytes_done,
            self.per_file.get(rel).copied().unwrap_or(0),
        )
    }

    pub fn bytes_done(&self) -> u64 {
        self.bytes_done
    }
}

pub fn fraction(model: &UiModel) -> Option<f32> {
    match model.phase {
        Phase::Idle | Phase::Scanning => None,
        Phase::Done if model.finalized => Some(1.0),
        Phase::Transferring | Phase::Failed | Phase::Done => {
            if model.bytes_total == 0 {
                Some(0.0)
            } else {
                let raw = model.bytes_done as f32 / model.bytes_total as f32;
                Some(raw.min(0.99))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retried_bytes_do_not_increase_progress_twice() {
        let mut ledger = Ledger::default();
        assert_eq!(ledger.confirm("a", 1, 5_000_000).0, 5_000_000);
        assert_eq!(ledger.confirm("a", 1, 5_000_000).0, 5_000_000);
        assert_eq!(ledger.confirm("a", 2, 100).0, 5_000_100);
        assert_eq!(ledger.confirm("a", 0, 5_000_100).0, 10_000_200);
    }

    #[test]
    fn percent_is_hidden_while_scanning_and_full_only_after_finalize() {
        let mut model = UiModel::new("dayna");
        model.phase = Phase::Scanning;
        model.bytes_done = 10;
        model.bytes_total = 10;
        assert!(fraction(&model).is_none());
        model.phase = Phase::Transferring;
        assert_eq!(fraction(&model), Some(0.99));
        model.phase = Phase::Done;
        model.finalized = true;
        assert_eq!(fraction(&model), Some(1.0));
    }
}
