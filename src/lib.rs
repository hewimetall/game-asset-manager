pub mod config;
pub mod credentials;
pub mod index;
pub mod limits;
pub mod lock;
pub mod logsetup;
pub mod pass;
pub mod paths;
pub mod plan;
pub mod retry;
pub mod s3;
pub mod scan;
pub mod slots;
pub mod store;
pub mod transfer;
pub mod ui_model;

#[cfg(test)]
pub mod testutil {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    pub fn temp_dir(name: &str) -> PathBuf {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("stand-sync-{name}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}
