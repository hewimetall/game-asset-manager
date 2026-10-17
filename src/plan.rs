use crate::paths::normalize_etag;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFile {
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteFile {
    pub size: u64,
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexFile {
    pub size: u64,
    pub sha256: String,
    pub remote_etag: String,
    pub remote_size: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictReason {
    BothChanged,
    NoBaseline,
}

impl ConflictReason {
    pub fn label(self) -> &'static str {
        match self {
            Self::BothChanged => "изменён локально и на сервере",
            Self::NoBaseline => "есть и локально, и на сервере, ещё не синхронизировался",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Skip,
    Upload,
    Download,
    /// Файл удалён на сервере и локальная копия не менялась с прошлой синхронизации.
    DeleteLocal,
    /// Файл удалён локально и серверная копия не менялась с прошлой синхронизации.
    DeleteRemote,
    /// Файла уже нет ни локально, ни на сервере.
    Forget,
    Conflict(ConflictReason),
}

pub fn decide(
    local: Option<&LocalFile>,
    remote: Option<&RemoteFile>,
    index: Option<&IndexFile>,
) -> Action {
    match (local, remote, index) {
        (None, None, None) => Action::Skip,
        (None, None, Some(_)) => Action::Forget,
        (Some(local), None, Some(index)) => {
            if local.sha256 == index.sha256 {
                Action::DeleteLocal
            } else {
                Action::Upload
            }
        }
        (Some(_), None, None) => Action::Upload,
        (None, Some(remote), Some(index)) => {
            if normalize_etag(&remote.etag) == normalize_etag(&index.remote_etag) {
                Action::DeleteRemote
            } else {
                Action::Download
            }
        }
        (None, Some(_), None) => Action::Download,
        // Пары без общей базы отправляются с диска: иначе правка никогда не уходит на сервер.
        (Some(_), Some(_), None) => Action::Upload,
        (Some(local), Some(remote), Some(index)) => {
            let local_same = local.sha256 == index.sha256;
            let remote_same = normalize_etag(&remote.etag) == normalize_etag(&index.remote_etag);
            match (local_same, remote_same) {
                (true, true) => Action::Skip,
                (false, true) => Action::Upload,
                (true, false) => Action::Download,
                (false, false) => Action::Conflict(ConflictReason::BothChanged),
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Planned {
    pub rel: String,
    pub action: Action,
    pub local: Option<LocalFile>,
    pub remote: Option<RemoteFile>,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub items: Vec<Planned>,
    pub skipped: u32,
    pub uploads: u32,
    pub downloads: u32,
    pub deletes: u32,
    pub conflicts: u32,
    pub bytes: u64,
}

pub fn build_plan(
    local: &BTreeMap<String, LocalFile>,
    remote: &BTreeMap<String, RemoteFile>,
    index: &BTreeMap<String, IndexFile>,
) -> Plan {
    let mut names = BTreeMap::<String, ()>::new();
    for key in local.keys().chain(remote.keys()).chain(index.keys()) {
        names.insert(key.clone(), ());
    }
    let mut plan = Plan::default();
    for rel in names.into_keys() {
        let local_file = local.get(&rel);
        let remote_file = remote.get(&rel);
        let index_file = index.get(&rel);
        let action = decide(local_file, remote_file, index_file);
        match &action {
            Action::Skip | Action::Forget => {
                plan.skipped += 1;
                if matches!(action, Action::Forget) {
                    plan.items.push(Planned {
                        rel,
                        action,
                        local: None,
                        remote: None,
                        bytes: 0,
                    });
                }
                continue;
            }
            Action::Upload => plan.uploads += 1,
            Action::Download => plan.downloads += 1,
            Action::DeleteLocal | Action::DeleteRemote => plan.deletes += 1,
            Action::Conflict(_) => plan.conflicts += 1,
        }
        let bytes = match &action {
            Action::Upload => local_file.map(|file| file.size).unwrap_or(0),
            Action::Download => remote_file.map(|file| file.size).unwrap_or(0),
            _ => 0,
        };
        plan.bytes = plan.bytes.saturating_add(bytes);
        plan.items.push(Planned {
            rel,
            action,
            local: local_file.cloned(),
            remote: remote_file.cloned(),
            bytes,
        });
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(sha: &str) -> LocalFile {
        LocalFile {
            size: 10,
            sha256: sha.into(),
        }
    }
    fn remote(etag: &str) -> RemoteFile {
        RemoteFile {
            size: 10,
            etag: etag.into(),
        }
    }
    fn index(sha: &str, etag: &str) -> IndexFile {
        IndexFile {
            size: 10,
            sha256: sha.into(),
            remote_etag: etag.into(),
            remote_size: 10,
        }
    }

    #[test]
    fn plan_matrix() {
        let idx = index("sha", "etag");
        assert_eq!(
            decide(Some(&local("sha")), Some(&remote("\"etag\"")), Some(&idx)),
            Action::Skip
        );
        assert_eq!(
            decide(Some(&local("new")), Some(&remote("etag")), Some(&idx)),
            Action::Upload
        );
        assert_eq!(
            decide(Some(&local("sha")), Some(&remote("etag-2")), Some(&idx)),
            Action::Download
        );
        assert_eq!(
            decide(Some(&local("new")), Some(&remote("other")), Some(&idx)),
            Action::Conflict(ConflictReason::BothChanged)
        );
        assert_eq!(
            decide(Some(&local("sha")), None, Some(&idx)),
            Action::DeleteLocal
        );
        assert_eq!(
            decide(Some(&local("edited")), None, Some(&idx)),
            Action::Upload
        );
        assert_eq!(
            decide(None, Some(&remote("etag")), Some(&idx)),
            Action::DeleteRemote
        );
        assert_eq!(
            decide(None, Some(&remote("etag-2")), Some(&idx)),
            Action::Download
        );
        assert_eq!(decide(Some(&local("sha")), None, None), Action::Upload);
        assert_eq!(decide(None, Some(&remote("etag")), None), Action::Download);
        assert_eq!(
            decide(Some(&local("sha")), Some(&remote("etag")), None),
            Action::Upload
        );
        assert_eq!(decide(None, None, Some(&idx)), Action::Forget);
    }

    #[test]
    fn multipart_etag_is_only_a_version_string() {
        let idx = index("sha256-of-bytes", "abc123-5");
        assert_eq!(
            decide(
                Some(&local("sha256-of-bytes")),
                Some(&remote("abc123-5")),
                Some(&idx)
            ),
            Action::Skip
        );
        assert_ne!("sha256-of-bytes", "abc123-5");
    }

    #[test]
    fn size_is_not_the_decision() {
        let mut idx = index("sha", "etag");
        idx.size = 1;
        idx.remote_size = 2;
        let mut local = local("sha");
        local.size = 99;
        let mut remote = remote("etag");
        remote.size = 100;
        assert_eq!(
            decide(Some(&local), Some(&remote), Some(&idx)),
            Action::Skip
        );
    }

    #[test]
    fn build_counts_bytes_without_conflicts() {
        let mut local_files = BTreeMap::new();
        local_files.insert(
            "a.txt".into(),
            LocalFile {
                size: 5,
                sha256: "l".into(),
            },
        );
        local_files.insert(
            "b.txt".into(),
            LocalFile {
                size: 7,
                sha256: "same".into(),
            },
        );
        let mut remote_files = BTreeMap::new();
        remote_files.insert(
            "b.txt".into(),
            RemoteFile {
                size: 7,
                etag: "r".into(),
            },
        );
        remote_files.insert(
            "c.txt".into(),
            RemoteFile {
                size: 11,
                etag: "c".into(),
            },
        );
        let plan = build_plan(&local_files, &remote_files, &BTreeMap::new());
        assert_eq!(plan.uploads, 2);
        assert_eq!(plan.downloads, 1);
        assert_eq!(plan.conflicts, 0);
        assert_eq!(plan.bytes, 23);
        assert!(plan
            .items
            .iter()
            .any(|item| item.rel == "b.txt" && item.action == Action::Upload));
    }
}
