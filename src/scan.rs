use crate::paths::{is_excluded_rel, sha256_file, to_rel};
use crate::plan::LocalFile;
use std::collections::BTreeMap;
use std::path::Path;
use walkdir::WalkDir;

#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error("обход {path}: {source}")]
    Walk {
        path: String,
        source: walkdir::Error,
    },
    #[error("не удалось прочитать {path}: {source}")]
    Hash {
        path: String,
        source: std::io::Error,
    },
    #[error("имя файла не в UTF-8: {0}")]
    Name(String),
}

pub fn scan_local(root: &Path, exe_name: &str) -> Result<BTreeMap<String, LocalFile>, ScanError> {
    let mut out = BTreeMap::new();
    let walker = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| {
            if entry.depth() == 0 {
                return true;
            }
            if entry.depth() == 1
                && entry
                    .file_name()
                    .to_string_lossy()
                    .eq_ignore_ascii_case(".stand-sync")
            {
                return false;
            }
            true
        });
    for entry in walker {
        let entry = entry.map_err(|err| ScanError::Walk {
            path: root.display().to_string(),
            source: err,
        })?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let rel = to_rel(root, path).ok_or_else(|| ScanError::Name(path.display().to_string()))?;
        if is_excluded_rel(&rel, exe_name) {
            continue;
        }
        let metadata = entry.metadata().map_err(|err| ScanError::Walk {
            path: path.display().to_string(),
            source: err,
        })?;
        let sha256 = sha256_file(path).map_err(|err| ScanError::Hash {
            path: rel.clone(),
            source: err,
        })?;
        out.insert(
            rel,
            LocalFile {
                size: metadata.len(),
                sha256,
            },
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn skips_service_files_and_hashes_content() {
        let dir = crate::testutil::temp_dir("scan");
        fs::write(dir.join("config.toml"), b"config").unwrap();
        fs::write(dir.join("logs.txt"), b"log").unwrap();
        fs::write(dir.join("logs.txt.1"), b"old").unwrap();
        fs::write(dir.join("sync.exe"), b"binary").unwrap();
        fs::write(dir.join(".stand-sync.sqlite"), b"db").unwrap();
        fs::write(dir.join(".stand-sync.sqlite-wal"), b"wal").unwrap();
        fs::create_dir_all(dir.join(".stand-sync").join("snapshots")).unwrap();
        fs::write(
            dir.join(".stand-sync").join("snapshots").join("hidden"),
            b"nope",
        )
        .unwrap();
        fs::create_dir_all(dir.join("sprites")).unwrap();
        fs::write(dir.join("sprites").join("queen.png"), b"queen").unwrap();
        fs::create_dir_all(dir.join("data")).unwrap();
        fs::write(dir.join("data").join("note.txt"), b"note").unwrap();

        let files = scan_local(&dir, "sync.exe").unwrap();
        let mut names: Vec<_> = files.keys().cloned().collect();
        names.sort();
        assert_eq!(
            names,
            vec!["data/note.txt".to_string(), "sprites/queen.png".to_string()]
        );
        assert_eq!(
            files["sprites/queen.png"].sha256,
            crate::paths::sha256_bytes(b"queen")
        );
        assert_ne!(
            files["sprites/queen.png"].sha256,
            files["data/note.txt"].sha256
        );
    }
}
