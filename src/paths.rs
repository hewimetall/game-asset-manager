use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

pub const SQLITE_NAME: &str = ".stand-sync.sqlite";

pub fn exe_directory() -> Result<PathBuf, String> {
    let exe = std::env::current_exe()
        .map_err(|err| format!("не удалось определить путь к программе: {err}"))?;
    let resolved = fs::canonicalize(&exe).unwrap_or(exe);
    let resolved = strip_verbatim(&resolved);
    resolved
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "у программы нет папки".to_string())
}

pub fn strip_verbatim(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{rest}"))
    } else if let Some(rest) = text.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        path.to_path_buf()
    }
}

pub fn exe_file_name(dir: &Path) -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| path.file_name().map(|n| n.to_string_lossy().into_owned()))
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| {
            let _ = dir;
            "sync.exe".to_string()
        })
}

/// Read, write and rename inside `.stand-sync/`, which is never synced.
pub fn probe_workdir(dir: &Path) -> Result<(), String> {
    let base = dir.join(".stand-sync");
    fs::create_dir_all(&base)
        .map_err(|err| format!("папка программы недоступна для записи: {err}"))?;
    let src = base.join("probe-src");
    let dst = base.join("probe-dst");
    fs::write(&src, b"ok")
        .map_err(|err| format!("папка программы недоступна для записи: {err}"))?;
    let _ = fs::remove_file(&dst);
    fs::rename(&src, &dst)
        .map_err(|err| format!("в папке программы нельзя переименовывать файлы: {err}"))?;
    let data =
        fs::read(&dst).map_err(|err| format!("папка программы недоступна для чтения: {err}"))?;
    let _ = fs::remove_file(&dst);
    if data != b"ok" {
        return Err("проверка чтения и записи в папке программы не прошла".into());
    }
    Ok(())
}

pub fn is_excluded_rel(rel: &str, exe_name: &str) -> bool {
    if rel.is_empty() {
        return true;
    }
    let lower = rel.to_ascii_lowercase();
    if lower == ".stand-sync" || lower.starts_with(".stand-sync/") {
        return true;
    }
    if rel.contains('/') || rel.contains('\\') {
        return false;
    }
    let exe = exe_name.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "config.toml"
            | "logs.txt"
            | "logs.txt.1"
            | "logs.txt.2"
            | ".stand-sync.sqlite"
            | ".stand-sync.sqlite-wal"
            | ".stand-sync.sqlite-shm"
            | ".stand-sync.sqlite-journal"
    ) || lower == exe
}

#[derive(Debug, PartialEq, Eq)]
pub enum KeyMap {
    File(String),
    Ignore,
    Reject(String),
}

pub fn map_remote_key(prefix: &str, key: &str) -> KeyMap {
    if prefix.is_empty() || !prefix.ends_with('/') {
        return KeyMap::Reject("пустой префикс".into());
    }
    let Some(rest) = key.strip_prefix(prefix) else {
        return KeyMap::Reject("ключ вне префикса".into());
    };
    if rest.is_empty() || rest.ends_with('/') {
        return KeyMap::Ignore;
    }
    if rest.contains('\\') || rest.starts_with('/') {
        return KeyMap::Reject(rest.to_string());
    }
    let mut rel = String::new();
    for part in rest.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return KeyMap::Reject(rest.to_string());
        }
        if !rel.is_empty() {
            rel.push('/');
        }
        rel.push_str(part);
    }
    KeyMap::File(rel)
}

pub fn local_target(workdir: &Path, rel: &str) -> Result<PathBuf, String> {
    if rel.is_empty() {
        return Err("пустой путь".into());
    }
    let mut out = workdir.to_path_buf();
    for part in rel.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(format!("недопустимый путь: {rel}"));
        }
        if part.contains('\\') {
            return Err(format!("недопустимый путь: {rel}"));
        }
        out.push(part);
    }
    if !out.starts_with(workdir) {
        return Err(format!("путь выходит за рабочую папку: {rel}"));
    }
    Ok(out)
}

pub fn to_rel(root: &Path, path: &Path) -> Option<String> {
    let rel = path.strip_prefix(root).ok()?;
    let mut out = String::new();
    for component in rel.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str()?;
                if part.is_empty() || part == "." || part == ".." {
                    return None;
                }
                if !out.is_empty() {
                    out.push('/');
                }
                out.push_str(part);
            }
            _ => return None,
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

pub fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_encode(&hasher.finalize()))
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn normalize_etag(raw: &str) -> String {
    let trimmed = raw.trim();
    let stripped = trimmed.strip_prefix("W/").unwrap_or(trimmed).trim();
    stripped.trim_matches('"').to_string()
}

pub fn fmt_mib(bytes: u64) -> String {
    format!("{:.1}", bytes as f64 / (1024.0 * 1024.0))
}

pub fn replace_file(from: &Path, to: &Path) -> io::Result<()> {
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }
    replace_file_same_volume(from, to)
}

#[cfg(windows)]
fn replace_file_same_volume(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };
    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }
    let from_w = wide(from);
    let to_w = wide(to);
    let ok = unsafe {
        MoveFileExW(
            from_w.as_ptr(),
            to_w.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn replace_file_same_volume(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_prefix_and_rejects_escape() {
        let prefix = "stand-dayna/";
        assert_eq!(
            map_remote_key(prefix, "stand-dayna/sprites/queen.png"),
            KeyMap::File("sprites/queen.png".into())
        );
        assert_eq!(map_remote_key(prefix, "stand-dayna/"), KeyMap::Ignore);
        assert_eq!(
            map_remote_key(prefix, "stand-dayna/sprites/"),
            KeyMap::Ignore
        );
        assert!(matches!(
            map_remote_key(prefix, "stand-dayna/../secret"),
            KeyMap::Reject(_)
        ));
        assert!(matches!(
            map_remote_key(prefix, "stand-dayna/sprites/../config.toml"),
            KeyMap::Reject(_)
        ));
        assert!(matches!(
            map_remote_key(prefix, "other/sprites/queen.png"),
            KeyMap::Reject(_)
        ));
    }

    #[test]
    fn service_names_are_excluded_only_at_root() {
        let exe = "sync.exe";
        assert!(is_excluded_rel("sync.exe", exe));
        assert!(is_excluded_rel("SYNC.EXE", exe));
        assert!(is_excluded_rel("config.toml", exe));
        assert!(is_excluded_rel("logs.txt", exe));
        assert!(is_excluded_rel("logs.txt.1", exe));
        assert!(is_excluded_rel("logs.txt.2", exe));
        assert!(is_excluded_rel(".stand-sync.sqlite", exe));
        assert!(is_excluded_rel(".stand-sync.sqlite-wal", exe));
        assert!(is_excluded_rel(".stand-sync.sqlite-shm", exe));
        assert!(is_excluded_rel(".stand-sync/snapshots/a", exe));
        assert!(!is_excluded_rel("sprites/queen.png", exe));
        assert!(!is_excluded_rel("sprites/config.toml", exe));
        assert!(!is_excluded_rel("data/logs.txt", exe));
    }

    #[test]
    fn target_stays_inside_workdir() {
        let root = Path::new("/work");
        assert_eq!(
            local_target(root, "sprites/queen.png").unwrap(),
            PathBuf::from("/work/sprites/queen.png")
        );
        assert!(local_target(root, "../secret").is_err());
        assert!(local_target(root, "sprites/../../secret").is_err());
    }

    #[test]
    fn etag_quotes_are_stripped() {
        assert_eq!(normalize_etag("\"abc\""), "abc");
        assert_eq!(normalize_etag("W/\"abc-2\""), "abc-2");
    }

    #[test]
    fn probe_checks_write_and_rename() {
        let dir = crate::testutil::temp_dir("probe");
        probe_workdir(&dir).unwrap();
        assert!(!dir.join(".stand-sync").join("probe-src").exists());
        assert!(!dir.join(".stand-sync").join("probe-dst").exists());
    }
}
