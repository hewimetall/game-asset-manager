use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

pub const LOG_NAME: &str = "logs.txt";
pub const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Debug)]
pub struct LogState {
    pub active: Mutex<Vec<String>>,
    pub recent_errors: Mutex<VecDeque<String>>,
    pub secrets: Mutex<Vec<String>>,
    pub version: String,
    pub os: String,
}

impl LogState {
    pub fn new() -> Self {
        Self {
            active: Mutex::new(Vec::new()),
            recent_errors: Mutex::new(VecDeque::new()),
            secrets: Mutex::new(Vec::new()),
            version: env!("CARGO_PKG_VERSION").to_string(),
            os: os_label(),
        }
    }

    pub fn remember_secret(&self, value: &str) {
        if value.len() >= 8 {
            self.lock_secrets().push(value.to_string());
        }
    }

    pub fn push_error(&self, line: &str) {
        let secrets = self.lock_secrets().clone();
        let clean = redact(line, &secrets);
        let mut errors = self.lock_errors();
        if errors.len() >= 20 {
            errors.pop_front();
        }
        errors.push_back(clean);
    }

    pub fn set_active(&self, items: Vec<String>) {
        *self.lock_active() = items;
    }

    fn lock_secrets(&self) -> MutexGuard<'_, Vec<String>> {
        self.secrets.lock().unwrap_or_else(|err| err.into_inner())
    }

    fn lock_errors(&self) -> MutexGuard<'_, VecDeque<String>> {
        self.recent_errors
            .lock()
            .unwrap_or_else(|err| err.into_inner())
    }

    fn lock_active(&self) -> MutexGuard<'_, Vec<String>> {
        self.active.lock().unwrap_or_else(|err| err.into_inner())
    }
}

impl Default for LogState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LogHandles {
    pub guard: tracing_appender::non_blocking::WorkerGuard,
    pub state: Arc<LogState>,
    pub path: PathBuf,
}

pub fn init_logging(dir: &Path) -> Result<LogHandles, String> {
    let state = Arc::new(LogState::new());
    let rotating = RotatingFile::open(dir, MAX_LOG_BYTES, Arc::clone(&state))
        .map_err(|err| format!("не удалось открыть logs.txt: {err}"))?;
    let path = dir.join(LOG_NAME);
    let (writer, guard) = tracing_appender::non_blocking(rotating);
    let filter = tracing_subscriber::EnvFilter::new(
        "info,\
         aws_sdk_s3=off,\
         aws_smithy_runtime=off,\
         aws_smithy_runtime_api=off,\
         aws_credential_types=off,\
         hyper=off,\
         h2=off,\
         rustls=off",
    );
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_target(false)
        .with_env_filter(filter)
        .with_writer(writer)
        .try_init()
        .map_err(|err| format!("журнал уже настроен: {err}"))?;
    tracing::info!("stand-sync {}", state.version);
    tracing::info!("os {}", state.os);
    Ok(LogHandles { guard, state, path })
}

pub struct RotatingFile {
    dir: PathBuf,
    file: File,
    size: u64,
    max_bytes: u64,
    state: Arc<LogState>,
}

impl RotatingFile {
    pub fn open(dir: &Path, max_bytes: u64, state: Arc<LogState>) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join(LOG_NAME);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            dir: dir.to_path_buf(),
            file,
            size,
            max_bytes: max_bytes.max(1),
            state,
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.file.flush()?;
        drop(std::mem::replace(&mut self.file, empty_file()?));
        let current = self.dir.join(LOG_NAME);
        let older = self.dir.join("logs.txt.1");
        let oldest = self.dir.join("logs.txt.2");
        let _ = fs::remove_file(&oldest);
        if older.exists() {
            let _ = fs::rename(&older, &oldest);
        }
        if current.exists() {
            fs::rename(&current, &older)?;
        }
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current)?;
        self.size = 0;
        let header = self.header();
        self.file.write_all(header.as_bytes())?;
        self.size = header.len() as u64;
        Ok(())
    }

    fn header(&self) -> String {
        let secrets = self
            .state
            .secrets
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        let active = self
            .state
            .active
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        let errors = self
            .state
            .recent_errors
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        let mut text = format!(
            "===== stand-sync {} =====\nos: {}\nротация журнала\nактивные задания:\n",
            self.state.version, self.state.os
        );
        if active.is_empty() {
            text.push_str("- нет\n");
        } else {
            for item in active {
                text.push_str("- ");
                text.push_str(&redact(&item, &secrets));
                text.push('\n');
            }
        }
        text.push_str("последние ошибки:\n");
        if errors.is_empty() {
            text.push_str("- нет\n");
        } else {
            for item in errors {
                text.push_str("- ");
                text.push_str(&redact(&item, &secrets));
                text.push('\n');
            }
        }
        text
    }
}

impl Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let secrets = self
            .state
            .secrets
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        let text = String::from_utf8_lossy(buf);
        let clean = redact(&text, &secrets);
        let bytes = clean.as_bytes();
        if self.size > 0 && self.size.saturating_add(bytes.len() as u64) > self.max_bytes {
            self.rotate()?;
        }
        self.file.write_all(bytes)?;
        self.size = self.size.saturating_add(bytes.len() as u64);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn empty_file() -> io::Result<File> {
    tempfile_placeholder()
}

fn tempfile_placeholder() -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(std::env::temp_dir().join(format!("stand-sync-log-hold-{}", std::process::id())))
        .or_else(|_| {
            let path =
                std::env::temp_dir().join(format!("stand-sync-log-hold-{}", std::process::id()));
            OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path)
        })
}

pub fn redact(input: &str, secrets: &[String]) -> String {
    let mut out = input.to_string();
    for secret in secrets {
        if secret.len() >= 8 && out.contains(secret.as_str()) {
            out = out.replace(secret.as_str(), "***");
        }
    }
    out = redact_labeled(&out, "authorization:");
    out = redact_labeled(&out, "x-amz-security-token:");
    out = redact_query(&out, "x-amz-signature");
    out = redact_query(&out, "x-amz-credential");
    out = redact_query(&out, "x-amz-security-token");
    out
}

fn redact_labeled(input: &str, label: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let Some(start) = lower.find(label) else {
        return input.to_string();
    };
    let value_start = start + label.len();
    let rest = &input[value_start..];
    let len = rest
        .find(['\n', '\r'])
        .unwrap_or(rest.len())
        .min(rest.find("  ").unwrap_or(rest.len()));
    let mut out = String::new();
    out.push_str(&input[..value_start]);
    out.push_str(" ***");
    out.push_str(&input[value_start + len..]);
    if out.to_ascii_lowercase().find(label).is_some()
        && out[value_start + 4..].to_ascii_lowercase().contains(label)
    {
        return redact_labeled(&out, label);
    }
    out
}

fn redact_query(input: &str, name: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let needle = format!("{name}=");
    let Some(start) = lower.find(&needle) else {
        return input.to_string();
    };
    let value_start = start + needle.len();
    let rest = &input[value_start..];
    let len = rest.find(['&', ' ', '\n', '\r', '"']).unwrap_or(rest.len());
    let mut out = String::new();
    out.push_str(&input[..value_start]);
    out.push_str("***");
    out.push_str(&input[value_start + len..]);
    out
}

pub fn os_label() -> String {
    #[cfg(windows)]
    {
        windows_version().unwrap_or_else(|| "Windows".to_string())
    }
    #[cfg(not(windows))]
    {
        format!("{} {}", std::env::consts::OS, std::env::consts::ARCH)
    }
}

#[cfg(windows)]
fn windows_version() -> Option<String> {
    #[repr(C)]
    struct OsVersionInfo {
        size: u32,
        major: u32,
        minor: u32,
        build: u32,
        platform: u32,
        csd: [u16; 128],
    }
    #[link(name = "ntdll")]
    extern "system" {
        fn RtlGetVersion(info: *mut OsVersionInfo) -> i32;
    }
    let mut info = OsVersionInfo {
        size: std::mem::size_of::<OsVersionInfo>() as u32,
        major: 0,
        minor: 0,
        build: 0,
        platform: 0,
        csd: [0; 128],
    };
    let status = unsafe { RtlGetVersion(&mut info) };
    if status != 0 {
        return None;
    }
    Some(format!(
        "Windows {}.{}.{}",
        info.major, info.minor, info.build
    ))
}

pub fn error_chain(err: &dyn std::error::Error) -> String {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(next) = source {
        text.push_str(" → ");
        text.push_str(&next.to_string());
        source = next.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_secrets_signatures_and_authorization() {
        let secrets = vec!["SuperSecretValue".to_string(), "AccessKeyValue".to_string()];
        let raw = "key=SuperSecretValue Authorization: AWS4-HMAC-SHA256 Credential=abc/secret \
                   X-Amz-Signature=deadbeefcafe X-Amz-Credential=AKIAEXAMPLE%2F20200101";
        let clean = redact(raw, &secrets);
        assert!(!clean.contains("SuperSecretValue"));
        assert!(!clean.contains("deadbeefcafe"));
        assert!(!clean.contains("AKIAEXAMPLE"));
        assert!(clean.contains("***"));
    }

    #[test]
    fn rotation_rewrites_header_active_jobs_and_errors() {
        let dir = crate::testutil::temp_dir("logs");
        let state = Arc::new(LogState::new());
        state.set_active(vec!["отправка sprites/queen.png".into()]);
        state.push_error("HTTP 503 request_id=abc");
        state.remember_secret("SuperSecretValue");
        let mut file = RotatingFile::open(&dir, 180, Arc::clone(&state)).unwrap();
        file.write_all(b"SuperSecretValue ").unwrap();
        let padding = "x".repeat(200);
        file.write_all(padding.as_bytes()).unwrap();
        file.flush().unwrap();
        assert!(dir.join("logs.txt.1").is_file());
        let current = fs::read_to_string(dir.join(LOG_NAME)).unwrap();
        assert!(current.contains("stand-sync"));
        assert!(current.contains("sprites/queen.png"));
        assert!(current.contains("request_id=abc"));
        let archived = fs::read_to_string(dir.join("logs.txt.1")).unwrap();
        assert!(!archived.contains("SuperSecretValue"));
        assert!(!current.contains("SuperSecretValue"));
    }
}
