use crate::paths::sha256_bytes;
use std::path::Path;

#[derive(Debug)]
pub enum LockError {
    AlreadyRunning,
    Failed(String),
}

pub struct InstanceLock {
    #[cfg(windows)]
    handle: windows_sys::Win32::Foundation::HANDLE,
    #[cfg(unix)]
    _file: std::fs::File,
}

pub fn try_lock(workdir: &Path) -> Result<InstanceLock, LockError> {
    let name = mutex_name(workdir);
    #[cfg(windows)]
    {
        lock_windows(&name)
    }
    #[cfg(unix)]
    {
        lock_unix(&name)
    }
}

pub fn mutex_name(workdir: &Path) -> String {
    let canonical = workdir.to_string_lossy().to_ascii_lowercase();
    let hash = &sha256_bytes(canonical.as_bytes())[..32];
    format!("Local\\StandSync-{hash}")
}

#[cfg(windows)]
fn lock_windows(name: &str) -> Result<InstanceLock, LockError> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::{GetLastError, ERROR_ALREADY_EXISTS};
    use windows_sys::Win32::System::Threading::CreateMutexW;
    let wide: Vec<u16> = std::ffi::OsStr::new(name)
        .encode_wide()
        .chain(Some(0))
        .collect();
    let handle = unsafe { CreateMutexW(std::ptr::null(), 1, wide.as_ptr()) };
    if handle.is_null() {
        return Err(LockError::Failed(
            "не удалось создать системную блокировку".into(),
        ));
    }
    let err = unsafe { GetLastError() };
    if err == ERROR_ALREADY_EXISTS {
        unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) };
        return Err(LockError::AlreadyRunning);
    }
    Ok(InstanceLock { handle })
}

#[cfg(windows)]
impl Drop for InstanceLock {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::System::Threading::ReleaseMutex(self.handle);
            windows_sys::Win32::Foundation::CloseHandle(self.handle);
        }
    }
}

#[cfg(unix)]
fn lock_unix(name: &str) -> Result<InstanceLock, LockError> {
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join(format!("stand-sync-{}.lock", name.replace('\\', "_")));
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|err| LockError::Failed(err.to_string()))?;
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::WouldBlock {
            return Err(LockError::AlreadyRunning);
        }
        return Err(LockError::Failed(err.to_string()));
    }
    Ok(InstanceLock { _file: file })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutex_name_depends_on_the_folder() {
        let a = mutex_name(Path::new("/games/dayna"));
        let b = mutex_name(Path::new("/games/other"));
        assert_ne!(a, b);
        assert!(a.starts_with("Local\\StandSync-"));
        assert!(!a.contains("dayna"));
    }

    #[test]
    fn second_lock_in_this_process_stops() {
        let dir = crate::testutil::temp_dir("lock");
        let first = try_lock(&dir).unwrap();
        let second = try_lock(&dir);
        assert!(matches!(second, Err(LockError::AlreadyRunning)));
        drop(first);
        let third = try_lock(&dir);
        assert!(third.is_ok());
    }
}
