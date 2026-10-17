use async_trait::async_trait;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedObject {
    pub key: String,
    pub size: u64,
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListPage {
    pub objects: Vec<ListedObject>,
    pub next_token: Option<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartInfo {
    pub number: i32,
    pub size: u64,
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartsPage {
    pub parts: Vec<PartInfo>,
    pub next_marker: Option<i32>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeadInfo {
    pub size: u64,
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Downloaded {
    pub bytes_written: u64,
    pub etag: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("нет сети: {message}")]
    Network {
        message: String,
        request_id: Option<String>,
    },
    #[error("отказ доступа ({code})")]
    Forbidden {
        code: String,
        request_id: Option<String>,
        message: String,
    },
    #[error("ключи отклонены ({code})")]
    Unauthorized {
        code: String,
        request_id: Option<String>,
        message: String,
    },
    #[error("объект не найден")]
    NotFound { request_id: Option<String> },
    #[error("сессия multipart не найдена")]
    NoSuchUpload { request_id: Option<String> },
    #[error("запрос отклонён: {0}")]
    Refused(String),
    #[error("ошибка S3 (HTTP {status}, {code}): {message}")]
    Other {
        status: u16,
        code: String,
        request_id: Option<String>,
        message: String,
    },
    #[error("останов теста")]
    TestStop,
}

impl StoreError {
    pub fn network(message: impl Into<String>) -> Self {
        Self::Network {
            message: message.into(),
            request_id: None,
        }
    }

    pub fn forbidden() -> Self {
        Self::Forbidden {
            code: "AccessDenied".into(),
            request_id: Some("req-forbidden".into()),
            message: "AccessDenied".into(),
        }
    }

    pub fn is_network(&self) -> bool {
        matches!(self, Self::Network { .. })
    }

    pub fn is_forbidden(&self) -> bool {
        matches!(self, Self::Forbidden { .. })
    }

    pub fn is_unauthorized(&self) -> bool {
        matches!(self, Self::Unauthorized { .. })
    }

    pub fn is_no_such_upload(&self) -> bool {
        matches!(self, Self::NoSuchUpload { .. })
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }

    pub fn retryable(&self) -> bool {
        self.is_network()
    }

    pub fn http_status(&self) -> Option<u16> {
        match self {
            Self::Forbidden { .. } => Some(403),
            Self::Unauthorized { .. } => Some(401),
            Self::NotFound { .. } => Some(404),
            Self::NoSuchUpload { .. } => Some(404),
            Self::Other { status, .. } => Some(*status),
            Self::Network { .. } | Self::Refused(_) | Self::TestStop => None,
        }
    }

    pub fn s3_code(&self) -> Option<&str> {
        match self {
            Self::Forbidden { code, .. }
            | Self::Unauthorized { code, .. }
            | Self::Other { code, .. } => Some(code.as_str()),
            Self::NotFound { .. } => Some("NotFound"),
            Self::NoSuchUpload { .. } => Some("NoSuchUpload"),
            _ => None,
        }
    }

    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Network { request_id, .. }
            | Self::Forbidden { request_id, .. }
            | Self::Unauthorized { request_id, .. }
            | Self::NotFound { request_id }
            | Self::NoSuchUpload { request_id }
            | Self::Other { request_id, .. } => request_id.as_deref(),
            Self::Refused(_) | Self::TestStop => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call {
    List {
        prefix: String,
        max_keys: i32,
    },
    Put {
        key: String,
        len: usize,
    },
    Create {
        key: String,
    },
    Part {
        key: String,
        upload_id: String,
        number: i32,
        len: usize,
    },
    ListParts {
        key: String,
        upload_id: String,
    },
    Complete {
        key: String,
        upload_id: String,
    },
    Head {
        key: String,
    },
    Get {
        key: String,
    },
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn list_page(
        &self,
        prefix: &str,
        token: Option<String>,
        max_keys: i32,
    ) -> Result<ListPage, StoreError>;
    async fn put_object(&self, key: &str, body: Vec<u8>) -> Result<String, StoreError>;
    async fn create_multipart(&self, key: &str) -> Result<String, StoreError>;
    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Vec<u8>,
    ) -> Result<String, StoreError>;
    async fn list_parts_page(
        &self,
        key: &str,
        upload_id: &str,
        marker: Option<i32>,
    ) -> Result<PartsPage, StoreError>;
    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<String, StoreError>;
    async fn head(&self, key: &str) -> Result<HeadInfo, StoreError>;
    async fn get_to(
        &self,
        key: &str,
        dest: &Path,
        progress: Arc<dyn Fn(u64) + Send + Sync>,
    ) -> Result<Downloaded, StoreError>;
}

pub async fn check_listing(store: &dyn ObjectStore, prefix: &str) -> Result<(), StoreError> {
    if !prefix.starts_with("stand-") || !prefix.ends_with('/') || prefix.contains("..") {
        return Err(StoreError::Refused("некорректный префикс".into()));
    }
    let _page = store.list_page(prefix, None, 1).await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompleteFault {
    None,
    /// The server finished the upload and dropped the session, but the response was lost.
    ApplyThenLose,
    /// The response was lost and the session is still open.
    LoseKeepSession,
    /// The session disappeared and the object was not written.
    DropSession,
}

struct FakeObject {
    bytes: Vec<u8>,
    etag: String,
}

struct FakeSession {
    key: String,
    parts: BTreeMap<i32, Vec<u8>>,
}

pub struct FakeStore {
    pub prefix: String,
    objects: Mutex<BTreeMap<String, FakeObject>>,
    sessions: Mutex<BTreeMap<String, FakeSession>>,
    next_upload: Mutex<u64>,
    pub calls: Mutex<Vec<Call>>,
    fail_network: Mutex<u32>,
    forbidden: AtomicBool,
    pub stop_on_part: Mutex<Option<i32>>,
    complete_fault: Mutex<CompleteFault>,
    pub page_cap: Mutex<Option<i32>>,
    on_before_get: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    pub data_inflight: AtomicUsize,
    pub data_max: AtomicUsize,
    pub fail_gets: AtomicUsize,
}

impl FakeStore {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            objects: Mutex::new(BTreeMap::new()),
            sessions: Mutex::new(BTreeMap::new()),
            next_upload: Mutex::new(1),
            calls: Mutex::new(Vec::new()),
            fail_network: Mutex::new(0),
            forbidden: AtomicBool::new(false),
            stop_on_part: Mutex::new(None),
            complete_fault: Mutex::new(CompleteFault::None),
            page_cap: Mutex::new(None),
            on_before_get: Mutex::new(None),
            data_inflight: AtomicUsize::new(0),
            data_max: AtomicUsize::new(0),
            fail_gets: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, key: impl Into<String>, bytes: Vec<u8>, etag: impl Into<String>) {
        self.objects.lock().unwrap().insert(
            key.into(),
            FakeObject {
                bytes,
                etag: etag.into(),
            },
        );
    }

    pub fn object(&self, key: &str) -> Option<Vec<u8>> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .map(|obj| obj.bytes.clone())
    }

    pub fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    pub fn fail_network_times(&self, times: u32) {
        *self.fail_network.lock().unwrap() = times;
    }

    pub fn set_forbidden(&self, value: bool) {
        self.forbidden.store(value, Ordering::SeqCst);
    }

    pub fn lose_complete_after_apply(&self) {
        *self.complete_fault.lock().unwrap() = CompleteFault::ApplyThenLose;
    }

    pub fn lose_complete_keep_session(&self) {
        *self.complete_fault.lock().unwrap() = CompleteFault::LoseKeepSession;
    }

    pub fn drop_session_on_complete(&self) {
        *self.complete_fault.lock().unwrap() = CompleteFault::DropSession;
    }

    pub fn set_before_get(&self, hook: impl Fn() + Send + Sync + 'static) {
        *self.on_before_get.lock().unwrap() = Some(Arc::new(hook));
    }

    fn record(&self, call: Call) {
        self.calls.lock().unwrap().push(call);
    }

    fn guard_prefix(&self, prefix: &str) -> Result<(), StoreError> {
        if prefix.is_empty() || !prefix.ends_with('/') {
            return Err(StoreError::Refused("листинг без префикса запрещён".into()));
        }
        Ok(())
    }

    fn guard_key(&self, key: &str) -> Result<(), StoreError> {
        if self.prefix.is_empty() || !key.starts_with(&self.prefix) || key.contains("..") {
            return Err(StoreError::Refused(format!("ключ вне префикса: {key}")));
        }
        Ok(())
    }

    fn check_faults(&self) -> Result<(), StoreError> {
        if self.forbidden.load(Ordering::SeqCst) {
            return Err(StoreError::forbidden());
        }
        let mut left = self.fail_network.lock().unwrap();
        if *left > 0 {
            *left -= 1;
            return Err(StoreError::network("соединение прервано"));
        }
        Ok(())
    }

    fn track_data(&self) -> DataGuard<'_> {
        let now = self.data_inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.data_max.fetch_max(now, Ordering::SeqCst);
        DataGuard { store: self }
    }
}

struct DataGuard<'a> {
    store: &'a FakeStore,
}

impl Drop for DataGuard<'_> {
    fn drop(&mut self) {
        self.store.data_inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for FakeStore {
    async fn list_page(
        &self,
        prefix: &str,
        token: Option<String>,
        max_keys: i32,
    ) -> Result<ListPage, StoreError> {
        self.guard_prefix(prefix)?;
        self.record(Call::List {
            prefix: prefix.to_string(),
            max_keys,
        });
        self.check_faults()?;
        let cap = self.page_cap.lock().unwrap().unwrap_or(max_keys).max(1) as usize;
        let objects = self.objects.lock().unwrap();
        let keys: Vec<String> = objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect();
        let start = match token {
            Some(token) => keys
                .iter()
                .position(|key| key == &token)
                .map(|idx| idx + 1)
                .unwrap_or(0),
            None => 0,
        };
        let slice = &keys[start.min(keys.len())..];
        let take = cap.min(slice.len());
        let mut page = Vec::new();
        for key in &slice[..take] {
            let obj = &objects[key];
            page.push(ListedObject {
                key: key.clone(),
                size: obj.bytes.len() as u64,
                etag: obj.etag.clone(),
            });
        }
        let truncated = slice.len() > take;
        let next_token = if truncated {
            page.last().map(|obj| obj.key.clone())
        } else {
            None
        };
        Ok(ListPage {
            objects: page,
            next_token,
            truncated,
        })
    }

    async fn put_object(&self, key: &str, body: Vec<u8>) -> Result<String, StoreError> {
        self.guard_key(key)?;
        self.record(Call::Put {
            key: key.to_string(),
            len: body.len(),
        });
        let _guard = self.track_data();
        self.check_faults()?;
        let etag = format!("put-{}", body.len());
        self.objects.lock().unwrap().insert(
            key.to_string(),
            FakeObject {
                bytes: body,
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    async fn create_multipart(&self, key: &str) -> Result<String, StoreError> {
        self.guard_key(key)?;
        self.record(Call::Create {
            key: key.to_string(),
        });
        self.check_faults()?;
        let mut next = self.next_upload.lock().unwrap();
        let id = format!("upload-{next}");
        *next += 1;
        self.sessions.lock().unwrap().insert(
            id.clone(),
            FakeSession {
                key: key.to_string(),
                parts: BTreeMap::new(),
            },
        );
        Ok(id)
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Vec<u8>,
    ) -> Result<String, StoreError> {
        self.guard_key(key)?;
        self.record(Call::Part {
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            number: part_number,
            len: body.len(),
        });
        let _guard = self.track_data();
        self.check_faults()?;
        if self.stop_on_part.lock().unwrap().as_ref() == Some(&part_number) {
            return Err(StoreError::TestStop);
        }
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions
            .get_mut(upload_id)
            .ok_or(StoreError::NoSuchUpload { request_id: None })?;
        if session.key != key {
            return Err(StoreError::Refused("чужая сессия".into()));
        }
        session.parts.insert(part_number, body);
        Ok(format!("etag-{part_number}"))
    }

    async fn list_parts_page(
        &self,
        key: &str,
        upload_id: &str,
        marker: Option<i32>,
    ) -> Result<PartsPage, StoreError> {
        self.guard_key(key)?;
        self.record(Call::ListParts {
            key: key.to_string(),
            upload_id: upload_id.to_string(),
        });
        self.check_faults()?;
        let sessions = self.sessions.lock().unwrap();
        let session = sessions
            .get(upload_id)
            .ok_or(StoreError::NoSuchUpload { request_id: None })?;
        let mut numbers: Vec<i32> = session
            .parts
            .keys()
            .copied()
            .filter(|n| marker.map(|m| *n > m).unwrap_or(true))
            .collect();
        numbers.sort_unstable();
        let take = numbers.len().min(1000);
        let parts = numbers[..take]
            .iter()
            .map(|number| PartInfo {
                number: *number,
                size: session.parts[number].len() as u64,
                etag: format!("etag-{number}"),
            })
            .collect::<Vec<_>>();
        let truncated = numbers.len() > take;
        let next_marker = if truncated {
            parts.last().map(|part| part.number)
        } else {
            None
        };
        Ok(PartsPage {
            parts,
            next_marker,
            truncated,
        })
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<String, StoreError> {
        self.guard_key(key)?;
        self.record(Call::Complete {
            key: key.to_string(),
            upload_id: upload_id.to_string(),
        });
        self.check_faults()?;
        let fault = *self.complete_fault.lock().unwrap();
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions
            .get(upload_id)
            .ok_or(StoreError::NoSuchUpload { request_id: None })?;
        if session.key != key {
            return Err(StoreError::Refused("чужая сессия".into()));
        }
        let mut ordered = parts;
        ordered.sort_by_key(|(number, _)| *number);
        let mut body = Vec::new();
        for (number, _) in &ordered {
            let part = session.parts.get(number).ok_or_else(|| StoreError::Other {
                status: 400,
                code: "InvalidPart".into(),
                request_id: None,
                message: format!("нет части {number}"),
            })?;
            body.extend_from_slice(part);
        }
        let etag = format!("multi-{}-{}", body.len(), ordered.len());
        if fault == CompleteFault::LoseKeepSession {
            *self.complete_fault.lock().unwrap() = CompleteFault::None;
            return Err(StoreError::network("ответ Complete потерян, сессия жива"));
        }
        if fault == CompleteFault::DropSession {
            *self.complete_fault.lock().unwrap() = CompleteFault::None;
            let id = upload_id.to_string();
            drop(sessions);
            self.sessions.lock().unwrap().remove(&id);
            return Err(StoreError::network(
                "ответ Complete потерян, сессия исчезла",
            ));
        }
        self.objects.lock().unwrap().insert(
            key.to_string(),
            FakeObject {
                bytes: body,
                etag: etag.clone(),
            },
        );
        sessions.remove(upload_id);
        if fault == CompleteFault::ApplyThenLose {
            *self.complete_fault.lock().unwrap() = CompleteFault::None;
            return Err(StoreError::network("ответ Complete потерян"));
        }
        Ok(etag)
    }

    async fn head(&self, key: &str) -> Result<HeadInfo, StoreError> {
        self.guard_key(key)?;
        self.record(Call::Head {
            key: key.to_string(),
        });
        self.check_faults()?;
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .map(|obj| HeadInfo {
                size: obj.bytes.len() as u64,
                etag: obj.etag.clone(),
            })
            .ok_or(StoreError::NotFound { request_id: None })
    }

    async fn get_to(
        &self,
        key: &str,
        dest: &Path,
        progress: Arc<dyn Fn(u64) + Send + Sync>,
    ) -> Result<Downloaded, StoreError> {
        self.guard_key(key)?;
        self.record(Call::Get {
            key: key.to_string(),
        });
        let _guard = self.track_data();
        self.check_faults()?;
        let left = self.fail_gets.load(Ordering::SeqCst);
        if left > 0 {
            self.fail_gets.fetch_sub(1, Ordering::SeqCst);
            return Err(StoreError::network("обрыв GET"));
        }
        if let Some(hook) = self.on_before_get.lock().unwrap().clone() {
            hook();
        }
        let objects = self.objects.lock().unwrap();
        let obj = objects
            .get(key)
            .ok_or(StoreError::NotFound { request_id: None })?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|err| StoreError::network(err.to_string()))?;
        }
        let mut written = 0u64;
        let mut out = Vec::new();
        for chunk in obj.bytes.chunks(64) {
            out.extend_from_slice(chunk);
            written += chunk.len() as u64;
            progress(written);
        }
        std::fs::write(dest, &out).map_err(|err| StoreError::network(err.to_string()))?;
        Ok(Downloaded {
            bytes_written: written,
            etag: obj.etag.clone(),
        })
    }
}

pub async fn list_all_parts(
    store: &dyn ObjectStore,
    key: &str,
    upload_id: &str,
) -> Result<Vec<PartInfo>, StoreError> {
    let mut marker = None;
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for _ in 0..100 {
        let page = store.list_parts_page(key, upload_id, marker).await?;
        for part in page.parts {
            if seen.insert(part.number) {
                out.push(part);
            }
        }
        if page.truncated {
            marker = page.next_marker.or(marker);
            if marker.is_none() {
                return Err(StoreError::Other {
                    status: 0,
                    code: "BadList".into(),
                    request_id: None,
                    message: "листинг частей зациклился".into(),
                });
            }
        } else {
            return Ok(out);
        }
    }
    Err(StoreError::Other {
        status: 0,
        code: "BadList".into(),
        request_id: None,
        message: "слишком много страниц частей".into(),
    })
}

pub fn classify_http(
    status: u16,
    code: &str,
    request_id: Option<String>,
    message: String,
) -> StoreError {
    let code_l = code.to_ascii_lowercase();
    if status == 403 || code == "AccessDenied" {
        return StoreError::Forbidden {
            code: code.to_string(),
            request_id,
            message,
        };
    }
    if status == 401
        || code == "InvalidAccessKeyId"
        || code == "SignatureDoesNotMatch"
        || code == "InvalidClientTokenId"
    {
        return StoreError::Unauthorized {
            code: code.to_string(),
            request_id,
            message,
        };
    }
    if code == "NoSuchUpload" {
        return StoreError::NoSuchUpload { request_id };
    }
    if status == 404 || code == "NoSuchKey" || code == "NotFound" {
        return StoreError::NotFound { request_id };
    }
    if status == 0
        || status == 408
        || status == 429
        || (500..600).contains(&status)
        || code_l.contains("timeout")
        || code_l.contains("slowdown")
    {
        return StoreError::Network {
            message: if message.is_empty() {
                format!("HTTP {status} {code}")
            } else {
                message
            },
            request_id,
        };
    }
    StoreError::Other {
        status,
        code: code.to_string(),
        request_id,
        message,
    }
}

pub fn access_failure_message(err: &StoreError) -> String {
    if err.is_network() {
        "Нет сети. Ключи не проверены и не сохранены.".into()
    } else if err.is_forbidden() {
        "Отказ доступа.".into()
    } else if err.is_unauthorized() {
        "Ключи отклонены.".into()
    } else {
        format!("Не удалось проверить доступ: {err}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn check_lists_only_one_key_under_the_prefix() {
        let store = FakeStore::new("stand-dayna/");
        store.insert("stand-dayna/a.txt", b"a".to_vec(), "e1");
        store.insert("stand-dayna/b.txt", b"bb".to_vec(), "e2");
        check_listing(&store, "stand-dayna/").await.unwrap();
        assert_eq!(
            store.calls(),
            vec![Call::List {
                prefix: "stand-dayna/".into(),
                max_keys: 1
            }]
        );
    }

    #[tokio::test]
    async fn empty_prefix_is_refused() {
        let store = FakeStore::new("stand-dayna/");
        let err = store.list_page("", None, 1000).await.unwrap_err();
        assert!(matches!(err, StoreError::Refused(_)));
    }

    #[test]
    fn network_is_not_unauthorized() {
        let err = StoreError::network("dns");
        assert!(err.is_network());
        assert!(!err.is_unauthorized());
        assert!(!err.is_forbidden());
        let denied = classify_http(403, "AccessDenied", Some("rid".into()), "no".into());
        assert!(denied.is_forbidden());
        assert_eq!(denied.request_id(), Some("rid"));
        let text = access_failure_message(&StoreError::network("dns"));
        assert!(text.contains("Нет сети"));
        assert!(!text.to_lowercase().contains("неверн"));
    }
}
