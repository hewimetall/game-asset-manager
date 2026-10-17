use crate::config::Profile;
use crate::index::Index;
use crate::logsetup::LogState;
use crate::paths::{is_excluded_rel, map_remote_key, normalize_etag, KeyMap};
use crate::plan::{build_plan, Action, RemoteFile};
use crate::retry::MAX_ATTEMPTS;
use crate::scan::scan_local;
use crate::slots::Slots;
use crate::store::{ObjectStore, StoreError};
use crate::transfer::{run_item, Ctx, ItemResult};
use crate::ui_model::{Ledger, Phase, UiModel};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct PassOptions {
    pub workdir: PathBuf,
    pub exe_name: String,
    pub profile: Profile,
    pub prefix: String,
    pub store: Arc<dyn ObjectStore>,
    pub ui: Arc<Mutex<UiModel>>,
    pub log: Arc<LogState>,
    pub cancel: Arc<AtomicBool>,
    pub backoff: fn(u32) -> Duration,
    pub initial_slots: usize,
}

pub struct PassReport {
    pub uploaded: u32,
    pub downloaded: u32,
    pub removed: u32,
    pub conflicts: u32,
    pub failed: u32,
    pub skipped: u32,
    pub forbidden: bool,
    pub error: Option<String>,
}

struct SyncFlag {
    ui: Arc<Mutex<UiModel>>,
}

impl Drop for SyncFlag {
    fn drop(&mut self) {
        if let Ok(mut ui) = self.ui.lock() {
            ui.syncing = false;
        }
    }
}

pub async fn run_pass(opts: PassOptions) -> PassReport {
    let _flag = SyncFlag {
        ui: Arc::clone(&opts.ui),
    };
    {
        let mut ui = opts.ui.lock().unwrap_or_else(|err| err.into_inner());
        ui.phase = Phase::Scanning;
        ui.finalized = false;
        ui.syncing = true;
        ui.waiting_until = None;
        ui.files_done = 0;
        ui.files_total = 0;
        ui.bytes_done = 0;
        ui.bytes_total = 0;
        ui.current.clear();
        ui.conflicts.clear();
        ui.status.clear();
        ui.degraded = false;
    }
    tracing::info!(
        stand = %opts.profile.stand,
        endpoint = %opts.profile.endpoint,
        bucket = %opts.profile.bucket,
        prefix = %opts.prefix,
        "старт прохода"
    );

    let index = match Index::open(&Index::path_in(&opts.workdir), &opts.profile) {
        Ok(index) => index,
        Err(err) => return finish_error(&opts, err.to_string()),
    };
    if let Err(err) = index.recover_pending(&opts.workdir) {
        return finish_error(&opts, err.to_string());
    }
    gc_runtime_files(&opts.workdir, &index);

    let db = Arc::new(Mutex::new(index));
    let ctx = Ctx {
        workdir: opts.workdir.clone(),
        prefix: opts.prefix.clone(),
        store: Arc::clone(&opts.store),
        db: Arc::clone(&db),
        slots: Arc::new(Slots::with_max(opts.initial_slots)),
        ui: Arc::clone(&opts.ui),
        log: Arc::clone(&opts.log),
        cancel: Arc::clone(&opts.cancel),
        ledger: Arc::new(Mutex::new(Ledger::default())),
        backoff: opts.backoff,
    };

    let scan_root = opts.workdir.clone();
    let exe_name = opts.exe_name.clone();
    let scan_handle = tokio::task::spawn_blocking(move || scan_local(&scan_root, &exe_name));
    let remote_result = list_with_retry(&ctx, &opts).await;
    let local_result = match scan_handle.await {
        Ok(result) => result.map_err(|err| err.to_string()),
        Err(err) => Err(err.to_string()),
    };
    let local = match local_result {
        Ok(local) => local,
        Err(err) => return finish_error(&opts, err),
    };
    let remote = match remote_result {
        Ok(remote) => remote,
        Err(err) => {
            let forbidden = matches!(err, crate::transfer::TransferError::Stopped);
            return finish_error_kind(&opts, err.to_string(), forbidden);
        }
    };

    let stored = match db
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .load_files()
    {
        Ok(files) => files,
        Err(err) => return finish_error(&opts, err.to_string()),
    };
    let plan = build_plan(&local, &remote, &stored);
    tracing::info!(
        uploads = plan.uploads,
        downloads = plan.downloads,
        conflicts = plan.conflicts,
        skipped = plan.skipped,
        bytes = plan.bytes,
        "план"
    );
    for item in &plan.items {
        let op = match &item.action {
            Action::Upload => "отправить",
            Action::Download => "скачать",
            Action::DeleteLocal => "удалить локально",
            Action::DeleteRemote => "удалить на сервере",
            Action::Forget => "убрать из индекса",
            Action::Conflict(reason) => reason.label(),
            Action::Skip => "пропустить",
        };
        tracing::info!(rel = %item.rel, op, bytes = item.bytes, "файл плана");
    }
    {
        let mut ui = opts.ui.lock().unwrap_or_else(|err| err.into_inner());
        ui.phase = Phase::Transferring;
        ui.files_total = plan.uploads + plan.downloads + plan.deletes;
        ui.bytes_total = plan.bytes;
    }

    let mut pending: VecDeque<_> = plan.items.into_iter().collect();
    let mut active: tokio::task::JoinSet<ItemResult> = tokio::task::JoinSet::new();
    let mut uploaded = 0u32;
    let mut downloaded = 0u32;
    let mut removed = 0u32;
    let mut conflicts = plan.conflicts;
    let mut failed = 0u32;
    let mut forbidden = false;
    let mut cancelled = false;

    while !pending.is_empty() || !active.is_empty() {
        let stopped = ctx.slots.stopped() || ctx.cancel.load(Ordering::SeqCst);
        if !stopped {
            while active.len() < 2 {
                let Some(item) = pending.pop_front() else {
                    break;
                };
                match item.action {
                    Action::Conflict(reason) => {
                        tracing::warn!(rel = %item.rel, reason = reason.label(), "конфликт, файл не перезаписан");
                        opts.ui
                            .lock()
                            .unwrap_or_else(|err| err.into_inner())
                            .conflicts
                            .push(crate::ui_model::ConflictNote {
                                rel: item.rel,
                                reason: reason.label().to_string(),
                            });
                    }
                    Action::Upload
                    | Action::Download
                    | Action::DeleteLocal
                    | Action::DeleteRemote
                    | Action::Forget => {
                        let ctx = ctx.clone();
                        active.spawn(async move { run_item(ctx, item).await });
                    }
                    Action::Skip => {}
                }
            }
        }
        if active.is_empty() {
            break;
        }
        match active.join_next().await {
            Some(Ok(ItemResult::Uploaded)) => uploaded += 1,
            Some(Ok(ItemResult::Downloaded)) => downloaded += 1,
            Some(Ok(ItemResult::Removed)) => removed += 1,
            Some(Ok(ItemResult::Conflict)) => conflicts += 1,
            Some(Ok(ItemResult::Forbidden)) => {
                forbidden = true;
                failed += 1;
            }
            Some(Ok(ItemResult::Cancelled)) => cancelled = true,
            Some(Ok(ItemResult::Failed(err))) => {
                tracing::warn!(error = %err, "файл не синхронизирован");
                failed += 1;
            }
            Some(Err(err)) => {
                failed += 1;
                tracing::error!(error = %err, "задача передачи прервана");
            }
            None => break,
        }
    }

    let ok = failed == 0 && !forbidden && !cancelled;
    let status = if cancelled {
        "Остановлено.".to_string()
    } else if forbidden {
        "Отказ доступа. Очередь и файлы сохранены.".to_string()
    } else if failed > 0 {
        "Завершено с ошибками. Очередь сохранена. См. logs.txt.".to_string()
    } else if uploaded == 0 && downloaded == 0 && removed == 0 && conflicts == 0 {
        "Изменений нет.".to_string()
    } else if conflicts > 0 {
        "Готово. Конфликты не перезаписаны.".to_string()
    } else {
        "Готово.".to_string()
    };
    {
        let mut ui = opts.ui.lock().unwrap_or_else(|err| err.into_inner());
        ui.phase = if ok { Phase::Done } else { Phase::Failed };
        ui.finalized = ok;
        ui.status = status;
        ui.waiting_until = None;
        ui.current.clear();
    }
    tracing::info!(
        uploaded,
        downloaded,
        removed,
        conflicts,
        failed,
        forbidden,
        "конец прохода"
    );
    PassReport {
        uploaded,
        downloaded,
        removed,
        conflicts,
        failed,
        skipped: plan.skipped,
        forbidden,
        error: None,
    }
}

fn finish_error(opts: &PassOptions, message: String) -> PassReport {
    finish_error_kind(opts, message, false)
}

fn finish_error_kind(opts: &PassOptions, message: String, forbidden: bool) -> PassReport {
    tracing::error!(error = %message, forbidden, "проход остановлен");
    opts.log.push_error(&message);
    if let Ok(mut ui) = opts.ui.lock() {
        ui.phase = Phase::Failed;
        ui.finalized = false;
        ui.status = message.clone();
        ui.waiting_until = None;
    }
    PassReport {
        uploaded: 0,
        downloaded: 0,
        removed: 0,
        conflicts: 0,
        failed: 1,
        skipped: 0,
        forbidden,
        error: Some(message),
    }
}

async fn list_with_retry(
    ctx: &Ctx,
    opts: &PassOptions,
) -> Result<BTreeMap<String, RemoteFile>, crate::transfer::TransferError> {
    let mut attempt = 0u32;
    loop {
        match list_remote(ctx.store.as_ref(), &opts.prefix, &opts.exe_name).await {
            Ok(remote) => {
                if let Ok(mut ui) = opts.ui.lock() {
                    ui.waiting_until = None;
                }
                return Ok(remote);
            }
            Err(err) if err.is_forbidden() => {
                ctx.slots.stop();
                return Err(crate::transfer::TransferError::Stopped);
            }
            Err(err) if err.is_network() => {
                attempt += 1;
                ctx.slots.observe_network_error();
                if attempt >= MAX_ATTEMPTS {
                    return Err(err.into());
                }
                let delay = (opts.backoff)(attempt);
                tracing::warn!(
                    attempt,
                    wait_ms = delay.as_millis() as u64,
                    "повтор листинга"
                );
                if let Ok(mut ui) = opts.ui.lock() {
                    ui.waiting_until = Some(std::time::Instant::now() + delay);
                }
                tokio::time::sleep(delay).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
}

async fn list_remote(
    store: &dyn ObjectStore,
    prefix: &str,
    exe_name: &str,
) -> Result<BTreeMap<String, RemoteFile>, StoreError> {
    let mut token = None;
    let mut remote = BTreeMap::new();
    for _ in 0..100_000 {
        let page = store.list_page(prefix, token.clone(), 1000).await?;
        for object in page.objects {
            match map_remote_key(prefix, &object.key) {
                KeyMap::File(rel) if !is_excluded_rel(&rel, exe_name) => {
                    remote.insert(
                        rel,
                        RemoteFile {
                            size: object.size,
                            etag: normalize_etag(&object.etag),
                        },
                    );
                }
                KeyMap::File(rel) => {
                    tracing::info!(rel, "служебный ключ пропущен");
                }
                KeyMap::Ignore => {}
                KeyMap::Reject(raw) => {
                    tracing::warn!(key = %raw, "ключ вне рабочей папки пропущен");
                }
            }
        }
        if page.truncated {
            let next = page.next_token.ok_or_else(|| StoreError::Other {
                status: 0,
                code: "BadList".into(),
                request_id: None,
                message: "листинг не вернул продолжение".into(),
            })?;
            if token.as_ref() == Some(&next) {
                return Err(StoreError::Other {
                    status: 0,
                    code: "BadList".into(),
                    request_id: None,
                    message: "листинг зациклился".into(),
                });
            }
            token = Some(next);
        } else {
            return Ok(remote);
        }
    }
    Err(StoreError::Other {
        status: 0,
        code: "BadList".into(),
        request_id: None,
        message: "слишком много страниц листинга".into(),
    })
}

fn gc_runtime_files(workdir: &Path, index: &Index) {
    let tmp = workdir.join(".stand-sync").join("tmp");
    if let Ok(entries) = std::fs::read_dir(&tmp) {
        for entry in entries.flatten() {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    let keep = index
        .load_transfers()
        .unwrap_or_default()
        .into_iter()
        .map(|row| PathBuf::from(row.snapshot_path))
        .collect::<HashSet<_>>();
    let snaps = workdir.join(".stand-sync").join("snapshots");
    if let Ok(entries) = std::fs::read_dir(&snaps) {
        for entry in entries.flatten() {
            if !keep.contains(&entry.path()) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::MULTIPART_THRESHOLD;
    use crate::store::{Call, FakeStore};
    use std::fs;

    fn profile() -> Profile {
        Profile {
            endpoint: "https://s3.twcstorage.ru".into(),
            bucket: "bucket".into(),
            stand: "dayna".into(),
        }
    }

    fn fast(_: u32) -> Duration {
        Duration::from_millis(1)
    }

    async fn pass(dir: &Path, store: Arc<FakeStore>, slots: usize) -> PassReport {
        let ui = Arc::new(Mutex::new(UiModel::new("dayna")));
        run_pass(PassOptions {
            workdir: dir.to_path_buf(),
            exe_name: "sync.exe".into(),
            profile: profile(),
            prefix: "stand-dayna/".into(),
            store,
            ui,
            log: Arc::new(LogState::new()),
            cancel: Arc::new(AtomicBool::new(false)),
            backoff: fast,
            initial_slots: slots,
        })
        .await
    }

    fn write_rel(dir: &Path, rel: &str, bytes: &[u8]) {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, bytes).unwrap();
    }

    #[tokio::test]
    async fn service_files_stay_out_of_the_plan() {
        let dir = crate::testutil::temp_dir("plan-service");
        fs::write(dir.join("config.toml"), b"config-body").unwrap();
        fs::write(dir.join("logs.txt"), b"log").unwrap();
        fs::write(dir.join("sync.exe"), b"exe").unwrap();
        fs::create_dir_all(dir.join(".stand-sync")).unwrap();
        fs::write(dir.join(".stand-sync").join("secret"), b"nope").unwrap();
        write_rel(&dir, "sounds/hit.wav", b"sound");
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        store.insert("stand-dayna/config.toml", b"remote-config".to_vec(), "cfg");
        store.insert("stand-dayna/.stand-sync/secret", b"remote".to_vec(), "sec");
        store.insert("stand-dayna/../evil.txt", b"evil".to_vec(), "bad");
        store.insert("stand-dayna/i18n/ru.json", b"{\"a\":1}".to_vec(), "i18n");
        let report = pass(&dir, Arc::clone(&store), 2).await;
        assert!(report.error.is_none(), "{:?}", report.error);
        assert_eq!(report.uploaded, 1);
        assert_eq!(report.downloaded, 1);
        assert_eq!(report.conflicts, 0);
        assert_eq!(fs::read(dir.join("config.toml")).unwrap(), b"config-body");
        assert_eq!(fs::read(dir.join("i18n/ru.json")).unwrap(), b"{\"a\":1}");
        assert_eq!(
            store.object("stand-dayna/sounds/hit.wav").unwrap(),
            b"sound"
        );
        assert!(store.object("stand-dayna/config.toml").unwrap() == b"remote-config");
        assert!(!dir.join("evil.txt").exists());
        assert!(store.calls().iter().all(|call| match call {
            Call::List { prefix, max_keys } => prefix == "stand-dayna/" && *max_keys == 1000,
            Call::Put { key, .. } => key == "stand-dayna/sounds/hit.wav",
            Call::Get { key } => key == "stand-dayna/i18n/ru.json",
            Call::Delete { .. } => false,
            _ => false,
        }));
    }

    #[tokio::test]
    async fn folder_edit_replaces_the_remote_copy_without_a_baseline() {
        let dir = crate::testutil::temp_dir("conflict");
        write_rel(&dir, "sprites/queen.png", b"local-queen");
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        store.insert(
            "stand-dayna/sprites/queen.png",
            b"remote-queen".to_vec(),
            "remote",
        );
        let report = pass(&dir, Arc::clone(&store), 2).await;
        assert_eq!(report.conflicts, 0);
        assert_eq!(report.uploaded, 1, "{:?}", report.error);
        assert_eq!(report.downloaded, 0);
        assert_eq!(
            fs::read(dir.join("sprites/queen.png")).unwrap(),
            b"local-queen"
        );
        assert_eq!(
            store.object("stand-dayna/sprites/queen.png").unwrap(),
            b"local-queen"
        );
    }

    #[tokio::test]
    async fn deletes_and_edits_propagate_both_ways() {
        let dir = crate::testutil::temp_dir("both-ways");
        write_rel(&dir, "keep.txt", b"same");
        write_rel(&dir, "edit-local.txt", b"v1");
        write_rel(&dir, "delete-local.txt", b"gone-soon");
        write_rel(&dir, "edit-remote.txt", b"old");
        write_rel(&dir, "delete-remote.txt", b"drop-me");
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        let first = pass(&dir, Arc::clone(&store), 1).await;
        assert_eq!(first.uploaded, 5, "{:?}", first.error);
        assert_eq!(first.failed, 0);

        write_rel(&dir, "edit-local.txt", b"v2");
        fs::remove_file(dir.join("delete-local.txt")).unwrap();
        store.insert(
            "stand-dayna/edit-remote.txt",
            b"from-server".to_vec(),
            "etag-2",
        );
        store.remove("stand-dayna/delete-remote.txt");

        let second = pass(&dir, Arc::clone(&store), 1).await;
        assert_eq!(second.failed, 0, "{:?}", second.error);
        assert_eq!(second.uploaded, 1, "local edit");
        assert_eq!(second.downloaded, 1, "remote edit");
        assert_eq!(second.removed, 2, "one local delete and one remote delete");
        assert_eq!(store.object("stand-dayna/edit-local.txt").unwrap(), b"v2");
        assert!(store.object("stand-dayna/delete-local.txt").is_none());
        assert_eq!(
            fs::read(dir.join("edit-remote.txt")).unwrap(),
            b"from-server"
        );
        assert!(!dir.join("delete-remote.txt").exists());
        assert!(dir.join("keep.txt").is_file());
    }

    #[tokio::test]
    async fn second_pass_transfers_nothing() {
        let dir = crate::testutil::temp_dir("second");
        write_rel(&dir, "data/note.txt", b"hello");
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        let first = pass(&dir, Arc::clone(&store), 2).await;
        assert_eq!(first.uploaded, 1, "{:?}", first.error);
        let puts = store
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Put { .. }))
            .count();
        let second = pass(&dir, Arc::clone(&store), 2).await;
        assert_eq!(second.uploaded, 0);
        assert_eq!(second.downloaded, 0);
        assert_eq!(second.failed, 0, "{:?}", second.error);
        let puts_after = store
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Put { .. }))
            .count();
        assert_eq!(puts_after, puts);
        assert!(
            store
                .calls()
                .iter()
                .filter(|call| matches!(call, Call::Get { .. }))
                .count()
                == 0
        );
    }

    #[tokio::test]
    async fn other_stand_does_not_reuse_the_index_or_call_network() {
        let dir = crate::testutil::temp_dir("mismatch");
        let path = Index::path_in(&dir);
        Index::open(
            &path,
            &Profile {
                endpoint: "https://s3.twcstorage.ru".into(),
                bucket: "bucket".into(),
                stand: "other".into(),
            },
        )
        .unwrap();
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        let report = pass(&dir, Arc::clone(&store), 2).await;
        assert!(report.error.unwrap().contains("не применяется"));
        assert!(store.calls().is_empty());
        let index = Index::open(
            &path,
            &Profile {
                endpoint: "https://s3.twcstorage.ru".into(),
                bucket: "bucket".into(),
                stand: "other".into(),
            },
        )
        .unwrap();
        assert_eq!(index.profile().unwrap().stand, "other");
    }

    #[tokio::test]
    async fn failed_download_keeps_the_local_file() {
        let dir = crate::testutil::temp_dir("dl-fail");
        write_rel(&dir, "sprites/queen.png", b"original");
        let sha = crate::paths::sha256_bytes(b"original");
        let index = Index::open(&Index::path_in(&dir), &profile()).unwrap();
        index
            .commit_success(&crate::index::FileRow {
                rel: "sprites/queen.png".into(),
                size: 8,
                sha256: sha,
                remote_etag: "old".into(),
                remote_size: 8,
            })
            .unwrap();
        drop(index);
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        store.insert(
            "stand-dayna/sprites/queen.png",
            b"remote-bytes".to_vec(),
            "new",
        );
        store.fail_gets.store(100, Ordering::SeqCst);
        let report = pass(&dir, Arc::clone(&store), 2).await;
        assert!(report.failed >= 1);
        assert_eq!(report.downloaded, 0);
        assert_eq!(
            fs::read(dir.join("sprites/queen.png")).unwrap(),
            b"original"
        );
        let index = Index::open(&Index::path_in(&dir), &profile()).unwrap();
        assert_eq!(
            index.load_files().unwrap()["sprites/queen.png"].remote_etag,
            "old"
        );
    }

    #[tokio::test]
    async fn local_change_during_download_is_a_conflict() {
        let dir = crate::testutil::temp_dir("dl-race");
        let target = dir.join("sprites").join("queen.png");
        write_rel(&dir, "sprites/queen.png", b"original");
        let sha = crate::paths::sha256_bytes(b"original");
        let index = Index::open(&Index::path_in(&dir), &profile()).unwrap();
        index
            .commit_success(&crate::index::FileRow {
                rel: "sprites/queen.png".into(),
                size: 8,
                sha256: sha,
                remote_etag: "old".into(),
                remote_size: 8,
            })
            .unwrap();
        drop(index);
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        store.insert(
            "stand-dayna/sprites/queen.png",
            b"from-server".to_vec(),
            "new",
        );
        let hook_path = target.clone();
        store.set_before_get(move || {
            fs::write(&hook_path, b"user-edit").unwrap();
        });
        let report = pass(&dir, Arc::clone(&store), 2).await;
        assert_eq!(report.conflicts, 1, "{:?}", report.error);
        assert_eq!(report.downloaded, 0);
        assert_eq!(fs::read(&target).unwrap(), b"user-edit");
        let index = Index::open(&Index::path_in(&dir), &profile()).unwrap();
        assert_eq!(
            index.load_files().unwrap()["sprites/queen.png"].remote_etag,
            "old"
        );
    }

    #[tokio::test]
    async fn multipart_resume_does_not_resend_confirmed_parts() {
        let dir = crate::testutil::temp_dir("multipart");
        let bytes = vec![9u8; MULTIPART_THRESHOLD as usize];
        write_rel(&dir, "sprites/big.bin", &bytes);
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        *store.stop_on_part.lock().unwrap() = Some(2);
        let first = pass(&dir, Arc::clone(&store), 1).await;
        assert!(first.failed >= 1, "{:?}", first.error);
        let index = Index::open(&Index::path_in(&dir), &profile()).unwrap();
        let transfer = index
            .transfer("sprites/big.bin")
            .unwrap()
            .expect("upload id сохранён");
        assert!(transfer.upload_id.is_some());
        let parts = index.load_parts("sprites/big.bin").unwrap();
        assert!(parts.contains_key(&1));
        assert!(!parts.contains_key(&2));
        assert!(index.load_files().unwrap().get("sprites/big.bin").is_none());
        drop(index);
        *store.stop_on_part.lock().unwrap() = None;
        let second = pass(&dir, Arc::clone(&store), 1).await;
        assert_eq!(second.failed, 0, "{:?}", second.error);
        assert_eq!(second.uploaded, 1);
        let part1 = store
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Part { number: 1, .. }))
            .count();
        let creates = store
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Create { .. }))
            .count();
        assert_eq!(part1, 1);
        assert_eq!(creates, 1);
        assert_eq!(store.object("stand-dayna/sprites/big.bin").unwrap(), bytes);
    }

    #[tokio::test]
    async fn lost_complete_keeps_the_same_session() {
        let dir = crate::testutil::temp_dir("complete-keep");
        let bytes = vec![4u8; MULTIPART_THRESHOLD as usize];
        write_rel(&dir, "sprites/big.bin", &bytes);
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        store.lose_complete_keep_session();
        let report = pass(&dir, Arc::clone(&store), 1).await;
        assert_eq!(report.failed, 0, "{:?}", report.error);
        assert_eq!(report.uploaded, 1);
        assert_eq!(
            store
                .calls()
                .iter()
                .filter(|call| matches!(call, Call::Create { .. }))
                .count(),
            1
        );
        assert_eq!(
            store
                .calls()
                .iter()
                .filter(|call| matches!(call, Call::Part { number: 1, .. }))
                .count(),
            1
        );
        assert!(
            store
                .calls()
                .iter()
                .filter(|call| matches!(call, Call::Complete { .. }))
                .count()
                >= 2
        );
    }

    #[tokio::test]
    async fn lost_complete_after_apply_is_recovered_by_head() {
        let dir = crate::testutil::temp_dir("complete-head");
        let bytes = vec![5u8; MULTIPART_THRESHOLD as usize];
        write_rel(&dir, "sprites/big.bin", &bytes);
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        store.lose_complete_after_apply();
        let report = pass(&dir, Arc::clone(&store), 1).await;
        assert_eq!(report.failed, 0, "{:?}", report.error);
        assert_eq!(
            store
                .calls()
                .iter()
                .filter(|call| matches!(call, Call::Create { .. }))
                .count(),
            1
        );
        assert_eq!(store.object("stand-dayna/sprites/big.bin").unwrap(), bytes);
        let index = Index::open(&Index::path_in(&dir), &profile()).unwrap();
        assert!(index.load_files().unwrap().contains_key("sprites/big.bin"));
        assert!(index.transfer("sprites/big.bin").unwrap().is_none());
    }

    #[tokio::test]
    async fn vanished_session_does_not_open_a_new_one_in_the_same_pass() {
        let dir = crate::testutil::temp_dir("complete-drop");
        let bytes = vec![6u8; MULTIPART_THRESHOLD as usize];
        write_rel(&dir, "sprites/big.bin", &bytes);
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        store.drop_session_on_complete();
        let first = pass(&dir, Arc::clone(&store), 1).await;
        assert!(first.failed >= 1, "{:?}", first.error);
        assert_eq!(
            store
                .calls()
                .iter()
                .filter(|call| matches!(call, Call::Create { .. }))
                .count(),
            1
        );
        let index = Index::open(&Index::path_in(&dir), &profile()).unwrap();
        let row = index.transfer("sprites/big.bin").unwrap().unwrap();
        assert_eq!(row.state, "indeterminate");
        assert!(row.checks >= 1);
        drop(index);
        let second = pass(&dir, Arc::clone(&store), 1).await;
        assert_eq!(second.failed, 0, "{:?}", second.error);
        assert_eq!(
            store
                .calls()
                .iter()
                .filter(|call| matches!(call, Call::Create { .. }))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn forbidden_stops_and_keeps_the_file() {
        let dir = crate::testutil::temp_dir("forbidden");
        write_rel(&dir, "sprites/queen.png", b"keep-me");
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        store.set_forbidden(true);
        let report = pass(&dir, Arc::clone(&store), 2).await;
        assert!(report.forbidden);
        assert_eq!(fs::read(dir.join("sprites/queen.png")).unwrap(), b"keep-me");
        assert!(store.object("stand-dayna/sprites/queen.png").is_none());
    }

    #[tokio::test]
    async fn listing_follows_pages_with_the_prefix_cap() {
        let dir = crate::testutil::temp_dir("pages");
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        *store.page_cap.lock().unwrap() = Some(1);
        store.insert("stand-dayna/a.txt", b"a".to_vec(), "a");
        store.insert("stand-dayna/b.txt", b"b".to_vec(), "b");
        store.insert("stand-dayna/c.txt", b"c".to_vec(), "c");
        let report = pass(&dir, Arc::clone(&store), 2).await;
        assert_eq!(report.downloaded, 3, "{:?}", report.error);
        let lists: Vec<_> = store
            .calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::List { prefix, max_keys } => Some((prefix, max_keys)),
                _ => None,
            })
            .collect();
        assert!(lists.len() >= 3);
        assert!(lists
            .iter()
            .all(|(prefix, max_keys)| prefix == "stand-dayna/" && *max_keys == 1000));
        assert_eq!(fs::read(dir.join("a.txt")).unwrap(), b"a");
        assert_eq!(fs::read(dir.join("c.txt")).unwrap(), b"c");
    }

    #[tokio::test]
    async fn data_slots_never_exceed_two() {
        let dir = crate::testutil::temp_dir("slots-pass");
        write_rel(&dir, "a.txt", b"one");
        write_rel(&dir, "b.txt", b"two");
        write_rel(&dir, "c.txt", b"three");
        let store = Arc::new(FakeStore::new("stand-dayna/"));
        let report = pass(&dir, Arc::clone(&store), 2).await;
        assert_eq!(report.failed, 0, "{:?}", report.error);
        assert!(store.data_max.load(Ordering::SeqCst) <= 2);
    }
}
