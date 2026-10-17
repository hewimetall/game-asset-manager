use crate::index::{FileRow, Index, PartRow, TransferRow};
use crate::limits::{check_upload_size, part_count, use_multipart, PART_SIZE};
use crate::logsetup::{error_chain, LogState};
use crate::paths::{local_target, replace_file, sha256_bytes, sha256_file};
use crate::plan::{ConflictReason, Planned};
use crate::retry::MAX_ATTEMPTS;
use crate::slots::Slots;
use crate::store::{list_all_parts, ObjectStore, StoreError};
use crate::ui_model::{CurrentFile, Direction, Ledger, UiModel};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct Ctx {
    pub workdir: PathBuf,
    pub prefix: String,
    pub store: Arc<dyn ObjectStore>,
    pub db: Arc<Mutex<Index>>,
    pub slots: Arc<Slots>,
    pub ui: Arc<Mutex<UiModel>>,
    pub log: Arc<LogState>,
    pub cancel: Arc<AtomicBool>,
    pub ledger: Arc<Mutex<Ledger>>,
    pub backoff: fn(u32) -> Duration,
}

impl Clone for Ctx {
    fn clone(&self) -> Self {
        Self {
            workdir: self.workdir.clone(),
            prefix: self.prefix.clone(),
            store: Arc::clone(&self.store),
            db: Arc::clone(&self.db),
            slots: Arc::clone(&self.slots),
            ui: Arc::clone(&self.ui),
            log: Arc::clone(&self.log),
            cancel: Arc::clone(&self.cancel),
            ledger: Arc::clone(&self.ledger),
            backoff: self.backoff,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Db(#[from] crate::index::DbError),
    #[error("{0}")]
    Message(String),
    #[error("конфликт: {rel} ({reason})")]
    Conflict { rel: String, reason: String },
    #[error("неопределённый результат загрузки {rel}: новая сессия не открыта")]
    Indeterminate { rel: String },
    #[error("остановлено: отказ доступа")]
    Stopped,
    #[error("отменено")]
    Cancelled,
}

pub enum ItemResult {
    Uploaded,
    Downloaded,
    Conflict,
    Failed(TransferError),
    Forbidden,
    Cancelled,
}

pub async fn run_item(ctx: Ctx, item: Planned) -> ItemResult {
    if ctx.cancel.load(Ordering::SeqCst) {
        return ItemResult::Cancelled;
    }
    let result = match item.action {
        crate::plan::Action::Upload => upload_file(&ctx, &item)
            .await
            .map(|()| ItemResult::Uploaded),
        crate::plan::Action::Download => download_file(&ctx, &item)
            .await
            .map(|()| ItemResult::Downloaded),
        crate::plan::Action::Conflict(reason) => {
            record_conflict(&ctx, &item.rel, reason);
            return ItemResult::Conflict;
        }
        crate::plan::Action::Skip => return ItemResult::Uploaded,
    };
    match result {
        Ok(value) => value,
        Err(TransferError::Stopped) | Err(TransferError::Store(StoreError::Forbidden { .. })) => {
            ctx.slots.stop();
            forget_file(&ctx, &item.rel);
            log_transfer(&ctx, &item.rel, "останов", &TransferError::Stopped);
            ItemResult::Forbidden
        }
        Err(TransferError::Cancelled) => {
            forget_file(&ctx, &item.rel);
            ItemResult::Cancelled
        }
        Err(TransferError::Conflict { rel, reason }) => {
            forget_file(&ctx, &rel);
            tracing::warn!(rel, reason, "конфликт, файл не перезаписан");
            lock_ui(&ctx.ui)
                .conflicts
                .push(crate::ui_model::ConflictNote { rel, reason });
            ItemResult::Conflict
        }
        Err(err) => {
            forget_file(&ctx, &item.rel);
            log_transfer(&ctx, &item.rel, "ошибка", &err);
            ItemResult::Failed(err)
        }
    }
}

fn record_conflict(ctx: &Ctx, rel: &str, reason: ConflictReason) {
    tracing::warn!(
        rel,
        reason = reason.label(),
        "конфликт, файл не перезаписан"
    );
    let mut ui = lock_ui(&ctx.ui);
    ui.conflicts.push(crate::ui_model::ConflictNote {
        rel: rel.to_string(),
        reason: reason.label().to_string(),
    });
}

pub async fn upload_file(ctx: &Ctx, item: &Planned) -> Result<(), TransferError> {
    let rel = &item.rel;
    let local_path = local_target(&ctx.workdir, rel).map_err(TransferError::Message)?;
    let remote_key = format!("{}{rel}", ctx.prefix);
    let mut row = match db_call(ctx, |db| db.transfer(rel))? {
        Some(existing) => {
            if !Path::new(&existing.snapshot_path).is_file() {
                return Err(TransferError::Message(format!(
                    "снимок {rel} отсутствует, сессия загрузки сохранена"
                )));
            }
            existing
        }
        None => create_snapshot(ctx, rel, &local_path, &remote_key)?,
    };
    check_upload_size(row.snapshot_size).map_err(|err| TransferError::Message(err.to_string()))?;
    begin_file(ctx, rel, Direction::Upload, row.snapshot_size, 0);
    tracing::info!(rel, bytes = row.snapshot_size, "отправка");

    if !use_multipart(row.snapshot_size) {
        let etag = put_small(ctx, &row).await?;
        finish_upload(ctx, &row, &etag)?;
        return Ok(());
    }

    if row.state == "indeterminate" && row.checks >= 1 {
        match resolve_expired(ctx, &row, true).await? {
            Resolved::Done => return Ok(()),
            Resolved::Continue(updated) => row = updated,
        }
    }

    if row.upload_id.is_none() {
        let upload_id = retry_meta(ctx, rel, "create_multipart", || {
            let store = Arc::clone(&ctx.store);
            let key = remote_key.clone();
            Box::pin(async move { store.create_multipart(&key).await })
        })
        .await?;
        db_call(ctx, |db| db.set_upload_id(rel, &upload_id))?;
        row.upload_id = Some(upload_id);
        row.state = "uploading".into();
        tracing::info!(
            rel,
            upload_id = row.upload_id.as_deref().unwrap_or(""),
            "сессия multipart записана"
        );
    }

    let upload_id = row.upload_id.clone().unwrap_or_default();
    match sync_parts(ctx, &row, &upload_id).await {
        Ok(()) => {}
        Err(TransferError::Store(StoreError::NoSuchUpload { .. })) => {
            let allow_new = row.state == "indeterminate" && row.checks >= 1;
            match resolve_expired(ctx, &row, allow_new).await? {
                Resolved::Done => return Ok(()),
                Resolved::Continue(updated) => {
                    if updated.upload_id.as_deref() != Some(upload_id.as_str()) {
                        return upload_parts_and_complete(ctx, &updated).await;
                    }
                }
            }
        }
        Err(err) => return Err(err),
    }
    upload_parts_and_complete(ctx, &row).await
}

async fn upload_parts_and_complete(ctx: &Ctx, row: &TransferRow) -> Result<(), TransferError> {
    let upload_id = row
        .upload_id
        .clone()
        .ok_or_else(|| TransferError::Message(format!("нет upload id для {}", row.rel)))?;
    let total = part_count(row.snapshot_size) as i32;
    let have = db_call(ctx, |db| db.load_parts(&row.rel))?;
    for part in have.values() {
        credit(ctx, &row.rel, part.part_number, part.size);
    }
    let missing: Vec<i32> = (1..=total)
        .filter(|number| !have.contains_key(number))
        .collect();
    upload_missing(ctx, row, &upload_id, missing).await?;
    db_call(ctx, |db| db.set_state(&row.rel, "completing"))?;
    let parts = db_call(ctx, |db| db.load_parts(&row.rel))?;
    let listed: Vec<(i32, String)> = parts
        .into_iter()
        .map(|(number, part)| (number, part.etag))
        .collect();
    let key = row.remote_key.clone();
    let rel = row.rel.clone();
    let complete = retry_meta(ctx, &rel, "complete_multipart", || {
        let store = Arc::clone(&ctx.store);
        let key = key.clone();
        let upload_id = upload_id.clone();
        let listed = listed.clone();
        Box::pin(async move { store.complete_multipart(&key, &upload_id, listed).await })
    })
    .await;
    match complete {
        Ok(etag) => finish_upload(ctx, row, &etag),
        Err(TransferError::Store(StoreError::NoSuchUpload { .. }))
        | Err(TransferError::Store(StoreError::Network { .. })) => {
            resolve_lost_complete(ctx, row).await
        }
        Err(err) => Err(err),
    }
}

async fn resolve_lost_complete(ctx: &Ctx, row: &TransferRow) -> Result<(), TransferError> {
    tracing::warn!(rel = %row.rel, "ответ Complete потерян: сначала ListParts и Head, новую сессию не открываю");
    let Some(upload_id) = row.upload_id.clone() else {
        return mark_indeterminate(ctx, row);
    };
    match list_all_parts(ctx.store.as_ref(), &row.remote_key, &upload_id).await {
        Ok(parts) if !parts.is_empty() || row.snapshot_size == 0 => {
            merge_remote_parts(ctx, &row.rel, &parts)?;
            let listed = db_call(ctx, |db| db.load_parts(&row.rel))?
                .into_iter()
                .map(|(number, part)| (number, part.etag))
                .collect::<Vec<_>>();
            let etag = retry_meta(ctx, &row.rel, "complete_multipart", || {
                let store = Arc::clone(&ctx.store);
                let key = row.remote_key.clone();
                let upload_id = upload_id.clone();
                let listed = listed.clone();
                Box::pin(async move { store.complete_multipart(&key, &upload_id, listed).await })
            })
            .await?;
            finish_upload(ctx, row, &etag)
        }
        Ok(_) => mark_indeterminate(ctx, row),
        Err(StoreError::NoSuchUpload { .. }) => match ctx.store.head(&row.remote_key).await {
            Ok(head) if head.size == row.snapshot_size => {
                tracing::info!(rel = %row.rel, "результат Complete восстановлен по HeadObject");
                finish_upload(ctx, row, &head.etag)
            }
            Ok(_) | Err(StoreError::NotFound { .. }) => mark_indeterminate(ctx, row),
            Err(err) => Err(err.into()),
        },
        Err(err) => Err(err.into()),
    }
}

enum Resolved {
    Done,
    Continue(TransferRow),
}

async fn resolve_expired(
    ctx: &Ctx,
    row: &TransferRow,
    allow_new: bool,
) -> Result<Resolved, TransferError> {
    if let Some(upload_id) = row.upload_id.clone() {
        match list_all_parts(ctx.store.as_ref(), &row.remote_key, &upload_id).await {
            Ok(parts) => {
                merge_remote_parts(ctx, &row.rel, &parts)?;
                let updated = db_call(ctx, |db| db.transfer(&row.rel))?
                    .ok_or_else(|| TransferError::Message("сессия исчезла из индекса".into()))?;
                return Ok(Resolved::Continue(updated));
            }
            Err(StoreError::NoSuchUpload { .. }) => {}
            Err(err) => return Err(err.into()),
        }
    }
    match ctx.store.head(&row.remote_key).await {
        Ok(head)
            if head.size == row.snapshot_size
                && row.state != "uploading"
                && row.state != "prepared" =>
        {
            finish_upload(ctx, row, &head.etag)?;
            return Ok(Resolved::Done);
        }
        Ok(_) | Err(StoreError::NotFound { .. }) if allow_new => {
            tracing::info!(rel = %row.rel, "сессия мертва, после проверки открываю новую");
            db_call(ctx, |db| {
                for (number, _) in db.load_parts(&row.rel)? {
                    db.delete_part(&row.rel, number)?;
                }
                let mut fresh = row.clone();
                fresh.upload_id = None;
                fresh.state = "prepared".into();
                fresh.checks = row.checks;
                db.upsert_transfer(&fresh)?;
                Ok(fresh)
            })?;
            let upload_id = retry_meta(ctx, &row.rel, "create_multipart", || {
                let store = Arc::clone(&ctx.store);
                let key = row.remote_key.clone();
                Box::pin(async move { store.create_multipart(&key).await })
            })
            .await?;
            db_call(ctx, |db| db.set_upload_id(&row.rel, &upload_id))?;
            let updated = db_call(ctx, |db| db.transfer(&row.rel))?
                .ok_or_else(|| TransferError::Message("не удалось записать новую сессию".into()))?;
            Ok(Resolved::Continue(updated))
        }
        Ok(_) | Err(StoreError::NotFound { .. }) => {
            mark_indeterminate(ctx, row)?;
            Err(TransferError::Indeterminate {
                rel: row.rel.clone(),
            })
        }
        Err(err) => Err(err.into()),
    }
}

fn mark_indeterminate(ctx: &Ctx, row: &TransferRow) -> Result<(), TransferError> {
    db_call(ctx, |db| db.bump_checks(&row.rel))?;
    tracing::warn!(rel = %row.rel, "результат загрузки неопределён, новая сессия не открыта");
    Err(TransferError::Indeterminate {
        rel: row.rel.clone(),
    })
}

fn merge_remote_parts(
    ctx: &Ctx,
    rel: &str,
    parts: &[crate::store::PartInfo],
) -> Result<(), TransferError> {
    db_call(ctx, |db| {
        let local = db.load_parts(rel)?;
        for (number, _) in &local {
            if !parts.iter().any(|part| part.number == *number) {
                db.delete_part(rel, *number)?;
            }
        }
        for part in parts {
            db.upsert_part(
                rel,
                &PartRow {
                    part_number: part.number,
                    size: part.size,
                    etag: part.etag.clone(),
                },
            )?;
        }
        Ok(())
    })
}

async fn sync_parts(ctx: &Ctx, row: &TransferRow, upload_id: &str) -> Result<(), TransferError> {
    let parts = list_all_parts(ctx.store.as_ref(), &row.remote_key, upload_id).await?;
    merge_remote_parts(ctx, &row.rel, &parts)
}

async fn upload_missing(
    ctx: &Ctx,
    row: &TransferRow,
    upload_id: &str,
    missing: Vec<i32>,
) -> Result<(), TransferError> {
    let mut pending = missing.into_iter();
    let mut active: tokio::task::JoinSet<Result<(), TransferError>> = tokio::task::JoinSet::new();
    let mut first_error: Option<TransferError> = None;
    loop {
        while first_error.is_none() && active.len() < 2 {
            let Some(number) = pending.next() else { break };
            if ctx.slots.stopped() || ctx.cancel.load(Ordering::SeqCst) {
                break;
            }
            let ctx = ctx.clone();
            let row = row.clone();
            let upload_id = upload_id.to_string();
            active.spawn(async move { upload_one_part(&ctx, &row, &upload_id, number).await });
        }
        if active.is_empty() {
            break;
        }
        match active.join_next().await {
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(err))) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Some(Err(err)) => {
                if first_error.is_none() {
                    first_error = Some(TransferError::Message(err.to_string()));
                }
            }
            None => break,
        }
    }
    match first_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

async fn upload_one_part(
    ctx: &Ctx,
    row: &TransferRow,
    upload_id: &str,
    number: i32,
) -> Result<(), TransferError> {
    let path = row.snapshot_path.clone();
    let total = row.snapshot_size;
    let part_size = row.part_size;
    let bytes =
        tokio::task::spawn_blocking(move || read_part(Path::new(&path), number, part_size, total))
            .await
            .map_err(|err| TransferError::Message(err.to_string()))?
            .map_err(|err| TransferError::Message(err.to_string()))?;
    let len = bytes.len() as u64;
    let etag = retry_data(ctx, &row.rel, "upload_part", || {
        let store = Arc::clone(&ctx.store);
        let key = row.remote_key.clone();
        let upload_id = upload_id.to_string();
        let body = bytes.clone();
        Box::pin(async move { store.upload_part(&key, &upload_id, number, body).await })
    })
    .await?;
    db_call(ctx, |db| {
        db.upsert_part(
            &row.rel,
            &PartRow {
                part_number: number,
                size: len,
                etag,
            },
        )
    })?;
    credit(ctx, &row.rel, number, len);
    tracing::info!(rel = %row.rel, part = number, bytes = len, "multipart часть подтверждена");
    Ok(())
}

async fn put_small(ctx: &Ctx, row: &TransferRow) -> Result<String, TransferError> {
    let path = row.snapshot_path.clone();
    let bytes = tokio::task::spawn_blocking(move || std::fs::read(&path))
        .await
        .map_err(|err| TransferError::Message(err.to_string()))?
        .map_err(|err| TransferError::Message(err.to_string()))?;
    let etag = retry_data(ctx, &row.rel, "put_object", || {
        let store = Arc::clone(&ctx.store);
        let key = row.remote_key.clone();
        let body = bytes.clone();
        Box::pin(async move { store.put_object(&key, body).await })
    })
    .await?;
    credit(ctx, &row.rel, 0, row.snapshot_size);
    Ok(etag)
}

fn finish_upload(ctx: &Ctx, row: &TransferRow, etag: &str) -> Result<(), TransferError> {
    let file = FileRow {
        rel: row.rel.clone(),
        size: row.snapshot_size,
        sha256: row.snapshot_sha256.clone(),
        remote_etag: crate::paths::normalize_etag(etag),
        remote_size: row.snapshot_size,
    };
    db_call(ctx, |db| db.commit_success(&file))?;
    let _ = std::fs::remove_file(&row.snapshot_path);
    end_file(ctx, &row.rel);
    tracing::info!(rel = %row.rel, bytes = row.snapshot_size, "отправка завершена");
    Ok(())
}

fn create_snapshot(
    ctx: &Ctx,
    rel: &str,
    source: &Path,
    remote_key: &str,
) -> Result<TransferRow, TransferError> {
    let dir = ctx.workdir.join(".stand-sync").join("snapshots");
    std::fs::create_dir_all(&dir).map_err(|err| TransferError::Message(err.to_string()))?;
    let id = sha256_bytes(rel.as_bytes());
    let partial = dir.join(format!("{id}.partial"));
    let final_path = dir.join(&id);
    copy_file(source, &partial)
        .map_err(|err| TransferError::Message(format!("снимок {rel}: {err}")))?;
    let sha = sha256_file(&partial).map_err(|err| TransferError::Message(err.to_string()))?;
    let size = std::fs::metadata(&partial)
        .map_err(|err| TransferError::Message(err.to_string()))?
        .len();
    std::fs::rename(&partial, &final_path)
        .map_err(|err| TransferError::Message(err.to_string()))?;
    let row = TransferRow {
        rel: rel.to_string(),
        upload_id: None,
        snapshot_path: final_path.display().to_string(),
        snapshot_sha256: sha,
        snapshot_size: size,
        part_size: PART_SIZE,
        state: "prepared".into(),
        remote_key: remote_key.to_string(),
        checks: 0,
    };
    db_call(ctx, |db| db.upsert_transfer(&row))?;
    Ok(row)
}

pub async fn download_file(ctx: &Ctx, item: &Planned) -> Result<(), TransferError> {
    let rel = &item.rel;
    let remote = item
        .remote
        .as_ref()
        .ok_or_else(|| TransferError::Message(format!("нет удалённого объекта {rel}")))?;
    let remote_key = format!("{}{rel}", ctx.prefix);
    let target = local_target(&ctx.workdir, rel).map_err(TransferError::Message)?;
    let before = item.local.as_ref().map(|file| file.sha256.clone());
    begin_file(ctx, rel, Direction::Download, remote.size, 0);
    tracing::info!(rel, bytes = remote.size, "скачивание");
    let temp = temp_download_path(&ctx.workdir);
    let mut guard = TempFile {
        path: temp.clone(),
        keep: false,
    };
    let ctx_rel = rel.to_string();
    let ui = Arc::clone(&ctx.ui);
    let progress = Arc::new(move |done: u64| {
        let mut model = lock_ui(&ui);
        if let Some(file) = model.current.iter_mut().find(|file| file.rel == ctx_rel) {
            file.done = done;
        }
    });
    let downloaded = retry_data(ctx, rel, "get_object", || {
        let store = Arc::clone(&ctx.store);
        let key = remote_key.clone();
        let dest = temp.clone();
        let progress = Arc::clone(&progress);
        Box::pin(async move { store.get_to(&key, &dest, progress).await })
    })
    .await?;
    if downloaded.bytes_written != remote.size {
        return Err(TransferError::Message(format!(
            "{rel}: скачано {} байт, в листинге {}",
            downloaded.bytes_written, remote.size
        )));
    }
    let temp_sha = sha256_file(&temp).map_err(|err| TransferError::Message(err.to_string()))?;
    let current = if target.is_file() {
        Some(sha256_file(&target).map_err(|err| TransferError::Message(err.to_string()))?)
    } else {
        None
    };
    if current != before {
        tracing::warn!(
            rel,
            "локальный файл изменился во время скачивания, конфликт"
        );
        return Err(TransferError::Conflict {
            rel: rel.to_string(),
            reason: "изменён во время скачивания".into(),
        });
    }
    let etag = if downloaded.etag.is_empty() {
        remote.etag.clone()
    } else {
        downloaded.etag.clone()
    };
    let file = FileRow {
        rel: rel.to_string(),
        size: downloaded.bytes_written,
        sha256: temp_sha,
        remote_etag: crate::paths::normalize_etag(&etag),
        remote_size: remote.size,
    };
    db_call(ctx, |db| db.insert_pending(&file))?;
    replace_file(&temp, &target).map_err(|err| TransferError::Message(format!("{rel}: {err}")))?;
    guard.keep = true;
    db_call(ctx, |db| db.commit_success(&file))?;
    credit(ctx, rel, 0, file.size);
    end_file(ctx, rel);
    tracing::info!(rel, bytes = file.size, "скачивание завершено");
    Ok(())
}

fn copy_file(from: &Path, to: &Path) -> std::io::Result<()> {
    let mut input = std::fs::File::open(from)?;
    let mut output = std::fs::File::create(to)?;
    std::io::copy(&mut input, &mut output)?;
    output.sync_all()?;
    Ok(())
}

fn read_part(path: &Path, number: i32, part_size: u64, total: u64) -> std::io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let start = (number as u64 - 1) * part_size;
    if start > total {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "часть вне файла",
        ));
    }
    let len = part_size.min(total - start) as usize;
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

struct TempFile {
    path: PathBuf,
    keep: bool,
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn temp_download_path(workdir: &Path) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let dir = workdir.join(".stand-sync").join("tmp");
    let _ = std::fs::create_dir_all(&dir);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    dir.join(format!("{n}.partial"))
}

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

async fn retry_data<T>(
    ctx: &Ctx,
    rel: &str,
    op: &str,
    mut make: impl FnMut() -> BoxFut<Result<T, StoreError>>,
) -> Result<T, TransferError> {
    retry_loop(ctx, rel, op, true, &mut make).await
}

async fn retry_meta<T>(
    ctx: &Ctx,
    rel: &str,
    op: &str,
    mut make: impl FnMut() -> BoxFut<Result<T, StoreError>>,
) -> Result<T, TransferError> {
    retry_loop(ctx, rel, op, false, &mut make).await
}

async fn retry_loop<T>(
    ctx: &Ctx,
    rel: &str,
    op: &str,
    data_slot: bool,
    make: &mut impl FnMut() -> BoxFut<Result<T, StoreError>>,
) -> Result<T, TransferError> {
    let mut attempt = 0u32;
    loop {
        if ctx.cancel.load(Ordering::SeqCst) {
            return Err(TransferError::Cancelled);
        }
        if ctx.slots.stopped() {
            return Err(TransferError::Stopped);
        }
        let permit = if data_slot {
            Some(acquire_cancel(ctx, rel).await?)
        } else {
            None
        };
        let result = make().await;
        match result {
            Ok(value) => {
                if let Some(permit) = permit {
                    permit.release_ok();
                }
                clear_wait(ctx);
                set_degraded(ctx);
                return Ok(value);
            }
            Err(err) if err.is_forbidden() => {
                if let Some(permit) = permit {
                    permit.release_neutral();
                }
                ctx.slots.stop();
                log_transfer(ctx, rel, op, &TransferError::Store(err));
                return Err(TransferError::Stopped);
            }
            Err(err) if err.retryable() => {
                if let Some(permit) = permit {
                    permit.release_network();
                } else {
                    ctx.slots.observe_network_error();
                }
                attempt += 1;
                let delay = (ctx.backoff)(attempt);
                let line = format!(
                    "rel={rel} op={op} повтор {attempt} через {} мс status={:?} code={} request_id={} {}",
                    delay.as_millis(),
                    err.http_status(),
                    err.s3_code().unwrap_or(""),
                    err.request_id().unwrap_or(""),
                    error_chain(&err)
                );
                ctx.log.push_error(&line);
                tracing::warn!("{line}");
                set_degraded(ctx);
                if attempt >= MAX_ATTEMPTS {
                    return Err(TransferError::Store(err));
                }
                show_wait(ctx, delay);
                sleep_cancel(ctx, delay).await?;
                clear_wait(ctx);
            }
            Err(err) => {
                if let Some(permit) = permit {
                    permit.release_neutral();
                }
                return Err(TransferError::Store(err));
            }
        }
    }
}

async fn acquire_cancel(ctx: &Ctx, rel: &str) -> Result<crate::slots::Permit, TransferError> {
    tokio::select! {
        permit = ctx.slots.acquire(rel) => permit.map_err(|_| TransferError::Stopped),
        _ = wait_cancel(&ctx.cancel) => Err(TransferError::Cancelled),
    }
}

async fn wait_cancel(cancel: &AtomicBool) {
    loop {
        if cancel.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

async fn sleep_cancel(ctx: &Ctx, delay: Duration) -> Result<(), TransferError> {
    let step = Duration::from_millis(40);
    let mut left = delay;
    while !left.is_zero() {
        if ctx.cancel.load(Ordering::SeqCst) {
            return Err(TransferError::Cancelled);
        }
        let slice = left.min(step);
        tokio::time::sleep(slice).await;
        left = left.saturating_sub(slice);
    }
    Ok(())
}

fn show_wait(ctx: &Ctx, delay: Duration) {
    lock_ui(&ctx.ui).waiting_until = Some(std::time::Instant::now() + delay);
}

fn clear_wait(ctx: &Ctx) {
    lock_ui(&ctx.ui).waiting_until = None;
}

fn set_degraded(ctx: &Ctx) {
    lock_ui(&ctx.ui).degraded = ctx.slots.max() == 1;
}

fn begin_file(ctx: &Ctx, rel: &str, direction: Direction, total: u64, done: u64) {
    let mut ui = lock_ui(&ctx.ui);
    ui.current.retain(|file| file.rel != rel);
    if ui.current.len() < 2 {
        ui.current.push(CurrentFile {
            rel: rel.to_string(),
            direction,
            done,
            total,
        });
    }
    ui.degraded = ctx.slots.max() == 1;
    let active = ui
        .current
        .iter()
        .map(|file| format!("{} {}", file.direction.label(), file.rel))
        .collect();
    drop(ui);
    ctx.log.set_active(active);
}

fn forget_file(ctx: &Ctx, rel: &str) {
    let mut ui = lock_ui(&ctx.ui);
    ui.current.retain(|file| file.rel != rel);
    let active = ui
        .current
        .iter()
        .map(|file| format!("{} {}", file.direction.label(), file.rel))
        .collect();
    drop(ui);
    ctx.log.set_active(active);
}

fn end_file(ctx: &Ctx, rel: &str) {
    let mut ui = lock_ui(&ctx.ui);
    ui.current.retain(|file| file.rel != rel);
    ui.files_done = ui.files_done.saturating_add(1);
    ui.bytes_done = ctx
        .ledger
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .bytes_done()
        .min(ui.bytes_total);
    ui.degraded = ctx.slots.max() == 1;
    let active = ui
        .current
        .iter()
        .map(|file| format!("{} {}", file.direction.label(), file.rel))
        .collect();
    drop(ui);
    ctx.log.set_active(active);
}

fn credit(ctx: &Ctx, rel: &str, token: i32, bytes: u64) {
    let (global, file_done) = ctx
        .ledger
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .confirm(rel, token, bytes);
    let mut ui = lock_ui(&ctx.ui);
    ui.bytes_done = global.min(ui.bytes_total);
    if let Some(file) = ui.current.iter_mut().find(|file| file.rel == rel) {
        file.done = file_done;
    }
}

fn db_call<T>(
    ctx: &Ctx,
    f: impl FnOnce(&Index) -> Result<T, crate::index::DbError>,
) -> Result<T, TransferError> {
    let db = ctx.db.lock().unwrap_or_else(|err| err.into_inner());
    f(&db).map_err(TransferError::from)
}

fn lock_ui(ui: &Mutex<UiModel>) -> std::sync::MutexGuard<'_, UiModel> {
    ui.lock().unwrap_or_else(|err| err.into_inner())
}

fn log_transfer(ctx: &Ctx, rel: &str, op: &str, err: &TransferError) {
    let (status, code, request_id) = match err {
        TransferError::Store(store) => (
            store.http_status().unwrap_or(0),
            store.s3_code().unwrap_or("").to_string(),
            store.request_id().unwrap_or("").to_string(),
        ),
        _ => (0, String::new(), String::new()),
    };
    let line = format!(
        "rel={rel} op={op} status={status} code={code} request_id={request_id} {}",
        error_chain(err)
    );
    ctx.log.push_error(&line);
    tracing::warn!("{line}");
}
