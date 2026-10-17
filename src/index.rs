use crate::config::Profile;
use crate::paths::{sha256_file, SQLITE_NAME};
use crate::plan::IndexFile;
use rusqlite::{params, Connection};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS profile (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    endpoint TEXT NOT NULL,
    bucket TEXT NOT NULL,
    stand TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
    rel TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    remote_etag TEXT NOT NULL,
    remote_size INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS transfers (
    rel TEXT PRIMARY KEY,
    upload_id TEXT,
    snapshot_path TEXT NOT NULL,
    snapshot_sha256 TEXT NOT NULL,
    snapshot_size INTEGER NOT NULL,
    part_size INTEGER NOT NULL,
    state TEXT NOT NULL,
    remote_key TEXT NOT NULL,
    checks INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS parts (
    rel TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    PRIMARY KEY (rel, part_number),
    FOREIGN KEY (rel) REFERENCES transfers(rel) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS pending_files (
    rel TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    remote_etag TEXT NOT NULL,
    remote_size INTEGER NOT NULL
);
"#;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRow {
    pub rel: String,
    pub size: u64,
    pub sha256: String,
    pub remote_etag: String,
    pub remote_size: u64,
}

impl From<FileRow> for IndexFile {
    fn from(row: FileRow) -> Self {
        Self {
            size: row.size,
            sha256: row.sha256,
            remote_etag: row.remote_etag,
            remote_size: row.remote_size,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferRow {
    pub rel: String,
    pub upload_id: Option<String>,
    pub snapshot_path: String,
    pub snapshot_sha256: String,
    pub snapshot_size: u64,
    pub part_size: u64,
    pub state: String,
    pub remote_key: String,
    pub checks: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartRow {
    pub part_number: i32,
    pub size: u64,
    pub etag: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error(
        "config указывает другой профиль (stand {found_stand}, бакет {found_bucket}, endpoint {found_endpoint}). Индекс этой папки принадлежит другому стенду и не применяется. Файлы не удалены."
    )]
    ProfileMismatch {
        found_stand: String,
        found_bucket: String,
        found_endpoint: String,
    },
    #[error("индекс: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("индекс: {0}")]
    Message(String),
}

pub struct Index {
    conn: Connection,
}

impl Index {
    pub fn path_in(workdir: &Path) -> PathBuf {
        workdir.join(SQLITE_NAME)
    }

    pub fn open(path: &Path, expected: &Profile) -> Result<Self, DbError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| DbError::Message(err.to_string()))?;
        }
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(SCHEMA)?;
        let existing = load_profile(&conn)?;
        match existing {
            Some(found) if profiles_equal(&found, expected) => {}
            Some(found) => {
                return Err(DbError::ProfileMismatch {
                    found_stand: found.stand,
                    found_bucket: found.bucket,
                    found_endpoint: found.endpoint,
                });
            }
            None => {
                conn.execute(
                    "INSERT INTO profile (id, endpoint, bucket, stand) VALUES (1, ?1, ?2, ?3)",
                    params![expected.endpoint, expected.bucket, expected.stand],
                )?;
            }
        }
        Ok(Self { conn })
    }

    pub fn profile(&self) -> Result<Profile, DbError> {
        load_profile(&self.conn)?.ok_or_else(|| DbError::Message("в индексе нет профиля".into()))
    }

    pub fn load_files(&self) -> Result<BTreeMap<String, IndexFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT rel, size, sha256, remote_etag, remote_size FROM files ORDER BY rel",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                IndexFile {
                    size: row.get::<_, i64>(1)? as u64,
                    sha256: row.get(2)?,
                    remote_etag: row.get(3)?,
                    remote_size: row.get::<_, i64>(4)? as u64,
                },
            ))
        })?;
        let mut out = BTreeMap::new();
        for row in rows {
            let (rel, file) = row?;
            out.insert(rel, file);
        }
        Ok(out)
    }

    pub fn load_transfers(&self) -> Result<Vec<TransferRow>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT rel, upload_id, snapshot_path, snapshot_sha256, snapshot_size, part_size, state, remote_key, checks FROM transfers ORDER BY rel",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(TransferRow {
                rel: row.get(0)?,
                upload_id: row.get(1)?,
                snapshot_path: row.get(2)?,
                snapshot_sha256: row.get(3)?,
                snapshot_size: row.get::<_, i64>(4)? as u64,
                part_size: row.get::<_, i64>(5)? as u64,
                state: row.get(6)?,
                remote_key: row.get(7)?,
                checks: row.get(8)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(DbError::from)
    }

    pub fn transfer(&self, rel: &str) -> Result<Option<TransferRow>, DbError> {
        Ok(self
            .load_transfers()?
            .into_iter()
            .find(|row| row.rel == rel))
    }

    pub fn upsert_transfer(&self, row: &TransferRow) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO transfers (rel, upload_id, snapshot_path, snapshot_sha256, snapshot_size, part_size, state, remote_key, checks)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(rel) DO UPDATE SET
               upload_id = excluded.upload_id,
               snapshot_path = excluded.snapshot_path,
               snapshot_sha256 = excluded.snapshot_sha256,
               snapshot_size = excluded.snapshot_size,
               part_size = excluded.part_size,
               state = excluded.state,
               remote_key = excluded.remote_key,
               checks = excluded.checks",
            params![
                row.rel,
                row.upload_id,
                row.snapshot_path,
                row.snapshot_sha256,
                row.snapshot_size as i64,
                row.part_size as i64,
                row.state,
                row.remote_key,
                row.checks
            ],
        )?;
        Ok(())
    }

    pub fn set_upload_id(&self, rel: &str, upload_id: &str) -> Result<(), DbError> {
        let changed = self.conn.execute(
            "UPDATE transfers SET upload_id = ?1, state = 'uploading' WHERE rel = ?2",
            params![upload_id, rel],
        )?;
        if changed != 1 {
            return Err(DbError::Message(format!("нет сессии загрузки для {rel}")));
        }
        Ok(())
    }

    pub fn set_state(&self, rel: &str, state: &str) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE transfers SET state = ?1 WHERE rel = ?2",
            params![state, rel],
        )?;
        Ok(())
    }

    pub fn bump_checks(&self, rel: &str) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE transfers SET checks = checks + 1, state = 'indeterminate' WHERE rel = ?1",
            params![rel],
        )?;
        Ok(())
    }

    pub fn load_parts(&self, rel: &str) -> Result<BTreeMap<i32, PartRow>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT part_number, size, etag FROM parts WHERE rel = ?1 ORDER BY part_number",
        )?;
        let rows = stmt.query_map(params![rel], |row| {
            Ok(PartRow {
                part_number: row.get(0)?,
                size: row.get::<_, i64>(1)? as u64,
                etag: row.get(2)?,
            })
        })?;
        let mut out = BTreeMap::new();
        for row in rows {
            let part = row?;
            out.insert(part.part_number, part);
        }
        Ok(out)
    }

    pub fn upsert_part(&self, rel: &str, part: &PartRow) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO parts (rel, part_number, size, etag) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(rel, part_number) DO UPDATE SET size = excluded.size, etag = excluded.etag",
            params![rel, part.part_number, part.size as i64, part.etag],
        )?;
        Ok(())
    }

    pub fn delete_part(&self, rel: &str, part_number: i32) -> Result<(), DbError> {
        self.conn.execute(
            "DELETE FROM parts WHERE rel = ?1 AND part_number = ?2",
            params![rel, part_number],
        )?;
        Ok(())
    }

    pub fn forget(&self, rel: &str) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("DELETE FROM files WHERE rel = ?1", params![rel])?;
        tx.execute("DELETE FROM transfers WHERE rel = ?1", params![rel])?;
        tx.execute("DELETE FROM parts WHERE rel = ?1", params![rel])?;
        tx.execute("DELETE FROM pending_files WHERE rel = ?1", params![rel])?;
        tx.commit()?;
        Ok(())
    }

    pub fn commit_success(&self, file: &FileRow) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO files (rel, size, sha256, remote_etag, remote_size) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(rel) DO UPDATE SET
               size = excluded.size,
               sha256 = excluded.sha256,
               remote_etag = excluded.remote_etag,
               remote_size = excluded.remote_size",
            params![
                file.rel,
                file.size as i64,
                file.sha256,
                file.remote_etag,
                file.remote_size as i64
            ],
        )?;
        tx.execute("DELETE FROM parts WHERE rel = ?1", params![file.rel])?;
        tx.execute("DELETE FROM transfers WHERE rel = ?1", params![file.rel])?;
        tx.execute(
            "DELETE FROM pending_files WHERE rel = ?1",
            params![file.rel],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn insert_pending(&self, file: &FileRow) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO pending_files (rel, size, sha256, remote_etag, remote_size) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(rel) DO UPDATE SET
               size = excluded.size,
               sha256 = excluded.sha256,
               remote_etag = excluded.remote_etag,
               remote_size = excluded.remote_size",
            params![
                file.rel,
                file.size as i64,
                file.sha256,
                file.remote_etag,
                file.remote_size as i64
            ],
        )?;
        Ok(())
    }

    pub fn recover_pending(&self, workdir: &Path) -> Result<u32, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT rel, size, sha256, remote_etag, remote_size FROM pending_files")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(FileRow {
                    rel: row.get(0)?,
                    size: row.get::<_, i64>(1)? as u64,
                    sha256: row.get(2)?,
                    remote_etag: row.get(3)?,
                    remote_size: row.get::<_, i64>(4)? as u64,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut applied = 0u32;
        for row in rows {
            let path = match crate::paths::local_target(workdir, &row.rel) {
                Ok(path) => path,
                Err(_) => {
                    self.conn
                        .execute("DELETE FROM pending_files WHERE rel = ?1", params![row.rel])?;
                    continue;
                }
            };
            let matches = path.is_file()
                && sha256_file(&path).ok().as_deref() == Some(row.sha256.as_str())
                && path
                    .metadata()
                    .map(|meta| meta.len() == row.size)
                    .unwrap_or(false);
            if matches {
                self.commit_success(&row)?;
                applied += 1;
            } else {
                self.conn
                    .execute("DELETE FROM pending_files WHERE rel = ?1", params![row.rel])?;
            }
        }
        Ok(applied)
    }
}

fn load_profile(conn: &Connection) -> Result<Option<Profile>, DbError> {
    let mut stmt = conn.prepare("SELECT endpoint, bucket, stand FROM profile WHERE id = 1")?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        Ok(Some(Profile {
            endpoint: row.get(0)?,
            bucket: row.get(1)?,
            stand: row.get(2)?,
        }))
    } else {
        Ok(None)
    }
}

fn profiles_equal(left: &Profile, right: &Profile) -> bool {
    left.endpoint == right.endpoint && left.bucket == right.bucket && left.stand == right.stand
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(stand: &str) -> Profile {
        Profile {
            endpoint: "https://s3.twcstorage.ru".into(),
            bucket: "bucket".into(),
            stand: stand.into(),
        }
    }

    #[test]
    fn mismatch_does_not_replace_profile_or_rows() {
        let dir = crate::testutil::temp_dir("index-mismatch");
        let path = Index::path_in(&dir);
        {
            let index = Index::open(&path, &profile("dayna")).unwrap();
            index
                .commit_success(&FileRow {
                    rel: "sprites/queen.png".into(),
                    size: 4,
                    sha256: "abc".into(),
                    remote_etag: "etag".into(),
                    remote_size: 4,
                })
                .unwrap();
        }
        let err = match Index::open(&path, &profile("other")) {
            Err(err) => err,
            Ok(_) => panic!("ожидался отказ: индекс принадлежит другому стенду"),
        };
        assert!(matches!(err, DbError::ProfileMismatch { .. }));
        let index = Index::open(&path, &profile("dayna")).unwrap();
        assert_eq!(index.profile().unwrap().stand, "dayna");
        assert!(index
            .load_files()
            .unwrap()
            .contains_key("sprites/queen.png"));
        assert!(path.is_file());
    }

    #[test]
    fn scan_equivalent_read_does_not_change_files() {
        let dir = crate::testutil::temp_dir("index-readonly");
        let path = Index::path_in(&dir);
        let index = Index::open(&path, &profile("dayna")).unwrap();
        let before = index.load_files().unwrap();
        let _ = index.load_files().unwrap();
        let _ = index.load_transfers().unwrap();
        assert_eq!(index.load_files().unwrap(), before);
    }

    #[test]
    fn pending_download_is_applied_only_when_file_matches() {
        let dir = crate::testutil::temp_dir("index-pending");
        let path = Index::path_in(&dir);
        let index = Index::open(&path, &profile("dayna")).unwrap();
        let body = b"queen";
        let rel = "sprites/queen.png";
        let target = dir.join("sprites").join("queen.png");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, body).unwrap();
        let sha = crate::paths::sha256_bytes(body);
        index
            .insert_pending(&FileRow {
                rel: rel.into(),
                size: body.len() as u64,
                sha256: sha,
                remote_etag: "etag".into(),
                remote_size: body.len() as u64,
            })
            .unwrap();
        assert_eq!(index.recover_pending(&dir).unwrap(), 1);
        assert_eq!(
            index.load_files().unwrap().get(rel).unwrap().remote_etag,
            "etag"
        );
    }
}
