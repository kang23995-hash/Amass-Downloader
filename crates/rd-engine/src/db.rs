use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::error::{Error, Result};
use crate::model::{Segment, TaskId, TaskInfo, TaskState};
use crate::util::now_unix;

/// SQLite persistence. All access goes through `call`, which runs the
/// closure on the blocking pool so the async runtime is never blocked.
#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
    _lock: Option<Arc<std::fs::File>>,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        // An engine owns its segment map and file writers exclusively.
        // SQLite alone cannot prevent two engines writing the same part file.
        let mut lock_name = path.as_os_str().to_os_string();
        lock_name.push(".lock");
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(std::path::PathBuf::from(lock_name))?;
        lock.try_lock().map_err(|e| {
            Error::Config(format!(
                "database {} is already in use or cannot be locked: {e}",
                path.display()
            ))
        })?;
        let conn = Connection::open(path)?;
        let mut db = Self::init(conn)?;
        db._lock = Some(Arc::new(lock));
        Ok(db)
    }

    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version < 1 {
            conn.execute_batch(
                "
                CREATE TABLE IF NOT EXISTS tasks (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    url             TEXT NOT NULL,
                    final_url       TEXT,
                    file_name       TEXT NOT NULL,
                    file_path       TEXT NOT NULL,
                    part_path       TEXT NOT NULL,
                    state           TEXT NOT NULL,
                    total_size      INTEGER,
                    downloaded      INTEGER NOT NULL DEFAULT 0,
                    connections     INTEGER NOT NULL,
                    name_locked     INTEGER NOT NULL DEFAULT 0,
                    range_supported INTEGER NOT NULL DEFAULT 0,
                    etag            TEXT,
                    last_modified   TEXT,
                    expected_sha256 TEXT,
                    actual_sha256   TEXT,
                    rate_limit      INTEGER,
                    error           TEXT,
                    restart_count   INTEGER NOT NULL DEFAULT 0,
                    created_at      INTEGER NOT NULL,
                    updated_at      INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS segments (
                    task_id    INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                    idx        INTEGER NOT NULL,
                    start      INTEGER NOT NULL,
                    end        INTEGER,
                    downloaded INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (task_id, idx)
                );
                PRAGMA user_version = 1;
                ",
            )?;
        }
        if version < 2 {
            // v2: BLAKE3 integrity fields and the adaptive-concurrency flag.
            // Additive columns only, so v1 databases upgrade in place without
            // losing tasks.
            conn.execute_batch(
                "
                ALTER TABLE tasks ADD COLUMN expected_blake3 TEXT;
                ALTER TABLE tasks ADD COLUMN actual_blake3 TEXT;
                ALTER TABLE tasks ADD COLUMN auto_connections INTEGER NOT NULL DEFAULT 0;
                PRAGMA user_version = 2;
                ",
            )?;
        }
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            _lock: None,
        })
    }

    pub async fn call<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(Error::from)?;
            f(&guard)
        })
        .await?
    }
}

fn row_to_task(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskInfo> {
    let state_str: String = row.get("state")?;
    let state = TaskState::parse(&state_str).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            format!("unknown task state: {state_str}").into(),
        )
    })?;
    Ok(TaskInfo {
        id: row.get::<_, i64>("id")? as u64,
        url: row.get("url")?,
        final_url: row.get("final_url")?,
        file_name: row.get("file_name")?,
        file_path: row.get::<_, String>("file_path")?.into(),
        part_path: row.get::<_, String>("part_path")?.into(),
        state,
        total_size: row.get::<_, Option<i64>>("total_size")?.map(|v| v as u64),
        downloaded: row.get::<_, i64>("downloaded")? as u64,
        connections: row.get::<_, i64>("connections")? as u32,
        range_supported: row.get::<_, i64>("range_supported")? != 0,
        etag: row.get("etag")?,
        last_modified: row.get("last_modified")?,
        expected_sha256: row.get("expected_sha256")?,
        actual_sha256: row.get("actual_sha256")?,
        expected_blake3: row.get("expected_blake3")?,
        actual_blake3: row.get("actual_blake3")?,
        rate_limit_bps: row.get::<_, Option<i64>>("rate_limit")?.map(|v| v as u64),
        error: row.get("error")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        auto_concurrency: row.get::<_, i64>("auto_connections")? != 0,
        speed_bps: 0,
    })
}

pub fn insert_task(conn: &Connection, t: &TaskInfo, name_locked: bool) -> Result<TaskId> {
    conn.execute(
        "INSERT INTO tasks (url, final_url, file_name, file_path, part_path, state, total_size,
            downloaded, connections, name_locked, range_supported, etag, last_modified,
            expected_sha256, actual_sha256, expected_blake3, actual_blake3, rate_limit, error,
            restart_count, auto_connections, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,0,?20,?21,?22)",
        rusqlite::params![
            t.url,
            t.final_url,
            t.file_name,
            t.file_path.to_string_lossy(),
            t.part_path.to_string_lossy(),
            t.state.as_str(),
            t.total_size.map(|v| v as i64),
            t.downloaded as i64,
            t.connections as i64,
            name_locked as i64,
            t.range_supported as i64,
            t.etag,
            t.last_modified,
            t.expected_sha256,
            t.actual_sha256,
            t.expected_blake3,
            t.actual_blake3,
            t.rate_limit_bps.map(|v| v as i64),
            t.error,
            t.auto_concurrency as i64,
            t.created_at,
            t.updated_at,
        ],
    )?;
    Ok(conn.last_insert_rowid() as u64)
}

pub fn load_name_locked(conn: &Connection) -> Result<std::collections::HashMap<TaskId, bool>> {
    let mut stmt = conn.prepare("SELECT id, name_locked FROM tasks")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? != 0))
    })?;
    let mut out = std::collections::HashMap::new();
    for r in rows {
        let (id, locked) = r?;
        out.insert(id, locked);
    }
    Ok(out)
}

pub fn load_task(conn: &Connection, id: TaskId) -> Result<Option<TaskInfo>> {
    let mut stmt = conn.prepare("SELECT * FROM tasks WHERE id = ?1")?;
    let mut rows = stmt.query([id as i64])?;
    match rows.next()? {
        Some(row) => Ok(Some(row_to_task(row)?)),
        None => Ok(None),
    }
}

pub fn load_all_tasks(conn: &Connection) -> Result<Vec<TaskInfo>> {
    let mut stmt = conn.prepare("SELECT * FROM tasks ORDER BY id")?;
    let rows = stmt.query_map([], row_to_task)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

pub fn update_state(
    conn: &Connection,
    id: TaskId,
    state: TaskState,
    error: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE tasks SET state = ?2, error = ?3, updated_at = ?4 WHERE id = ?1",
        rusqlite::params![id as i64, state.as_str(), error, now_unix()],
    )?;
    Ok(())
}

pub fn update_downloaded(conn: &Connection, id: TaskId, downloaded: u64) -> Result<()> {
    conn.execute(
        "UPDATE tasks SET downloaded = ?2, updated_at = ?3 WHERE id = ?1",
        rusqlite::params![id as i64, downloaded as i64, now_unix()],
    )?;
    Ok(())
}

pub fn update_rate_limit(conn: &Connection, id: TaskId, rate: Option<u64>) -> Result<()> {
    conn.execute(
        "UPDATE tasks SET rate_limit = ?2, updated_at = ?3 WHERE id = ?1",
        rusqlite::params![id as i64, rate.map(|v| v as i64), now_unix()],
    )?;
    Ok(())
}

/// Metadata written by the runner after probing / finalizing.
pub struct MetaUpdate {
    pub final_url: Option<String>,
    pub file_name: String,
    pub file_path: std::path::PathBuf,
    pub part_path: std::path::PathBuf,
    pub total_size: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub range_supported: bool,
    pub connections: u32,
    pub actual_sha256: Option<String>,
    pub actual_blake3: Option<String>,
}

pub fn update_meta(conn: &Connection, id: TaskId, m: &MetaUpdate) -> Result<()> {
    conn.execute(
        "UPDATE tasks SET final_url=?2, file_name=?3, file_path=?4, part_path=?5, total_size=?6,
            etag=?7, last_modified=?8, range_supported=?9, connections=?10, actual_sha256=?11,
            actual_blake3=?12, updated_at=?13 WHERE id=?1",
        rusqlite::params![
            id as i64,
            m.final_url,
            m.file_name,
            m.file_path.to_string_lossy(),
            m.part_path.to_string_lossy(),
            m.total_size.map(|v| v as i64),
            m.etag,
            m.last_modified,
            m.range_supported as i64,
            m.connections as i64,
            m.actual_sha256,
            m.actual_blake3,
            now_unix(),
        ],
    )?;
    Ok(())
}

pub fn bump_restart_count(conn: &Connection, id: TaskId) -> Result<u32> {
    conn.execute(
        "UPDATE tasks SET restart_count = restart_count + 1, updated_at = ?2 WHERE id = ?1",
        rusqlite::params![id as i64, now_unix()],
    )?;
    let count: i64 = conn.query_row(
        "SELECT restart_count FROM tasks WHERE id = ?1",
        [id as i64],
        |r| r.get(0),
    )?;
    Ok(count as u32)
}

pub fn replace_segments(conn: &Connection, id: TaskId, segments: &[Segment]) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    tx.execute("DELETE FROM segments WHERE task_id = ?1", [id as i64])?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO segments (task_id, idx, start, end, downloaded) VALUES (?1,?2,?3,?4,?5)",
        )?;
        for s in segments {
            stmt.execute(rusqlite::params![
                id as i64,
                s.index as i64,
                s.start as i64,
                s.end.map(|v| v as i64),
                s.downloaded as i64,
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Commit all progress rows together: one WAL transaction per checkpoint.
pub fn checkpoint_segments(conn: &Connection, id: TaskId, rows: &[(u32, u64)]) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare_cached(
            "UPDATE segments SET downloaded = ?3 WHERE task_id = ?1 AND idx = ?2",
        )?;
        for &(index, downloaded) in rows {
            stmt.execute(rusqlite::params![
                id as i64,
                index as i64,
                downloaded as i64
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Shrink (or extend) a live segment after a split. Only `end` changes;
/// the segment's written data is untouched.
pub fn update_segment_end(
    conn: &Connection,
    id: TaskId,
    index: u32,
    end: Option<u64>,
) -> Result<()> {
    conn.execute(
        "UPDATE segments SET end = ?3 WHERE task_id = ?1 AND idx = ?2",
        rusqlite::params![id as i64, index as i64, end.map(|v| v as i64)],
    )?;
    Ok(())
}

/// Persist a segment created by a dynamic split at runtime.
pub fn insert_segment(conn: &Connection, id: TaskId, seg: &Segment) -> Result<()> {
    conn.execute(
        "INSERT INTO segments (task_id, idx, start, end, downloaded) VALUES (?1,?2,?3,?4,?5)",
        rusqlite::params![
            id as i64,
            seg.index as i64,
            seg.start as i64,
            seg.end.map(|v| v as i64),
            seg.downloaded as i64,
        ],
    )?;
    Ok(())
}

pub fn reset_segments(conn: &Connection, id: TaskId) -> Result<()> {
    conn.execute(
        "UPDATE segments SET downloaded = 0 WHERE task_id = ?1",
        [id as i64],
    )?;
    Ok(())
}

pub fn load_segments(conn: &Connection, id: TaskId) -> Result<Vec<Segment>> {
    let mut stmt = conn.prepare(
        "SELECT idx, start, end, downloaded FROM segments WHERE task_id = ?1 ORDER BY idx",
    )?;
    let rows = stmt.query_map([id as i64], |row| {
        Ok(Segment {
            index: row.get::<_, i64>(0)? as u32,
            start: row.get::<_, i64>(1)? as u64,
            end: row.get::<_, Option<i64>>(2)?.map(|v| v as u64),
            downloaded: row.get::<_, i64>(3)? as u64,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

pub fn delete_task(conn: &Connection, id: TaskId) -> Result<()> {
    conn.execute("DELETE FROM tasks WHERE id = ?1", [id as i64])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn sample_task() -> TaskInfo {
        TaskInfo {
            id: 0,
            url: "https://example.com/f.bin".into(),
            final_url: None,
            file_name: "f.bin".into(),
            file_path: PathBuf::from("/tmp/f.bin"),
            part_path: PathBuf::from("/tmp/f.bin.1.part"),
            state: TaskState::Pending,
            total_size: Some(1000),
            downloaded: 0,
            connections: 4,
            range_supported: true,
            etag: Some("\"abc\"".into()),
            last_modified: None,
            expected_sha256: None,
            actual_sha256: None,
            rate_limit_bps: None,
            error: None,
            created_at: now_unix(),
            updated_at: now_unix(),
            expected_blake3: None,
            actual_blake3: None,
            auto_concurrency: false,
            speed_bps: 0,
        }
    }

    #[test]
    fn database_has_one_engine_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("exclusive.db");
        let owner = Db::open(&path).unwrap();
        assert!(Db::open(&path).is_err());
        drop(owner);
        assert!(Db::open(&path).is_ok());
    }

    #[tokio::test]
    async fn checkpoint_batch_rolls_back_on_error() {
        let db = Db::open_in_memory().unwrap();
        db.call(|c| {
            let id = insert_task(c, &sample_task(), false)?;
            for index in 0..2 {
                insert_segment(
                    c,
                    id,
                    &Segment {
                        index,
                        start: index as u64 * 500,
                        end: Some(index as u64 * 500 + 499),
                        downloaded: 0,
                    },
                )?;
            }
            c.execute_batch(
                "CREATE TRIGGER fail_second BEFORE UPDATE ON segments
                WHEN NEW.idx = 1 BEGIN SELECT RAISE(ABORT, 'injected failure'); END;",
            )?;
            assert!(checkpoint_segments(c, id, &[(0, 100), (1, 100)]).is_err());
            assert!(load_segments(c, id)?.iter().all(|s| s.downloaded == 0));
            c.execute_batch("DROP TRIGGER fail_second;")?;
            checkpoint_segments(c, id, &[(0, 100), (1, 200)])?;
            let rows = load_segments(c, id)?;
            assert_eq!(rows[0].downloaded, 100);
            assert_eq!(rows[1].downloaded, 200);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn task_roundtrip() {
        let db = Db::open_in_memory().unwrap();
        let id = db
            .call(|c| insert_task(c, &sample_task(), false))
            .await
            .unwrap();
        let loaded = db.call(move |c| load_task(c, id)).await.unwrap().unwrap();
        assert_eq!(loaded.url, "https://example.com/f.bin");
        assert_eq!(loaded.state, TaskState::Pending);
        assert_eq!(loaded.total_size, Some(1000));
        assert_eq!(loaded.etag.as_deref(), Some("\"abc\""));
    }

    #[tokio::test]
    async fn segments_roundtrip_and_checkpoint() {
        let db = Db::open_in_memory().unwrap();
        let id = db
            .call(|c| insert_task(c, &sample_task(), false))
            .await
            .unwrap();
        let segs = vec![
            Segment {
                index: 0,
                start: 0,
                end: Some(499),
                downloaded: 0,
            },
            Segment {
                index: 1,
                start: 500,
                end: Some(999),
                downloaded: 0,
            },
        ];
        db.call(move |c| replace_segments(c, id, &segs))
            .await
            .unwrap();
        db.call(move |c| checkpoint_segments(c, id, &[(1, 250)]))
            .await
            .unwrap();
        let loaded = db.call(move |c| load_segments(c, id)).await.unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[1].downloaded, 250);
        db.call(move |c| reset_segments(c, id)).await.unwrap();
        let loaded = db.call(move |c| load_segments(c, id)).await.unwrap();
        assert!(loaded.iter().all(|s| s.downloaded == 0));
    }

    #[tokio::test]
    async fn state_and_delete() {
        let db = Db::open_in_memory().unwrap();
        let id = db
            .call(|c| insert_task(c, &sample_task(), false))
            .await
            .unwrap();
        db.call(move |c| update_state(c, id, TaskState::Failed, Some("boom")))
            .await
            .unwrap();
        let t = db.call(move |c| load_task(c, id)).await.unwrap().unwrap();
        assert_eq!(t.state, TaskState::Failed);
        assert_eq!(t.error.as_deref(), Some("boom"));
        db.call(move |c| delete_task(c, id)).await.unwrap();
        assert!(db.call(move |c| load_task(c, id)).await.unwrap().is_none());
        let segs = db.call(move |c| load_segments(c, id)).await.unwrap();
        assert!(segs.is_empty());
    }

    #[tokio::test]
    async fn file_backed_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let id = {
            let db = Db::open(&path).unwrap();
            db.call(|c| insert_task(c, &sample_task(), false))
                .await
                .unwrap()
        };
        let db = Db::open(&path).unwrap();
        let t = db.call(move |c| load_task(c, id)).await.unwrap();
        assert!(t.is_some());
    }
}
