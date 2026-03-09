use async_trait::async_trait;
use log::{debug, info, warn};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::fs as std_fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock as AsyncRwLock;

use crate::domain::{CopyError, CopyErrorCategory, CopyTask, TaskProgress, TaskSpec, TaskState};
use crate::infrastructure::{
    ChecksumProvider, FileSystem, LocalFileSystem, Sha256ChecksumProvider,
};

use super::artifact_store::ArtifactStore;
use super::config::OrchestratorConfig;
use super::errors::{duplicate_task_error, task_not_found_error, unsupported_feature_error};
use super::persistent_task::PersistentTask;
use super::sqlite_artifact_store::{
    SqliteArtifactStore, TASK_DESTINATION_ARTIFACTS_SCHEMA_SQL, TASK_SOURCE_ARTIFACTS_SCHEMA_SQL,
};
use super::task::{Task, TaskOrchestrator};

pub(crate) const TASKS_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    source_root TEXT NOT NULL,
    destinations_json TEXT NOT NULL,
    spec_json TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL,
    total_bytes INTEGER NOT NULL DEFAULT 0,
    complete_bytes INTEGER NOT NULL DEFAULT 0
);
"#;

pub(crate) const TASK_FILE_CHECKPOINTS_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS task_file_checkpoints (
    task_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    destination_path TEXT NOT NULL,
    status TEXT NOT NULL,
    copy_status TEXT NOT NULL DEFAULT 'pending',
    post_write_status TEXT NOT NULL DEFAULT 'pending',
    bytes_copied INTEGER NOT NULL DEFAULT 0,
    expected_bytes INTEGER NOT NULL DEFAULT 0,
    actual_destination_path TEXT,
    last_error TEXT,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (task_id, source_path, destination_path)
);
"#;

const TASKS_SPEC_JSON_MIGRATION_SQL: &str =
    "ALTER TABLE tasks ADD COLUMN spec_json TEXT NOT NULL DEFAULT '{}'";
const TASK_FILE_CHECKPOINTS_EXPECTED_BYTES_MIGRATION_SQL: &str =
    "ALTER TABLE task_file_checkpoints ADD COLUMN expected_bytes INTEGER NOT NULL DEFAULT 0";
const TASK_FILE_CHECKPOINTS_ACTUAL_DESTINATION_MIGRATION_SQL: &str =
    "ALTER TABLE task_file_checkpoints ADD COLUMN actual_destination_path TEXT";
const TASK_FILE_CHECKPOINTS_COPY_STATUS_MIGRATION_SQL: &str =
    "ALTER TABLE task_file_checkpoints ADD COLUMN copy_status TEXT NOT NULL DEFAULT 'pending'";
const TASK_FILE_CHECKPOINTS_POST_WRITE_STATUS_MIGRATION_SQL: &str =
    "ALTER TABLE task_file_checkpoints ADD COLUMN post_write_status TEXT NOT NULL DEFAULT 'pending'";

fn db_error(context: &'static str, err: sqlx::Error) -> CopyError {
    CopyError {
        category: CopyErrorCategory::Io,
        code: "DB_ERROR",
        message: format!("database error in {}: {}", context, err),
    }
}

fn is_duplicate_column_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.message().contains("duplicate column name"),
        _ => false,
    }
}

async fn apply_best_effort_migration(
    pool: &SqlitePool,
    sql: &'static str,
    context: &'static str,
) -> Result<(), CopyError> {
    match sqlx::query(sql).execute(pool).await {
        Ok(_) => {
            debug!("applied SQLite migration '{}'", context);
            Ok(())
        }
        Err(err) if is_duplicate_column_error(&err) => {
            debug!(
                "skipped SQLite migration '{}' because column already exists",
                context
            );
            Ok(())
        }
        Err(err) => Err(db_error(context, err)),
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn map_submit_error(task_id: &str, err: sqlx::Error) -> CopyError {
    if let sqlx::Error::Database(db_err) = &err {
        if db_err.is_unique_violation() {
            return duplicate_task_error(task_id);
        }
    }

    db_error("SqliteTaskOrchestrator::submit", err)
}

fn validate_submitted_task(task: &CopyTask) -> Result<(), CopyError> {
    if task.flow.has_runtime_stages() {
        return Err(unsupported_feature_error(
            "durable custom runtime stages for SQLite-backed tasks",
        ));
    }

    Ok(())
}

fn encode_task_spec(spec: &TaskSpec) -> Result<String, CopyError> {
    serde_json::to_string(spec).map_err(|err| CopyError {
        category: CopyErrorCategory::Io,
        code: "SERDE_ERROR",
        message: format!("failed to encode task spec: {}", err),
    })
}

fn ensure_parent_dir_exists(database_path: &Path) -> Result<(), CopyError> {
    if let Some(parent) = database_path.parent() {
        if !parent.as_os_str().is_empty() {
            std_fs::create_dir_all(parent).map_err(|err| CopyError {
                category: CopyErrorCategory::Path,
                code: "DB_DIR_CREATE_ERROR",
                message: format!(
                    "failed to create database directory '{}': {}",
                    parent.display(),
                    err
                ),
            })?;
        }
    }

    Ok(())
}

fn state_to_db_value(state: TaskState) -> &'static str {
    match state {
        TaskState::Created => "created",
        TaskState::Planned => "planned",
        TaskState::Running => "running",
        TaskState::Completed => "completed",
        TaskState::PartialFailed => "partial_failed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
        TaskState::Paused => "paused",
    }
}

fn state_from_db_value(value: &str) -> Option<TaskState> {
    match value {
        "created" => Some(TaskState::Created),
        "planned" => Some(TaskState::Planned),
        "running" => Some(TaskState::Running),
        "completed" => Some(TaskState::Completed),
        "partial_failed" => Some(TaskState::PartialFailed),
        "failed" => Some(TaskState::Failed),
        "cancelled" => Some(TaskState::Cancelled),
        "paused" => Some(TaskState::Paused),
        _ => None,
    }
}

fn encode_destinations(destinations: &[PathBuf]) -> Result<String, CopyError> {
    let values = destinations
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect::<Vec<_>>();

    serde_json::to_string(&values).map_err(|err| CopyError {
        category: CopyErrorCategory::Io,
        code: "SERDE_ERROR",
        message: format!("failed to encode destinations: {}", err),
    })
}

fn decode_destinations(destinations_json: &str) -> Result<Vec<PathBuf>, CopyError> {
    let values =
        serde_json::from_str::<Vec<String>>(destinations_json).map_err(|err| CopyError {
            category: CopyErrorCategory::Io,
            code: "SERDE_ERROR",
            message: format!("failed to decode destinations: {}", err),
        })?;

    Ok(values.into_iter().map(PathBuf::from).collect())
}

struct DbTaskRow {
    id: String,
    source_root: String,
    destinations_json: String,
    spec_json: String,
    state: String,
    total_bytes: i64,
    complete_bytes: i64,
}

fn decode_task_spec(row: &DbTaskRow) -> Result<TaskSpec, CopyError> {
    if !row.spec_json.trim().is_empty() && row.spec_json.trim() != "{}" {
        return serde_json::from_str::<TaskSpec>(&row.spec_json).map_err(|err| CopyError {
            category: CopyErrorCategory::Io,
            code: "SERDE_ERROR",
            message: format!("failed to decode task spec: {}", err),
        });
    }

    Ok(TaskSpec::new(
        PathBuf::from(&row.source_root),
        decode_destinations(&row.destinations_json)?,
        Default::default(),
    ))
}

fn task_from_row(row: DbTaskRow) -> Result<(CopyTask, TaskProgress), CopyError> {
    let state = state_from_db_value(&row.state).ok_or_else(|| CopyError {
        category: CopyErrorCategory::Unknown,
        code: "INVALID_STATE_VALUE",
        message: format!("unknown task state '{}' in storage", row.state),
    })?;

    let spec = decode_task_spec(&row)?;
    let task = CopyTask::from_spec(row.id, spec).with_state(state);

    let progress = TaskProgress {
        total_bytes: row.total_bytes.max(0) as u64,
        complete_bytes: row.complete_bytes.max(0) as u64,
        ..TaskProgress::default()
    };

    Ok((task, progress))
}

/// SQLite-backed orchestrator implementation for local durable task storage.
pub struct SqliteTaskOrchestrator {
    config: OrchestratorConfig,
    pool: SqlitePool,
    tasks: AsyncRwLock<HashMap<String, Arc<PersistentTask>>>,
    file_system: Arc<dyn FileSystem>,
    checksum_provider: Arc<dyn ChecksumProvider>,
    artifact_store: Arc<dyn ArtifactStore>,
}

impl SqliteTaskOrchestrator {
    /// Creates a SQLite orchestrator using a database file path.
    pub async fn new(
        database_path: impl AsRef<Path>,
        config: OrchestratorConfig,
    ) -> Result<Self, CopyError> {
        let database_path = database_path.as_ref();
        info!(
            "initializing SQLite orchestrator at '{}'",
            database_path.display()
        );
        ensure_parent_dir_exists(database_path)?;

        let connect_options = SqliteConnectOptions::new()
            .filename(database_path)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_parallel_tasks.max(1) as u32)
            .connect_with(connect_options)
            .await
            .map_err(|err| db_error("SqliteTaskOrchestrator::new", err))?;

        sqlx::query(TASKS_SCHEMA_SQL)
            .execute(&pool)
            .await
            .map_err(|err| db_error("SqliteTaskOrchestrator::new_schema", err))?;
        apply_best_effort_migration(
            &pool,
            TASKS_SPEC_JSON_MIGRATION_SQL,
            "SqliteTaskOrchestrator::migrate_tasks_spec_json",
        )
        .await?;

        sqlx::query(TASK_FILE_CHECKPOINTS_SCHEMA_SQL)
            .execute(&pool)
            .await
            .map_err(|err| db_error("SqliteTaskOrchestrator::new_checkpoint_schema", err))?;
        apply_best_effort_migration(
            &pool,
            TASK_FILE_CHECKPOINTS_EXPECTED_BYTES_MIGRATION_SQL,
            "SqliteTaskOrchestrator::migrate_checkpoint_expected_bytes",
        )
        .await?;
        apply_best_effort_migration(
            &pool,
            TASK_FILE_CHECKPOINTS_ACTUAL_DESTINATION_MIGRATION_SQL,
            "SqliteTaskOrchestrator::migrate_checkpoint_actual_destination",
        )
        .await?;
        apply_best_effort_migration(
            &pool,
            TASK_FILE_CHECKPOINTS_COPY_STATUS_MIGRATION_SQL,
            "SqliteTaskOrchestrator::migrate_checkpoint_copy_status",
        )
        .await?;
        apply_best_effort_migration(
            &pool,
            TASK_FILE_CHECKPOINTS_POST_WRITE_STATUS_MIGRATION_SQL,
            "SqliteTaskOrchestrator::migrate_checkpoint_post_write_status",
        )
        .await?;
        sqlx::query(TASK_SOURCE_ARTIFACTS_SCHEMA_SQL)
            .execute(&pool)
            .await
            .map_err(|err| db_error("SqliteTaskOrchestrator::new_source_artifact_schema", err))?;
        sqlx::query(TASK_DESTINATION_ARTIFACTS_SCHEMA_SQL)
            .execute(&pool)
            .await
            .map_err(|err| {
                db_error(
                    "SqliteTaskOrchestrator::new_destination_artifact_schema",
                    err,
                )
            })?;

        let artifact_store: Arc<dyn ArtifactStore> =
            Arc::new(SqliteArtifactStore::new(pool.clone()));

        let orchestrator = Self {
            config,
            pool,
            tasks: AsyncRwLock::new(HashMap::new()),
            file_system: Arc::new(LocalFileSystem::new()),
            checksum_provider: Arc::new(Sha256ChecksumProvider::new()),
            artifact_store,
        };

        let recovered = orchestrator.recover_unfinished_tasks().await?;
        debug!(
            "SQLite orchestrator initialized with {} recovered unfinished tasks",
            recovered
        );

        Ok(orchestrator)
    }

    /// Recovers unfinished tasks after an unclean shutdown.
    ///
    /// Tasks in `Planned` or `Running` are moved to `Paused`, so callers can
    /// decide whether and when to resume.
    pub async fn recover_unfinished_tasks(&self) -> Result<u64, CopyError> {
        let result = sqlx::query("UPDATE tasks SET state = ? WHERE state IN (?, ?)")
            .bind(state_to_db_value(TaskState::Paused))
            .bind(state_to_db_value(TaskState::Planned))
            .bind(state_to_db_value(TaskState::Running))
            .execute(&self.pool)
            .await
            .map_err(|err| db_error("SqliteTaskOrchestrator::recover_unfinished_tasks", err))?;

        let checkpoint_result = sqlx::query(
            "UPDATE task_file_checkpoints SET status = ?, copy_status = ?, post_write_status = ?, updated_at_ms = ? WHERE status IN (?, ?)",
        )
        .bind("pending")
        .bind("pending")
        .bind("pending")
        .bind(now_unix_ms())
        .bind("writing")
        .bind("post_write_running")
        .execute(&self.pool)
        .await
        .map_err(|err| {
            db_error(
                "SqliteTaskOrchestrator::recover_unfinished_checkpoints",
                err,
            )
        })?;

        let recovered_tasks = result.rows_affected();
        let recovered_checkpoints = checkpoint_result.rows_affected();

        if recovered_tasks > 0 || recovered_checkpoints > 0 {
            warn!(
                "recovered {} unfinished tasks and {} running checkpoints from SQLite storage",
                recovered_tasks, recovered_checkpoints
            );
        } else {
            debug!("no unfinished SQLite tasks required recovery");
        }

        Ok(recovered_tasks)
    }

    async fn load_row_by_id(&self, task_id: &str) -> Result<DbTaskRow, CopyError> {
        let row = sqlx::query(
            "SELECT id, source_root, destinations_json, spec_json, state, total_bytes, complete_bytes FROM tasks WHERE id = ?",
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| db_error("SqliteTaskOrchestrator::load_row_by_id", err))?
        .ok_or_else(|| task_not_found_error(task_id))?;

        Ok(DbTaskRow {
            id: row.get::<String, _>("id"),
            source_root: row.get::<String, _>("source_root"),
            destinations_json: row.get::<String, _>("destinations_json"),
            spec_json: row.get::<String, _>("spec_json"),
            state: row.get::<String, _>("state"),
            total_bytes: row.get::<i64, _>("total_bytes"),
            complete_bytes: row.get::<i64, _>("complete_bytes"),
        })
    }

    async fn load_all_rows(&self) -> Result<Vec<DbTaskRow>, CopyError> {
        let rows = sqlx::query(
            "SELECT id, source_root, destinations_json, spec_json, state, total_bytes, complete_bytes FROM tasks",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| db_error("SqliteTaskOrchestrator::load_all_rows", err))?;

        Ok(rows
            .into_iter()
            .map(|row| DbTaskRow {
                id: row.get::<String, _>("id"),
                source_root: row.get::<String, _>("source_root"),
                destinations_json: row.get::<String, _>("destinations_json"),
                spec_json: row.get::<String, _>("spec_json"),
                state: row.get::<String, _>("state"),
                total_bytes: row.get::<i64, _>("total_bytes"),
                complete_bytes: row.get::<i64, _>("complete_bytes"),
            })
            .collect())
    }

    async fn create_handle_from_row(
        &self,
        row: DbTaskRow,
    ) -> Result<Arc<PersistentTask>, CopyError> {
        debug!("creating persistent task handle for '{}'", row.id);
        let (task, progress) = task_from_row(row)?;
        let handle = Arc::new(PersistentTask::new(
            task,
            progress,
            self.config.event_channel_capacity,
            self.pool.clone(),
            Arc::clone(&self.file_system),
            Arc::clone(&self.checksum_provider),
            Arc::clone(&self.artifact_store),
        ));

        Ok(handle)
    }
}

#[async_trait]
impl TaskOrchestrator for SqliteTaskOrchestrator {
    async fn submit(&self, task: CopyTask) -> Result<Arc<dyn Task>, CopyError> {
        validate_submitted_task(&task)?;
        info!("submitting SQLite-backed task '{}'", task.id);
        let destinations_json = encode_destinations(&task.destinations)?;
        let spec_json = encode_task_spec(&task.spec())?;
        sqlx::query(
            "INSERT INTO tasks (id, source_root, destinations_json, spec_json, state, total_bytes, complete_bytes) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&task.id)
        .bind(task.source_root.to_string_lossy().to_string())
        .bind(destinations_json)
        .bind(spec_json)
        .bind(state_to_db_value(task.state))
        .bind(0_i64)
        .bind(0_i64)
        .execute(&self.pool)
        .await
        .map_err(|err| map_submit_error(&task.id, err))?;

        let handle = Arc::new(PersistentTask::new(
            task,
            TaskProgress::default(),
            self.config.event_channel_capacity,
            self.pool.clone(),
            Arc::clone(&self.file_system),
            Arc::clone(&self.checksum_provider),
            Arc::clone(&self.artifact_store),
        ));

        let mut tasks = self.tasks.write().await;
        tasks.insert(handle.id().to_string(), Arc::clone(&handle));

        let as_dyn: Arc<dyn Task> = handle;
        Ok(as_dyn)
    }

    async fn get_task(&self, task_id: &str) -> Result<Arc<dyn Task>, CopyError> {
        {
            let tasks = self.tasks.read().await;
            if let Some(handle) = tasks.get(task_id) {
                debug!("loaded SQLite task '{}' from in-memory cache", task_id);
                let cloned: Arc<PersistentTask> = Arc::clone(handle);
                let as_dyn: Arc<dyn Task> = cloned;
                return Ok(as_dyn);
            }
        }

        debug!("loading SQLite task '{}' from durable storage", task_id);
        let row = self.load_row_by_id(task_id).await?;
        let handle = self.create_handle_from_row(row).await?;

        let mut tasks = self.tasks.write().await;
        let entry = tasks
            .entry(task_id.to_string())
            .or_insert_with(|| Arc::clone(&handle));

        let cloned: Arc<PersistentTask> = Arc::clone(entry);
        let as_dyn: Arc<dyn Task> = cloned;
        Ok(as_dyn)
    }

    async fn get_tasks(&self) -> Result<Vec<Arc<dyn Task>>, CopyError> {
        let rows = self.load_all_rows().await?;
        debug!("loading {} SQLite task rows", rows.len());
        let mut tasks_guard = self.tasks.write().await;
        let mut handles = Vec::with_capacity(rows.len());

        for row in rows {
            if let Some(existing) = tasks_guard.get(&row.id) {
                let cloned: Arc<PersistentTask> = Arc::clone(existing);
                let as_dyn: Arc<dyn Task> = cloned;
                handles.push(as_dyn);
                continue;
            }

            let handle = self.create_handle_from_row(row).await?;
            let task_id = handle.id().to_string();
            tasks_guard.insert(task_id, Arc::clone(&handle));
            let as_dyn: Arc<dyn Task> = handle;
            handles.push(as_dyn);
        }

        Ok(handles)
    }

    async fn pause_all(&self) -> Result<(), CopyError> {
        info!("pausing all running SQLite tasks");
        let handles = self.get_tasks().await?;

        for handle in handles {
            if handle.state().await? == TaskState::Running {
                handle.pause().await?;
            }
        }

        Ok(())
    }

    async fn resume_all(&self) -> Result<(), CopyError> {
        info!("resuming all paused SQLite tasks");
        let handles = self.get_tasks().await?;

        for handle in handles {
            if handle.state().await? == TaskState::Paused {
                handle.resume().await?;
            }
        }

        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), CopyError> {
        info!("cancelling all active SQLite tasks");
        let handles = self.get_tasks().await?;

        for handle in handles {
            let state = handle.state().await?;
            if !state.is_terminal() {
                handle.cancel().await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_mapping_roundtrip_is_stable() {
        let all = [
            TaskState::Created,
            TaskState::Planned,
            TaskState::Running,
            TaskState::Completed,
            TaskState::PartialFailed,
            TaskState::Failed,
            TaskState::Cancelled,
            TaskState::Paused,
        ];

        for state in all {
            let db_value = state_to_db_value(state);
            assert_eq!(state_from_db_value(db_value), Some(state));
        }
    }

    #[test]
    fn decode_destinations_rejects_invalid_json() {
        let err = decode_destinations("{not-json}").expect_err("must fail on invalid json");
        assert_eq!(err.code, "SERDE_ERROR");
    }

    #[test]
    fn task_from_row_clamps_negative_progress_values() {
        let row = DbTaskRow {
            id: "task-neg".to_string(),
            source_root: "/src".to_string(),
            destinations_json: "[\"/dst\"]".to_string(),
            spec_json: "{}".to_string(),
            state: "created".to_string(),
            total_bytes: -10,
            complete_bytes: -3,
        };

        let (_task, progress) = task_from_row(row).expect("row should decode");
        assert_eq!(progress.total_bytes, 0);
        assert_eq!(progress.complete_bytes, 0);
    }

    #[test]
    fn ensure_parent_dir_exists_creates_nested_parent() {
        let base = std::env::temp_dir().join(format!(
            "sure_copy_core_sqlite_parent_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after unix epoch")
                .as_nanos()
        ));
        let db_path = base.join("nested").join("tasks.db");

        ensure_parent_dir_exists(&db_path).expect("must create missing parent dir");
        assert!(db_path.parent().expect("has parent").exists());

        let _ = std::fs::remove_dir_all(base);
    }
}
