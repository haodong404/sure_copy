use async_trait::async_trait;
use globset::Glob;
use log::{debug, info, warn};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::fs as std_fs;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock as AsyncRwLock;

use crate::domain::{
    CopyError, CopyErrorCategory, CopyTask, FilePlan, TaskProgress, TaskSpec, TaskState,
};
use crate::infrastructure::{
    ChecksumProvider, FileSystem, LocalFileSystem, Sha256ChecksumProvider,
};
use crate::pipeline::StageRegistry;

use super::artifact_store::ArtifactStore;
use super::config::OrchestratorConfig;
use super::errors::{duplicate_task_error, task_not_found_error};
use super::persistent_task::PersistentTask;
use super::sqlite_artifact_store::{
    SqliteArtifactStore, TASK_DESTINATION_ARTIFACTS_SCHEMA_SQL, TASK_SOURCE_ARTIFACTS_SCHEMA_SQL,
    TASK_STAGE_STATES_SCHEMA_SQL,
};
use super::task::{Task, TaskOrchestrator};

pub(crate) const TASKS_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    source_root TEXT NOT NULL,
    destinations_json TEXT NOT NULL,
    spec_json TEXT NOT NULL DEFAULT '{}',
    file_plans_json TEXT NOT NULL DEFAULT '[]',
    state TEXT NOT NULL,
    total_bytes INTEGER NOT NULL DEFAULT 0,
    complete_bytes INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    started_at_ms INTEGER,
    finished_at_ms INTEGER
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
const TASKS_FILE_PLANS_JSON_MIGRATION_SQL: &str =
    "ALTER TABLE tasks ADD COLUMN file_plans_json TEXT NOT NULL DEFAULT '[]'";
const TASKS_RETRY_COUNT_MIGRATION_SQL: &str =
    "ALTER TABLE tasks ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0";
const TASKS_STARTED_AT_MIGRATION_SQL: &str = "ALTER TABLE tasks ADD COLUMN started_at_ms INTEGER";
const TASKS_FINISHED_AT_MIGRATION_SQL: &str = "ALTER TABLE tasks ADD COLUMN finished_at_ms INTEGER";
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

fn normalize_path_for_validation(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }

    normalized
}

fn is_same_or_nested_path(lhs: &Path, rhs: &Path) -> bool {
    lhs == rhs || lhs.starts_with(rhs) || rhs.starts_with(lhs)
}

fn validate_submitted_task(task: &CopyTask) -> Result<(), CopyError> {
    if task.id.trim().is_empty() {
        return Err(CopyError {
            category: CopyErrorCategory::Path,
            code: "TASK_ID_EMPTY",
            message: "task id must not be empty".to_string(),
        });
    }

    if task.destinations.is_empty() {
        return Err(CopyError {
            category: CopyErrorCategory::Path,
            code: "DESTINATIONS_EMPTY",
            message: format!("task '{}' must include at least one destination", task.id),
        });
    }

    let normalized_source = normalize_path_for_validation(&task.source_root);
    if normalized_source.as_os_str().is_empty() {
        return Err(CopyError {
            category: CopyErrorCategory::Path,
            code: "SOURCE_ROOT_EMPTY",
            message: format!("task '{}' must include a non-empty source root", task.id),
        });
    }

    let mut unique_destinations = std::collections::HashSet::new();
    for destination in &task.destinations {
        let normalized_destination = normalize_path_for_validation(destination);
        if normalized_destination.as_os_str().is_empty() {
            return Err(CopyError {
                category: CopyErrorCategory::Path,
                code: "DESTINATION_EMPTY",
                message: format!("task '{}' contains an empty destination path", task.id),
            });
        }

        let key = normalized_destination.to_string_lossy().to_string();
        if !unique_destinations.insert(key.clone()) {
            return Err(CopyError {
                category: CopyErrorCategory::Path,
                code: "DUPLICATE_DESTINATION",
                message: format!(
                    "task '{}' contains duplicate destination '{}'",
                    task.id, key
                ),
            });
        }

        if is_same_or_nested_path(&normalized_source, &normalized_destination) {
            return Err(CopyError {
                category: CopyErrorCategory::Path,
                code: "OVERLAPPING_SOURCE_DESTINATION",
                message: format!(
                    "task '{}' source '{}' overlaps destination '{}'",
                    task.id,
                    normalized_source.display(),
                    normalized_destination.display()
                ),
            });
        }
    }

    for pattern in task
        .options
        .include_patterns
        .iter()
        .chain(task.options.exclude_patterns.iter())
    {
        Glob::new(pattern).map_err(|err| CopyError {
            category: CopyErrorCategory::Path,
            code: "INVALID_GLOB_PATTERN",
            message: format!("invalid glob pattern '{}': {}", pattern, err),
        })?;
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
        TaskState::Preparing => "preparing",
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
        "preparing" => Some(TaskState::Preparing),
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

fn encode_file_plans(file_plans: &[FilePlan]) -> Result<String, CopyError> {
    serde_json::to_string(file_plans).map_err(|err| CopyError {
        category: CopyErrorCategory::Io,
        code: "SERDE_ERROR",
        message: format!("failed to encode file plans: {}", err),
    })
}

fn decode_file_plans(file_plans_json: &str) -> Result<Vec<FilePlan>, CopyError> {
    if file_plans_json.trim().is_empty() {
        return Ok(Vec::new());
    }

    serde_json::from_str::<Vec<FilePlan>>(file_plans_json).map_err(|err| CopyError {
        category: CopyErrorCategory::Io,
        code: "SERDE_ERROR",
        message: format!("failed to decode file plans: {}", err),
    })
}

struct DbTaskRow {
    id: String,
    source_root: String,
    destinations_json: String,
    spec_json: String,
    file_plans_json: String,
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
    let file_plans = decode_file_plans(&row.file_plans_json)?;
    let task = CopyTask::from_spec(row.id, spec)
        .with_state(state)
        .with_file_plans(file_plans);

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
    stage_registry: Option<Arc<dyn StageRegistry>>,
}

impl SqliteTaskOrchestrator {
    /// Creates a SQLite orchestrator using a database file path.
    pub async fn new(
        database_path: impl AsRef<Path>,
        config: OrchestratorConfig,
    ) -> Result<Self, CopyError> {
        Self::new_with_stage_registry(database_path, config, None).await
    }

    /// Creates a SQLite orchestrator that can rebuild runtime stages from
    /// durably persisted stage specs.
    pub async fn new_with_stage_registry(
        database_path: impl AsRef<Path>,
        config: OrchestratorConfig,
        stage_registry: Option<Arc<dyn StageRegistry>>,
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
        apply_best_effort_migration(
            &pool,
            TASKS_FILE_PLANS_JSON_MIGRATION_SQL,
            "SqliteTaskOrchestrator::migrate_tasks_file_plans_json",
        )
        .await?;
        apply_best_effort_migration(
            &pool,
            TASKS_RETRY_COUNT_MIGRATION_SQL,
            "SqliteTaskOrchestrator::migrate_tasks_retry_count",
        )
        .await?;
        apply_best_effort_migration(
            &pool,
            TASKS_STARTED_AT_MIGRATION_SQL,
            "SqliteTaskOrchestrator::migrate_tasks_started_at_ms",
        )
        .await?;
        apply_best_effort_migration(
            &pool,
            TASKS_FINISHED_AT_MIGRATION_SQL,
            "SqliteTaskOrchestrator::migrate_tasks_finished_at_ms",
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
        sqlx::query(TASK_STAGE_STATES_SCHEMA_SQL)
            .execute(&pool)
            .await
            .map_err(|err| db_error("SqliteTaskOrchestrator::new_stage_state_schema", err))?;

        let artifact_store: Arc<dyn ArtifactStore> =
            Arc::new(SqliteArtifactStore::new(pool.clone()));

        let orchestrator = Self {
            config,
            pool,
            tasks: AsyncRwLock::new(HashMap::new()),
            file_system: Arc::new(LocalFileSystem::new()),
            checksum_provider: Arc::new(Sha256ChecksumProvider::new()),
            artifact_store,
            stage_registry,
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
        let result = sqlx::query("UPDATE tasks SET state = ? WHERE state IN (?, ?, ?)")
            .bind(state_to_db_value(TaskState::Paused))
            .bind(state_to_db_value(TaskState::Planned))
            .bind(state_to_db_value(TaskState::Preparing))
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
            "SELECT id, source_root, destinations_json, spec_json, file_plans_json, state, total_bytes, complete_bytes FROM tasks WHERE id = ?",
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
            file_plans_json: row.get::<String, _>("file_plans_json"),
            state: row.get::<String, _>("state"),
            total_bytes: row.get::<i64, _>("total_bytes"),
            complete_bytes: row.get::<i64, _>("complete_bytes"),
        })
    }

    async fn load_all_rows(&self) -> Result<Vec<DbTaskRow>, CopyError> {
        let rows = sqlx::query(
            "SELECT id, source_root, destinations_json, spec_json, file_plans_json, state, total_bytes, complete_bytes FROM tasks",
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
                file_plans_json: row.get::<String, _>("file_plans_json"),
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
            self.stage_registry.clone(),
        ));

        Ok(handle)
    }

    fn prepare_submitted_task(&self, task: CopyTask) -> Result<CopyTask, CopyError> {
        if task.flow_spec.is_some() || !task.flow.has_runtime_stages() {
            return Ok(task);
        }

        let flow_spec = task.flow.try_to_spec()?;
        Ok(task.with_flow_spec(flow_spec.unwrap_or_default()))
    }
}

#[async_trait]
impl TaskOrchestrator for SqliteTaskOrchestrator {
    async fn submit(&self, task: CopyTask) -> Result<Arc<dyn Task>, CopyError> {
        let task = self.prepare_submitted_task(task)?;
        validate_submitted_task(&task)?;
        info!("submitting SQLite-backed task '{}'", task.id);
        let destinations_json = encode_destinations(&task.destinations)?;
        let spec_json = encode_task_spec(&task.spec())?;
        let file_plans_json = encode_file_plans(&task.file_plans)?;
        sqlx::query(
            "INSERT INTO tasks (id, source_root, destinations_json, spec_json, file_plans_json, state, total_bytes, complete_bytes, retry_count, started_at_ms, finished_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&task.id)
        .bind(task.source_root.to_string_lossy().to_string())
        .bind(destinations_json)
        .bind(spec_json)
        .bind(file_plans_json)
        .bind(state_to_db_value(task.state))
        .bind(0_i64)
        .bind(0_i64)
        .bind(0_i64)
        .bind(Option::<i64>::None)
        .bind(Option::<i64>::None)
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
            self.stage_registry.clone(),
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
    fn validate_submitted_task_rejects_empty_task_id() {
        let err = validate_submitted_task(&CopyTask::new(
            "   ",
            PathBuf::from("/src"),
            vec![PathBuf::from("/dst")],
            Default::default(),
        ))
        .expect_err("empty task id should be rejected");

        assert_eq!(err.code, "TASK_ID_EMPTY");
    }

    #[test]
    fn validate_submitted_task_rejects_empty_destinations() {
        let err = validate_submitted_task(&CopyTask::new(
            "task-empty-destinations",
            PathBuf::from("/src"),
            Vec::new(),
            Default::default(),
        ))
        .expect_err("missing destinations should be rejected");

        assert_eq!(err.code, "DESTINATIONS_EMPTY");
    }

    #[test]
    fn validate_submitted_task_rejects_duplicate_destinations() {
        let err = validate_submitted_task(&CopyTask::new(
            "task-duplicate-destinations",
            PathBuf::from("/src"),
            vec![PathBuf::from("/dst"), PathBuf::from("/dst")],
            Default::default(),
        ))
        .expect_err("duplicate destinations should be rejected");

        assert_eq!(err.code, "DUPLICATE_DESTINATION");
    }

    #[test]
    fn validate_submitted_task_rejects_empty_source_root() {
        let err = validate_submitted_task(&CopyTask::new(
            "task-empty-source",
            PathBuf::new(),
            vec![PathBuf::from("/dst")],
            Default::default(),
        ))
        .expect_err("empty source root should be rejected");

        assert_eq!(err.code, "SOURCE_ROOT_EMPTY");
    }

    #[test]
    fn state_mapping_roundtrip_is_stable() {
        let all = [
            TaskState::Created,
            TaskState::Planned,
            TaskState::Preparing,
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
            file_plans_json: "[]".to_string(),
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
