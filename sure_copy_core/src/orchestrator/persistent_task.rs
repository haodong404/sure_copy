use async_trait::async_trait;
use log::{debug, error, info, warn};
use sqlx::Row;
use sqlx::SqlitePool;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::sync::{broadcast, Mutex as AsyncMutex};

use crate::domain::{
    CopyError, CopyErrorCategory, CopyReport, CopyTask, FilePlan, OverwritePolicy, TaskProgress,
    TaskState, VerificationPolicy,
};
use crate::infrastructure::{ChecksumProvider, FileSystem};

use super::errors::{invalid_state_error, lock_poisoned_error};
use super::task::{Task, TaskStream, TaskUpdate};

#[derive(Clone)]
struct TaskRuntime {
    snapshot: CopyTask,
    progress: TaskProgress,
    report: Option<CopyReport>,
}

#[derive(Debug, Clone)]
struct DbFileCheckpointRow {
    source_path: String,
    destination_path: String,
    status: String,
    bytes_copied: i64,
    expected_bytes: i64,
    actual_destination_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileCheckpointStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunOutcome {
    Completed,
    Paused,
    Cancelled,
}

fn checkpoint_status_to_db_value(status: FileCheckpointStatus) -> &'static str {
    match status {
        FileCheckpointStatus::Pending => "pending",
        FileCheckpointStatus::Running => "running",
        FileCheckpointStatus::Completed => "completed",
        FileCheckpointStatus::Failed => "failed",
    }
}

fn checkpoint_status_from_db_value(value: &str) -> Option<FileCheckpointStatus> {
    match value {
        "pending" => Some(FileCheckpointStatus::Pending),
        "running" => Some(FileCheckpointStatus::Running),
        "completed" => Some(FileCheckpointStatus::Completed),
        "failed" => Some(FileCheckpointStatus::Failed),
        _ => None,
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn sqlx_error(context: &'static str, err: sqlx::Error) -> CopyError {
    CopyError {
        category: CopyErrorCategory::Io,
        code: "DB_ERROR",
        message: format!("database error in {}: {}", context, err),
    }
}

fn io_error(code: &'static str, message: String) -> CopyError {
    CopyError {
        category: CopyErrorCategory::Io,
        code,
        message,
    }
}

fn checksum_mismatch_error(source: &Path, destination: &Path) -> CopyError {
    CopyError {
        category: CopyErrorCategory::Checksum,
        code: "CHECKSUM_MISMATCH",
        message: format!(
            "copy verification failed because '{}' and '{}' do not match",
            source.display(),
            destination.display()
        ),
    }
}

/// SQLite-backed concrete task handle with per-file checkpoint execution.
pub struct PersistentTask {
    id: String,
    pool: SqlitePool,
    runtime: RwLock<TaskRuntime>,
    event_tx: broadcast::Sender<TaskUpdate>,
    file_system: Arc<dyn FileSystem>,
    checksum_provider: Arc<dyn ChecksumProvider>,
    execution_lock: AsyncMutex<()>,
}

impl PersistentTask {
    pub fn new(
        task: CopyTask,
        progress: TaskProgress,
        event_channel_capacity: usize,
        pool: SqlitePool,
        file_system: Arc<dyn FileSystem>,
        checksum_provider: Arc<dyn ChecksumProvider>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(event_channel_capacity.max(1));
        let id = task.id.clone();
        debug!(
            "creating persistent task '{}' with state {:?} and event channel capacity {}",
            id,
            task.state,
            event_channel_capacity.max(1)
        );

        Self {
            id,
            pool,
            runtime: RwLock::new(TaskRuntime {
                snapshot: task,
                progress,
                report: None,
            }),
            event_tx,
            file_system,
            checksum_provider,
            execution_lock: AsyncMutex::new(()),
        }
    }

    async fn persist_state(&self, state: TaskState) -> Result<(), CopyError> {
        debug!("persisting task '{}' state {:?}", self.id, state);
        sqlx::query("UPDATE tasks SET state = ? WHERE id = ?")
            .bind(state_to_db_value(state))
            .bind(&self.id)
            .execute(&self.pool)
            .await
            .map_err(|err| sqlx_error("PersistentTask::persist_state", err))?;

        Ok(())
    }

    async fn transition_state(&self, next: TaskState) -> Result<(), CopyError> {
        let current = self.state().await?;
        if !current.can_transition_to(next) {
            return Err(invalid_state_error(&self.id, current, next));
        }

        info!(
            "persistent task '{}' state transition {:?} -> {:?}",
            self.id, current, next
        );
        self.persist_state(next).await?;

        let mut runtime = self
            .runtime
            .write()
            .map_err(|_| lock_poisoned_error("PersistentTask::transition_state"))?;
        runtime.snapshot.state = next;
        runtime.report = None;

        let _ = self.event_tx.send(TaskUpdate::State(next));
        Ok(())
    }

    fn build_report(runtime: &TaskRuntime) -> CopyReport {
        CopyReport {
            snapshot: runtime.snapshot.clone(),
            total_files: runtime.snapshot.file_plans.len() as u64,
            succeeded_files: 0,
            failed_files: 0,
            total_bytes: runtime.progress.total_bytes,
            complete_bytes: runtime.progress.complete_bytes,
            duration_ms: 0,
            retry_count: 0,
            failures: Vec::new(),
        }
    }

    fn options(&self) -> Result<crate::domain::TaskOptions, CopyError> {
        let runtime = self
            .runtime
            .read()
            .map_err(|_| lock_poisoned_error("PersistentTask::options"))?;
        Ok(runtime.snapshot.options.clone())
    }

    fn snapshot_task(&self) -> Result<CopyTask, CopyError> {
        let runtime = self
            .runtime
            .read()
            .map_err(|_| lock_poisoned_error("PersistentTask::snapshot_task"))?;
        Ok(runtime.snapshot.clone())
    }

    fn set_file_plans(&self, file_plans: Vec<FilePlan>) -> Result<(), CopyError> {
        let mut runtime = self
            .runtime
            .write()
            .map_err(|_| lock_poisoned_error("PersistentTask::set_file_plans"))?;
        runtime.snapshot.file_plans = file_plans;
        runtime.report = None;
        Ok(())
    }

    async fn ensure_file_plans(&self) -> Result<Vec<FilePlan>, CopyError> {
        let snapshot = self.snapshot_task()?;
        if !snapshot.file_plans.is_empty() {
            debug!(
                "task '{}' reused {} cached file plans",
                self.id,
                snapshot.file_plans.len()
            );
            return Ok(snapshot.file_plans);
        }

        debug!(
            "task '{}' scanning source tree '{}'",
            self.id,
            snapshot.source_root.display()
        );
        let source_files = self
            .file_system
            .read_dir_recursive(&snapshot.source_root)
            .await?;
        let mut plans = Vec::with_capacity(source_files.len());

        for source_file in source_files {
            let relative = source_file
                .strip_prefix(&snapshot.source_root)
                .map_err(|err| CopyError {
                    category: CopyErrorCategory::Path,
                    code: "RELATIVE_PATH_ERROR",
                    message: format!(
                        "failed to compute relative path '{}' under '{}': {err}",
                        source_file.display(),
                        snapshot.source_root.display()
                    ),
                })?;

            let destinations = snapshot
                .destinations
                .iter()
                .map(|root| root.join(relative))
                .collect::<Vec<_>>();

            let metadata = fs::metadata(&source_file).await.map_err(|err| CopyError {
                category: CopyErrorCategory::Io,
                code: "SOURCE_METADATA_ERROR",
                message: format!(
                    "failed to read source file metadata '{}': {err}",
                    source_file.display()
                ),
            })?;

            plans.push(FilePlan {
                source: source_file,
                destinations,
                expected_size_bytes: Some(metadata.len()),
                expected_checksum: None,
            });
        }

        self.set_file_plans(plans.clone())?;
        debug!("task '{}' planned {} source files", self.id, plans.len());
        Ok(plans)
    }

    async fn ensure_checkpoints_seeded(&self, file_plans: &[FilePlan]) -> Result<(), CopyError> {
        let checkpoint_count = file_plans
            .iter()
            .map(|plan| plan.destinations.len())
            .sum::<usize>();
        let mut tx =
            self.pool.begin().await.map_err(|err| {
                sqlx_error("PersistentTask::ensure_checkpoints_seeded_begin", err)
            })?;

        for plan in file_plans {
            let expected_bytes = plan.expected_size_bytes.unwrap_or(0) as i64;
            for destination in &plan.destinations {
                sqlx::query(
                    "INSERT OR IGNORE INTO task_file_checkpoints (task_id, source_path, destination_path, status, bytes_copied, expected_bytes, actual_destination_path, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&self.id)
                .bind(plan.source.to_string_lossy().to_string())
                .bind(destination.to_string_lossy().to_string())
                .bind(checkpoint_status_to_db_value(FileCheckpointStatus::Pending))
                .bind(0_i64)
                .bind(expected_bytes)
                .bind(Option::<String>::None)
                .bind(now_unix_ms())
                .execute(&mut *tx)
                .await
                .map_err(|err| sqlx_error("PersistentTask::ensure_checkpoints_seeded_insert", err))?;
            }
        }

        tx.commit()
            .await
            .map_err(|err| sqlx_error("PersistentTask::ensure_checkpoints_seeded_commit", err))?;
        debug!(
            "task '{}' ensured {} file checkpoints",
            self.id, checkpoint_count
        );
        Ok(())
    }

    async fn load_non_completed_checkpoints(&self) -> Result<Vec<DbFileCheckpointRow>, CopyError> {
        let rows = sqlx::query(
            "SELECT source_path, destination_path, status, bytes_copied, expected_bytes, actual_destination_path FROM task_file_checkpoints WHERE task_id = ? AND status != ? ORDER BY source_path, destination_path",
        )
        .bind(&self.id)
        .bind(checkpoint_status_to_db_value(FileCheckpointStatus::Completed))
        .fetch_all(&self.pool)
        .await
        .map_err(|err| sqlx_error("PersistentTask::load_non_completed_checkpoints", err))?;

        let checkpoints = rows
            .into_iter()
            .map(|row| {
                Ok(DbFileCheckpointRow {
                    source_path: row.try_get("source_path").map_err(|err| {
                        sqlx_error("PersistentTask::load_non_completed_checkpoints_source", err)
                    })?,
                    destination_path: row.try_get("destination_path").map_err(|err| {
                        sqlx_error(
                            "PersistentTask::load_non_completed_checkpoints_destination",
                            err,
                        )
                    })?,
                    status: row.try_get("status").map_err(|err| {
                        sqlx_error("PersistentTask::load_non_completed_checkpoints_status", err)
                    })?,
                    bytes_copied: row.try_get("bytes_copied").map_err(|err| {
                        sqlx_error("PersistentTask::load_non_completed_checkpoints_bytes", err)
                    })?,
                    expected_bytes: row.try_get("expected_bytes").map_err(|err| {
                        sqlx_error(
                            "PersistentTask::load_non_completed_checkpoints_expected_bytes",
                            err,
                        )
                    })?,
                    actual_destination_path: row.try_get("actual_destination_path").map_err(
                        |err| {
                            sqlx_error(
                                "PersistentTask::load_non_completed_checkpoints_actual_destination",
                                err,
                            )
                        },
                    )?,
                })
            })
            .collect::<Result<Vec<_>, CopyError>>()?;

        debug!(
            "task '{}' loaded {} non-completed checkpoints",
            self.id,
            checkpoints.len()
        );

        Ok(checkpoints)
    }

    async fn update_checkpoint(
        &self,
        source: &Path,
        destination: &Path,
        status: FileCheckpointStatus,
        bytes_copied: u64,
        expected_bytes: u64,
        actual_destination: Option<&Path>,
        last_error: Option<&str>,
    ) -> Result<(), CopyError> {
        debug!(
            "task '{}' checkpoint '{}' -> '{}' updated to {:?} ({}/{} bytes)",
            self.id,
            source.display(),
            destination.display(),
            status,
            bytes_copied,
            expected_bytes
        );
        sqlx::query(
            "UPDATE task_file_checkpoints SET status = ?, bytes_copied = ?, expected_bytes = ?, actual_destination_path = ?, last_error = ?, updated_at_ms = ? WHERE task_id = ? AND source_path = ? AND destination_path = ?",
        )
        .bind(checkpoint_status_to_db_value(status))
        .bind(bytes_copied as i64)
        .bind(expected_bytes as i64)
        .bind(actual_destination.map(|path| path.to_string_lossy().to_string()))
        .bind(last_error)
        .bind(now_unix_ms())
        .bind(&self.id)
        .bind(source.to_string_lossy().to_string())
        .bind(destination.to_string_lossy().to_string())
        .execute(&self.pool)
        .await
        .map_err(|err| sqlx_error("PersistentTask::update_checkpoint", err))?;

        Ok(())
    }

    async fn refresh_progress_from_checkpoints(&self) -> Result<(), CopyError> {
        let rows = sqlx::query(
            "SELECT status, bytes_copied, expected_bytes FROM task_file_checkpoints WHERE task_id = ?",
        )
        .bind(&self.id)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| sqlx_error("PersistentTask::refresh_progress_from_checkpoints_select", err))?;

        let mut total_bytes = 0_u64;
        let mut complete_bytes = 0_u64;

        for row in rows {
            let status_raw: String = row.try_get("status").map_err(|err| {
                sqlx_error(
                    "PersistentTask::refresh_progress_from_checkpoints_status",
                    err,
                )
            })?;
            let bytes = row
                .try_get::<i64, _>("bytes_copied")
                .map_err(|err| {
                    sqlx_error(
                        "PersistentTask::refresh_progress_from_checkpoints_bytes",
                        err,
                    )
                })?
                .max(0) as u64;
            let expected = row
                .try_get::<i64, _>("expected_bytes")
                .map_err(|err| {
                    sqlx_error(
                        "PersistentTask::refresh_progress_from_checkpoints_expected_bytes",
                        err,
                    )
                })?
                .max(0) as u64;

            let target_bytes = expected.max(bytes);
            total_bytes = total_bytes.saturating_add(target_bytes);

            if checkpoint_status_from_db_value(&status_raw) == Some(FileCheckpointStatus::Completed)
            {
                complete_bytes = complete_bytes.saturating_add(target_bytes);
            }
        }

        sqlx::query("UPDATE tasks SET total_bytes = ?, complete_bytes = ? WHERE id = ?")
            .bind(total_bytes as i64)
            .bind(complete_bytes as i64)
            .bind(&self.id)
            .execute(&self.pool)
            .await
            .map_err(|err| {
                sqlx_error(
                    "PersistentTask::refresh_progress_from_checkpoints_update",
                    err,
                )
            })?;

        let mut runtime = self.runtime.write().map_err(|_| {
            lock_poisoned_error("PersistentTask::refresh_progress_from_checkpoints")
        })?;
        runtime.progress = TaskProgress {
            total_bytes,
            complete_bytes,
        };
        runtime.report = None;
        debug!(
            "task '{}' progress refreshed to {}/{} bytes",
            self.id, complete_bytes, total_bytes
        );

        let _ = self.event_tx.send(TaskUpdate::Progress(runtime.progress));

        Ok(())
    }

    async fn apply_overwrite_policy(
        &self,
        source: &Path,
        destination: &Path,
        policy: OverwritePolicy,
    ) -> Result<Option<PathBuf>, CopyError> {
        let destination_exists = self.file_system.exists(destination).await?;
        if !destination_exists {
            debug!(
                "task '{}' destination '{}' is available for source '{}'",
                self.id,
                destination.display(),
                source.display()
            );
            return Ok(Some(destination.to_path_buf()));
        }

        match policy {
            OverwritePolicy::Overwrite => {
                info!(
                    "task '{}' overwriting existing destination '{}'",
                    self.id,
                    destination.display()
                );
                Ok(Some(destination.to_path_buf()))
            }
            OverwritePolicy::Skip => {
                info!(
                    "task '{}' skipping existing destination '{}'",
                    self.id,
                    destination.display()
                );
                Ok(None)
            }
            OverwritePolicy::Rename => {
                let parent = destination.parent().ok_or(CopyError {
                    category: CopyErrorCategory::Path,
                    code: "DESTINATION_PARENT_MISSING",
                    message: format!(
                        "destination '{}' has no parent directory",
                        destination.display()
                    ),
                })?;
                let stem = destination
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("copy");
                let ext = destination
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or("");

                for i in 1..=10_000 {
                    let candidate_name = if ext.is_empty() {
                        format!("{stem} ({i})")
                    } else {
                        format!("{stem} ({i}).{ext}")
                    };
                    let candidate = parent.join(candidate_name);
                    if !self.file_system.exists(&candidate).await? {
                        info!(
                            "task '{}' renaming destination '{}' to '{}'",
                            self.id,
                            destination.display(),
                            candidate.display()
                        );
                        return Ok(Some(candidate));
                    }
                }

                Err(io_error(
                    "RENAME_POLICY_EXHAUSTED",
                    format!(
                        "failed to find available destination name for '{}'",
                        destination.display()
                    ),
                ))
            }
            OverwritePolicy::NewerOnly => {
                let src_meta = fs::metadata(source).await.map_err(|err| {
                    io_error(
                        "SOURCE_METADATA_ERROR",
                        format!(
                            "failed to read source metadata '{}': {err}",
                            source.display()
                        ),
                    )
                })?;
                let dst_meta = fs::metadata(destination).await.map_err(|err| {
                    io_error(
                        "DESTINATION_METADATA_ERROR",
                        format!(
                            "failed to read destination metadata '{}': {err}",
                            destination.display()
                        ),
                    )
                })?;

                let should_copy = match (src_meta.modified(), dst_meta.modified()) {
                    (Ok(src_m), Ok(dst_m)) => src_m > dst_m,
                    _ => true,
                };

                if should_copy {
                    debug!(
                        "task '{}' copying newer source '{}' over '{}'",
                        self.id,
                        source.display(),
                        destination.display()
                    );
                    Ok(Some(destination.to_path_buf()))
                } else {
                    info!(
                        "task '{}' skipped '{}' because destination is newer",
                        self.id,
                        destination.display()
                    );
                    Ok(None)
                }
            }
        }
    }

    async fn copy_file_with_retry(
        &self,
        source: &Path,
        destination: &Path,
        max_retries: u32,
        initial_backoff_ms: u64,
        exponential_factor: u32,
    ) -> Result<u64, CopyError> {
        let mut attempt = 0_u32;
        loop {
            debug!(
                "task '{}' copying '{}' -> '{}' (attempt {}/{})",
                self.id,
                source.display(),
                destination.display(),
                attempt + 1,
                max_retries + 1
            );
            match fs::copy(source, destination).await {
                Ok(bytes) => return Ok(bytes),
                Err(err) if attempt < max_retries => {
                    attempt += 1;
                    let factor = exponential_factor.max(1) as u64;
                    let backoff =
                        initial_backoff_ms.saturating_mul(factor.saturating_pow(attempt - 1));
                    warn!(
                        "task '{}' copy attempt {} for '{}' -> '{}' failed: {}. retrying in {} ms",
                        self.id,
                        attempt,
                        source.display(),
                        destination.display(),
                        err,
                        backoff
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                }
                Err(err) => {
                    error!(
                        "task '{}' exhausted copy retries for '{}' -> '{}': {}",
                        self.id,
                        source.display(),
                        destination.display(),
                        err
                    );
                    return Err(io_error(
                        "FILE_COPY_FAILED",
                        format!(
                            "failed to copy '{}' to '{}': {err}",
                            source.display(),
                            destination.display()
                        ),
                    ));
                }
            }
        }
    }

    async fn verify_copy(
        &self,
        source: &Path,
        destination: &Path,
        policy: VerificationPolicy,
        cached_source_checksum: Option<String>,
    ) -> Result<(), CopyError> {
        if policy == VerificationPolicy::None {
            debug!(
                "task '{}' verification disabled for '{}'",
                self.id,
                source.display()
            );
            return Ok(());
        }

        let source_checksum = match cached_source_checksum {
            Some(value) => value,
            None => self.checksum_provider.checksum_file(source).await?,
        };
        let destination_checksum = self.checksum_provider.checksum_file(destination).await?;

        if source_checksum == destination_checksum {
            debug!(
                "task '{}' verified '{}' -> '{}'",
                self.id,
                source.display(),
                destination.display()
            );
            Ok(())
        } else {
            warn!(
                "task '{}' checksum mismatch for '{}' -> '{}'",
                self.id,
                source.display(),
                destination.display()
            );
            Err(checksum_mismatch_error(source, destination))
        }
    }

    async fn execute_single_checkpoint(&self, row: &DbFileCheckpointRow) -> Result<(), CopyError> {
        let source = PathBuf::from(&row.source_path);
        let destination = PathBuf::from(&row.destination_path);
        let options = self.options()?;

        let expected_bytes = row.expected_bytes.max(0) as u64;
        info!(
            "task '{}' executing checkpoint '{}' -> '{}'",
            self.id,
            source.display(),
            destination.display()
        );

        let cached_source_checksum =
            if options.verification_policy == VerificationPolicy::PreAndPostCopy {
                Some(self.checksum_provider.checksum_file(&source).await?)
            } else {
                None
            };

        self.update_checkpoint(
            &source,
            &destination,
            FileCheckpointStatus::Running,
            row.bytes_copied.max(0) as u64,
            expected_bytes,
            row.actual_destination_path.as_deref().map(Path::new),
            None,
        )
        .await?;

        let (completed_bytes, actual_destination) = if let Some(effective_destination) = self
            .apply_overwrite_policy(&source, &destination, options.overwrite_policy)
            .await?
        {
            if let Some(parent) = effective_destination.parent() {
                self.file_system.create_dir_all(parent).await?;
            }

            let bytes = self
                .copy_file_with_retry(
                    &source,
                    &effective_destination,
                    options.retry_policy.max_retries,
                    options.retry_policy.initial_backoff_ms,
                    options.retry_policy.exponential_factor,
                )
                .await?;

            self.verify_copy(
                &source,
                &effective_destination,
                options.verification_policy,
                cached_source_checksum,
            )
            .await?;

            (bytes.max(expected_bytes), Some(effective_destination))
        } else {
            (expected_bytes, Some(destination.clone()))
        };

        self.update_checkpoint(
            &source,
            &destination,
            FileCheckpointStatus::Completed,
            completed_bytes,
            expected_bytes,
            actual_destination.as_deref(),
            None,
        )
        .await?;
        self.refresh_progress_from_checkpoints().await?;
        debug!(
            "task '{}' completed checkpoint '{}' -> '{}'",
            self.id,
            source.display(),
            destination.display()
        );

        Ok(())
    }

    async fn current_run_outcome(&self) -> Result<RunOutcome, CopyError> {
        match self.state().await? {
            TaskState::Running => Ok(RunOutcome::Completed),
            TaskState::Paused => Ok(RunOutcome::Paused),
            TaskState::Cancelled => Ok(RunOutcome::Cancelled),
            current => Err(invalid_state_error(&self.id, current, TaskState::Running)),
        }
    }

    async fn execute_copy_workload(&self) -> Result<RunOutcome, CopyError> {
        let pending = self.load_non_completed_checkpoints().await?;

        for row in pending {
            match self.current_run_outcome().await? {
                RunOutcome::Completed => {}
                other => {
                    info!(
                        "task '{}' stopping workload early with outcome {:?}",
                        self.id, other
                    );
                    return Ok(other);
                }
            }

            if checkpoint_status_from_db_value(&row.status).is_none() {
                return Err(CopyError {
                    category: CopyErrorCategory::Unknown,
                    code: "UNKNOWN_FILE_CHECKPOINT_STATUS",
                    message: format!("unknown checkpoint status '{}'", row.status),
                });
            }

            if let Err(err) = self.execute_single_checkpoint(&row).await {
                let source = PathBuf::from(&row.source_path);
                let destination = PathBuf::from(&row.destination_path);
                error!(
                    "task '{}' failed checkpoint '{}' -> '{}': {}",
                    self.id,
                    source.display(),
                    destination.display(),
                    err.message
                );
                self.update_checkpoint(
                    &source,
                    &destination,
                    FileCheckpointStatus::Failed,
                    row.bytes_copied.max(0) as u64,
                    row.expected_bytes.max(0) as u64,
                    row.actual_destination_path.as_deref().map(Path::new),
                    Some(&err.message),
                )
                .await?;
                return Err(err);
            }

            match self.current_run_outcome().await? {
                RunOutcome::Completed => {}
                other => {
                    info!(
                        "task '{}' stopped after checkpoint with outcome {:?}",
                        self.id, other
                    );
                    return Ok(other);
                }
            }
        }

        Ok(self.current_run_outcome().await?)
    }

    async fn run_with_lock(&self) -> Result<(), CopyError> {
        let current = self.state().await?;
        info!("task '{}' starting run from state {:?}", self.id, current);

        match current {
            TaskState::Created => {
                self.transition_state(TaskState::Planned).await?;
                self.transition_state(TaskState::Running).await?;
            }
            TaskState::Planned | TaskState::Paused => {
                self.transition_state(TaskState::Running).await?;
            }
            TaskState::Running => {}
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled => {
                return Err(invalid_state_error(&self.id, current, TaskState::Running));
            }
        }

        let file_plans = self.ensure_file_plans().await?;
        self.ensure_checkpoints_seeded(&file_plans).await?;
        self.refresh_progress_from_checkpoints().await?;

        match self.execute_copy_workload().await {
            Ok(RunOutcome::Completed) => {
                if self.state().await? == TaskState::Running {
                    self.transition_state(TaskState::Completed).await?;
                }
                info!("task '{}' completed successfully", self.id);
                Ok(())
            }
            Ok(RunOutcome::Paused) => {
                info!("task '{}' paused", self.id);
                Ok(())
            }
            Ok(RunOutcome::Cancelled) => {
                warn!("task '{}' cancelled during execution", self.id);
                Ok(())
            }
            Err(err) => {
                if self.state().await.unwrap_or(TaskState::Failed) == TaskState::Running {
                    let _ = self.transition_state(TaskState::Failed).await;
                }
                error!("task '{}' failed: {}", self.id, err.message);
                Err(err)
            }
        }
    }
}

fn state_to_db_value(state: TaskState) -> &'static str {
    match state {
        TaskState::Created => "created",
        TaskState::Planned => "planned",
        TaskState::Running => "running",
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
        TaskState::Paused => "paused",
    }
}

#[async_trait]
impl Task for PersistentTask {
    fn snapshot(&self) -> CopyTask {
        self.runtime
            .read()
            .map(|runtime| runtime.snapshot.clone())
            .unwrap_or_else(|_| {
                CopyTask::new(
                    self.id.clone(),
                    PathBuf::new(),
                    Vec::new(),
                    Default::default(),
                )
                .with_state(TaskState::Failed)
            })
    }

    fn id(&self) -> &str {
        &self.id
    }

    async fn run(&self) -> Result<(), CopyError> {
        let _execution = match self.execution_lock.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                if self.state().await? == TaskState::Running {
                    debug!("task '{}' run requested while already running", self.id);
                    return Ok(());
                }
                debug!("task '{}' waiting for active execution lock", self.id);
                self.execution_lock.lock().await
            }
        };

        self.run_with_lock().await
    }

    async fn pause(&self) -> Result<(), CopyError> {
        self.transition_state(TaskState::Paused).await
    }

    async fn resume(&self) -> Result<(), CopyError> {
        match self.state().await? {
            TaskState::Paused => self.run().await,
            TaskState::Running => Ok(()),
            current => Err(invalid_state_error(&self.id, current, TaskState::Running)),
        }
    }

    async fn cancel(&self) -> Result<(), CopyError> {
        self.transition_state(TaskState::Cancelled).await
    }

    async fn state(&self) -> Result<TaskState, CopyError> {
        let runtime = self
            .runtime
            .read()
            .map_err(|_| lock_poisoned_error("PersistentTask::state"))?;
        Ok(runtime.snapshot.state)
    }

    async fn progress(&self) -> Result<TaskProgress, CopyError> {
        let runtime = self
            .runtime
            .read()
            .map_err(|_| lock_poisoned_error("PersistentTask::progress"))?;
        Ok(runtime.progress)
    }

    fn subscribe(&self) -> Result<TaskStream, CopyError> {
        Ok(self.event_tx.subscribe())
    }

    async fn report(&self) -> Result<CopyReport, CopyError> {
        let mut runtime = self
            .runtime
            .write()
            .map_err(|_| lock_poisoned_error("PersistentTask::report"))?;
        let report = runtime
            .report
            .clone()
            .unwrap_or_else(|| Self::build_report(&runtime));
        runtime.report = Some(report.clone());
        Ok(report)
    }
}
