use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use log::{debug, error, info, warn};
use sqlx::{Row, SqlitePool};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, Mutex as AsyncMutex};
use tokio::task::JoinSet;

use crate::domain::{
    CopyError, CopyErrorCategory, CopyReport, CopyTask, DestinationCopyStatus,
    DestinationPostWriteStatus, DestinationReport, FileFailure, FilePlan, OverwritePolicy,
    TaskProgress, TaskState,
};
use crate::infrastructure::{ChecksumProvider, FileSystem};
use crate::pipeline::{
    DestinationChecksumVerifyStage, PipelineArtifacts, PostWriteContext, PostWritePipeline,
    PostWritePipelineMode, SourceChunk, SourceHashStage, SourceObserverPipeline,
    SourceObserverStage, SourcePipelineMode, StageArtifacts, StageId,
};

use super::artifact_store::{ArtifactStore, SourceFingerprint};
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
    copy_status: String,
    post_write_status: String,
    bytes_copied: i64,
    expected_bytes: i64,
    actual_destination_path: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileCheckpointStatus {
    Pending,
    Writing,
    PostWriteRunning,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunOutcome {
    Continue,
    Paused,
    Cancelled,
}

#[derive(Debug, Clone)]
struct DestinationBranchPlan {
    requested_destination: PathBuf,
    actual_destination: PathBuf,
    expected_bytes: u64,
}

#[derive(Debug, Clone)]
struct DestinationBranchResult {
    destination_path: PathBuf,
    actual_destination_path: Option<PathBuf>,
    bytes_written: u64,
    copy_status: DestinationCopyStatus,
    post_write_status: DestinationPostWriteStatus,
    error: Option<CopyError>,
    retries: u32,
}

#[derive(Debug)]
enum FileSessionResult {
    Completed(Vec<DestinationBranchResult>),
    Stopped(RunOutcome),
}

#[derive(Debug)]
enum FanOutAttemptResult {
    Completed {
        observer_artifacts: PipelineArtifacts,
        branch_results: Vec<DestinationBranchResult>,
    },
    Stopped(RunOutcome),
}

type BranchMessage = Option<SourceChunk>;

fn checkpoint_status_to_db_value(status: FileCheckpointStatus) -> &'static str {
    match status {
        FileCheckpointStatus::Pending => "pending",
        FileCheckpointStatus::Writing => "writing",
        FileCheckpointStatus::PostWriteRunning => "post_write_running",
        FileCheckpointStatus::Completed => "completed",
        FileCheckpointStatus::Failed => "failed",
    }
}

fn checkpoint_status_from_db_value(value: &str) -> Option<FileCheckpointStatus> {
    match value {
        "pending" => Some(FileCheckpointStatus::Pending),
        "writing" => Some(FileCheckpointStatus::Writing),
        "post_write_running" => Some(FileCheckpointStatus::PostWriteRunning),
        "completed" => Some(FileCheckpointStatus::Completed),
        "failed" => Some(FileCheckpointStatus::Failed),
        _ => None,
    }
}

fn copy_status_to_db_value(status: DestinationCopyStatus) -> &'static str {
    match status {
        DestinationCopyStatus::Pending => "pending",
        DestinationCopyStatus::Skipped => "skipped",
        DestinationCopyStatus::Succeeded => "succeeded",
        DestinationCopyStatus::Failed => "failed",
    }
}

fn copy_status_from_db_value(value: &str) -> Option<DestinationCopyStatus> {
    match value {
        "pending" => Some(DestinationCopyStatus::Pending),
        "skipped" => Some(DestinationCopyStatus::Skipped),
        "succeeded" => Some(DestinationCopyStatus::Succeeded),
        "failed" => Some(DestinationCopyStatus::Failed),
        _ => None,
    }
}

fn post_write_status_to_db_value(status: DestinationPostWriteStatus) -> &'static str {
    match status {
        DestinationPostWriteStatus::Pending => "pending",
        DestinationPostWriteStatus::NotRun => "not_run",
        DestinationPostWriteStatus::Succeeded => "succeeded",
        DestinationPostWriteStatus::Failed => "failed",
    }
}

fn post_write_status_from_db_value(value: &str) -> Option<DestinationPostWriteStatus> {
    match value {
        "pending" => Some(DestinationPostWriteStatus::Pending),
        "not_run" => Some(DestinationPostWriteStatus::NotRun),
        "succeeded" => Some(DestinationPostWriteStatus::Succeeded),
        "failed" => Some(DestinationPostWriteStatus::Failed),
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

fn source_observer_branch_name(source: &Path) -> String {
    format!("source-observer:{}", source.display())
}

/// SQLite-backed concrete task handle with file/destination checkpoint execution.
pub struct PersistentTask {
    id: String,
    pool: SqlitePool,
    runtime: RwLock<TaskRuntime>,
    event_tx: broadcast::Sender<TaskUpdate>,
    file_system: Arc<dyn FileSystem>,
    checksum_provider: Arc<dyn ChecksumProvider>,
    artifact_store: Arc<dyn ArtifactStore>,
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
        artifact_store: Arc<dyn ArtifactStore>,
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
            artifact_store,
            execution_lock: AsyncMutex::new(()),
        }
    }

    async fn persist_state(&self, state: TaskState) -> Result<(), CopyError> {
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
        Ok(plans)
    }

    async fn ensure_checkpoints_seeded(&self, file_plans: &[FilePlan]) -> Result<(), CopyError> {
        let mut tx =
            self.pool.begin().await.map_err(|err| {
                sqlx_error("PersistentTask::ensure_checkpoints_seeded_begin", err)
            })?;

        for plan in file_plans {
            let expected_bytes = plan.expected_size_bytes.unwrap_or(0) as i64;
            for destination in &plan.destinations {
                sqlx::query(
                    "INSERT OR IGNORE INTO task_file_checkpoints (task_id, source_path, destination_path, status, copy_status, post_write_status, bytes_copied, expected_bytes, actual_destination_path, last_error, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&self.id)
                .bind(plan.source.to_string_lossy().to_string())
                .bind(destination.to_string_lossy().to_string())
                .bind(checkpoint_status_to_db_value(FileCheckpointStatus::Pending))
                .bind(copy_status_to_db_value(DestinationCopyStatus::Pending))
                .bind(post_write_status_to_db_value(DestinationPostWriteStatus::Pending))
                .bind(0_i64)
                .bind(expected_bytes)
                .bind(Option::<String>::None)
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
        Ok(())
    }

    async fn load_checkpoints(
        &self,
        only_non_completed: bool,
    ) -> Result<Vec<DbFileCheckpointRow>, CopyError> {
        let sql = if only_non_completed {
            "SELECT source_path, destination_path, status, copy_status, post_write_status, bytes_copied, expected_bytes, actual_destination_path, last_error FROM task_file_checkpoints WHERE task_id = ? AND status != ? ORDER BY source_path, destination_path"
        } else {
            "SELECT source_path, destination_path, status, copy_status, post_write_status, bytes_copied, expected_bytes, actual_destination_path, last_error FROM task_file_checkpoints WHERE task_id = ? ORDER BY source_path, destination_path"
        };

        let mut query = sqlx::query(sql).bind(&self.id);
        if only_non_completed {
            query = query.bind(checkpoint_status_to_db_value(
                FileCheckpointStatus::Completed,
            ));
        }

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| sqlx_error("PersistentTask::load_checkpoints", err))?;

        rows.into_iter()
            .map(|row| {
                Ok(DbFileCheckpointRow {
                    source_path: row.try_get("source_path").map_err(|err| {
                        sqlx_error("PersistentTask::load_checkpoints_source_path", err)
                    })?,
                    destination_path: row.try_get("destination_path").map_err(|err| {
                        sqlx_error("PersistentTask::load_checkpoints_destination_path", err)
                    })?,
                    status: row.try_get("status").map_err(|err| {
                        sqlx_error("PersistentTask::load_checkpoints_status", err)
                    })?,
                    copy_status: row.try_get("copy_status").map_err(|err| {
                        sqlx_error("PersistentTask::load_checkpoints_copy_status", err)
                    })?,
                    post_write_status: row.try_get("post_write_status").map_err(|err| {
                        sqlx_error("PersistentTask::load_checkpoints_post_write_status", err)
                    })?,
                    bytes_copied: row.try_get("bytes_copied").map_err(|err| {
                        sqlx_error("PersistentTask::load_checkpoints_bytes_copied", err)
                    })?,
                    expected_bytes: row.try_get("expected_bytes").map_err(|err| {
                        sqlx_error("PersistentTask::load_checkpoints_expected_bytes", err)
                    })?,
                    actual_destination_path: row.try_get("actual_destination_path").map_err(
                        |err| {
                            sqlx_error("PersistentTask::load_checkpoints_actual_destination", err)
                        },
                    )?,
                    last_error: row.try_get("last_error").map_err(|err| {
                        sqlx_error("PersistentTask::load_checkpoints_last_error", err)
                    })?,
                })
            })
            .collect()
    }

    async fn update_checkpoint(
        &self,
        source: &Path,
        destination: &Path,
        status: FileCheckpointStatus,
        copy_status: DestinationCopyStatus,
        post_write_status: DestinationPostWriteStatus,
        bytes_copied: u64,
        expected_bytes: u64,
        actual_destination: Option<&Path>,
        last_error: Option<&str>,
    ) -> Result<(), CopyError> {
        sqlx::query(
            "UPDATE task_file_checkpoints SET status = ?, copy_status = ?, post_write_status = ?, bytes_copied = ?, expected_bytes = ?, actual_destination_path = ?, last_error = ?, updated_at_ms = ? WHERE task_id = ? AND source_path = ? AND destination_path = ?",
        )
        .bind(checkpoint_status_to_db_value(status))
        .bind(copy_status_to_db_value(copy_status))
        .bind(post_write_status_to_db_value(post_write_status))
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
            let status_raw: String = row
                .try_get("status")
                .map_err(|err| sqlx_error("PersistentTask::refresh_progress_status", err))?;
            let bytes = row
                .try_get::<i64, _>("bytes_copied")
                .map_err(|err| sqlx_error("PersistentTask::refresh_progress_bytes", err))?
                .max(0) as u64;
            let expected = row
                .try_get::<i64, _>("expected_bytes")
                .map_err(|err| sqlx_error("PersistentTask::refresh_progress_expected", err))?
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
            return Ok(Some(destination.to_path_buf()));
        }

        match policy {
            OverwritePolicy::Overwrite => Ok(Some(destination.to_path_buf())),
            OverwritePolicy::Skip => Ok(None),
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
                    Ok(Some(destination.to_path_buf()))
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn current_run_outcome(&self) -> Result<RunOutcome, CopyError> {
        match self.state().await? {
            TaskState::Running => Ok(RunOutcome::Continue),
            TaskState::Paused => Ok(RunOutcome::Paused),
            TaskState::Cancelled => Ok(RunOutcome::Cancelled),
            current => Err(invalid_state_error(&self.id, current, TaskState::Running)),
        }
    }

    async fn resolve_destination_branches(
        &self,
        source: &Path,
        rows: &[DbFileCheckpointRow],
        policy: OverwritePolicy,
    ) -> Result<(Vec<DestinationBranchPlan>, Vec<DestinationBranchResult>), CopyError> {
        let mut copy_branches = Vec::new();
        let mut completed_results = Vec::new();

        for row in rows {
            if checkpoint_status_from_db_value(&row.status).is_none() {
                return Err(CopyError {
                    category: CopyErrorCategory::Unknown,
                    code: "UNKNOWN_FILE_CHECKPOINT_STATUS",
                    message: format!("unknown checkpoint status '{}'", row.status),
                });
            }

            let requested_destination = PathBuf::from(&row.destination_path);
            let expected_bytes = row.expected_bytes.max(0) as u64;
            let actual_destination = if let Some(restored) = row.actual_destination_path.as_deref()
            {
                Some(PathBuf::from(restored))
            } else {
                match self
                    .apply_overwrite_policy(source, &requested_destination, policy)
                    .await
                {
                    Ok(actual_destination) => actual_destination,
                    Err(err) => {
                        self.update_checkpoint(
                            source,
                            &requested_destination,
                            FileCheckpointStatus::Failed,
                            DestinationCopyStatus::Failed,
                            DestinationPostWriteStatus::NotRun,
                            0,
                            expected_bytes,
                            None,
                            Some(&err.message),
                        )
                        .await?;
                        completed_results.push(DestinationBranchResult {
                            destination_path: requested_destination,
                            actual_destination_path: None,
                            bytes_written: 0,
                            copy_status: DestinationCopyStatus::Failed,
                            post_write_status: DestinationPostWriteStatus::NotRun,
                            error: Some(err),
                            retries: 0,
                        });
                        continue;
                    }
                }
            };

            match actual_destination {
                Some(actual_destination) => {
                    copy_branches.push(DestinationBranchPlan {
                        requested_destination,
                        actual_destination,
                        expected_bytes,
                    });
                }
                None => {
                    self.update_checkpoint(
                        source,
                        &requested_destination,
                        FileCheckpointStatus::Completed,
                        DestinationCopyStatus::Skipped,
                        DestinationPostWriteStatus::NotRun,
                        expected_bytes,
                        expected_bytes,
                        Some(&requested_destination),
                        None,
                    )
                    .await?;
                    completed_results.push(DestinationBranchResult {
                        destination_path: requested_destination.clone(),
                        actual_destination_path: Some(requested_destination),
                        bytes_written: 0,
                        copy_status: DestinationCopyStatus::Skipped,
                        post_write_status: DestinationPostWriteStatus::NotRun,
                        error: None,
                        retries: 0,
                    });
                }
            }
        }

        Ok((copy_branches, completed_results))
    }

    fn build_effective_source_observer_pipeline(
        &self,
        snapshot: &CopyTask,
    ) -> Option<SourceObserverPipeline> {
        let mut pipeline =
            snapshot.flow.source_observer.clone().unwrap_or_else(|| {
                SourceObserverPipeline::new(snapshot.flow.source_pipeline_mode())
            });
        pipeline.mode = snapshot.flow.source_pipeline_mode();

        if snapshot
            .options
            .verification_policy
            .requires_post_write_verification()
            || snapshot
                .options
                .verification_policy
                .requires_source_hash_observer()
        {
            pipeline
                .stages
                .push(Arc::new(SourceHashStage::new(Arc::clone(
                    &self.checksum_provider,
                ))));
        }

        if pipeline.stages.is_empty() {
            None
        } else {
            Some(pipeline)
        }
    }

    fn build_effective_post_write_pipeline(
        &self,
        snapshot: &CopyTask,
    ) -> Option<PostWritePipeline> {
        let mut pipeline =
            snapshot.flow.post_write.clone().unwrap_or_else(|| {
                PostWritePipeline::new(snapshot.flow.post_write_pipeline_mode())
            });
        pipeline.mode = snapshot.flow.post_write_pipeline_mode();

        if snapshot
            .options
            .verification_policy
            .requires_post_write_verification()
        {
            pipeline
                .stages
                .push(Arc::new(DestinationChecksumVerifyStage::new(Arc::clone(
                    &self.checksum_provider,
                ))));
        }

        if pipeline.stages.is_empty() {
            None
        } else {
            Some(pipeline)
        }
    }

    async fn source_fingerprint(&self, source: &Path) -> Result<SourceFingerprint, CopyError> {
        let metadata = fs::metadata(source).await.map_err(|err| {
            io_error(
                "SOURCE_METADATA_ERROR",
                format!(
                    "failed to read source metadata '{}': {}",
                    source.display(),
                    err
                ),
            )
        })?;
        let modified_unix_ms = metadata.modified().ok().and_then(|modified| {
            modified
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|duration| duration.as_millis() as i64)
        });

        Ok(SourceFingerprint {
            size_bytes: metadata.len(),
            modified_unix_ms,
        })
    }

    async fn load_reusable_source_artifacts(
        &self,
        source: &Path,
    ) -> Result<Option<PipelineArtifacts>, CopyError> {
        let Some(stored) = self
            .artifact_store
            .load_source_artifacts(&self.id, source)
            .await?
        else {
            return Ok(None);
        };

        let current_fingerprint = self.source_fingerprint(source).await?;
        if stored.fingerprint == current_fingerprint {
            debug!(
                "task '{}' reusing persisted source artifacts for '{}'",
                self.id,
                source.display()
            );
            Ok(Some(stored.artifacts))
        } else {
            warn!(
                "task '{}' invalidating stale source artifacts for '{}'",
                self.id,
                source.display()
            );
            self.artifact_store
                .delete_source_artifacts(&self.id, source)
                .await?;
            Ok(None)
        }
    }

    async fn persist_source_artifacts(
        &self,
        source: &Path,
        artifacts: &PipelineArtifacts,
    ) -> Result<(), CopyError> {
        let fingerprint = self.source_fingerprint(source).await?;
        self.artifact_store
            .save_source_artifacts(&self.id, source, &fingerprint, artifacts)
            .await
    }

    async fn run_source_observer_serial(
        &self,
        task: &CopyTask,
        plan: &FilePlan,
        pipeline: &SourceObserverPipeline,
    ) -> Result<PipelineArtifacts, CopyError> {
        let options = self.options()?;
        let mut reader = fs::File::open(&plan.source).await.map_err(|err| {
            io_error(
                "SOURCE_OPEN_ERROR",
                format!("failed to open source '{}': {}", plan.source.display(), err),
            )
        })?;
        let mut buffer = vec![0_u8; options.buffer_size_bytes.max(1)];
        let mut offset = 0_u64;

        loop {
            let read_len = reader.read(&mut buffer).await.map_err(|err| {
                io_error(
                    "SOURCE_READ_ERROR",
                    format!("failed to read source '{}': {}", plan.source.display(), err),
                )
            })?;
            if read_len == 0 {
                break;
            }

            let chunk = SourceChunk {
                offset,
                bytes: Arc::<[u8]>::from(buffer[..read_len].to_vec()),
            };
            offset = offset.saturating_add(read_len as u64);

            for stage in &pipeline.stages {
                stage.observe_chunk(task, plan, &chunk).await?;
            }
        }

        let mut artifacts = PipelineArtifacts::new();
        for stage in &pipeline.stages {
            artifacts.insert_stage_output(stage.id(), stage.finish(task, plan).await?);
        }
        Ok(artifacts)
    }

    async fn source_observer_branch_task(
        task: CopyTask,
        plan: FilePlan,
        stage: Arc<dyn SourceObserverStage>,
        mut rx: mpsc::Receiver<BranchMessage>,
    ) -> Result<(StageId, StageArtifacts), CopyError> {
        let stage_id = stage.id();
        while let Some(message) = rx.recv().await {
            match message {
                Some(chunk) => stage.observe_chunk(&task, &plan, &chunk).await?,
                None => break,
            }
        }

        Ok((stage_id, stage.finish(&task, &plan).await?))
    }

    async fn write_branch_task(
        branch_name: String,
        path: PathBuf,
        mut rx: mpsc::Receiver<BranchMessage>,
    ) -> Result<u64, CopyError> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|err| {
                io_error(
                    "DESTINATION_OPEN_ERROR",
                    format!(
                        "failed to open destination '{}' for {}: {}",
                        path.display(),
                        branch_name,
                        err
                    ),
                )
            })?;
        let mut written = 0_u64;

        while let Some(message) = rx.recv().await {
            match message {
                Some(chunk) => {
                    file.write_all(chunk.bytes.as_ref()).await.map_err(|err| {
                        io_error(
                            "DESTINATION_WRITE_ERROR",
                            format!("failed to write destination '{}': {}", path.display(), err),
                        )
                    })?;
                    written = written.saturating_add(chunk.len() as u64);
                }
                None => break,
            }
        }

        file.flush().await.map_err(|err| {
            io_error(
                "DESTINATION_FLUSH_ERROR",
                format!("failed to flush destination '{}': {}", path.display(), err),
            )
        })?;

        Ok(written)
    }

    async fn fan_out_copy_attempt(
        &self,
        task: &CopyTask,
        file_plan: &FilePlan,
        branches: &[DestinationBranchPlan],
        observer_pipeline: Option<&SourceObserverPipeline>,
    ) -> Result<FanOutAttemptResult, CopyError> {
        let options = self.options()?;
        let mut reader = fs::File::open(&file_plan.source).await.map_err(|err| {
            io_error(
                "SOURCE_OPEN_ERROR",
                format!(
                    "failed to open source '{}': {}",
                    file_plan.source.display(),
                    err
                ),
            )
        })?;
        let mut writers = Vec::with_capacity(branches.len());
        let mut join_set = JoinSet::new();

        for branch in branches {
            let (tx, rx) = mpsc::channel::<BranchMessage>(4);
            writers.push((branch.requested_destination.clone(), tx));
            let requested = branch.requested_destination.clone();
            let actual = branch.actual_destination.clone();
            join_set.spawn(async move {
                let branch_name = format!("writer:{}", requested.display());
                let result = Self::write_branch_task(branch_name, actual.clone(), rx).await;
                (requested, actual, result)
            });
        }

        let mut observer_senders = Vec::new();
        let mut observer_handles = Vec::new();
        if let Some(observer_pipeline) = observer_pipeline {
            for stage in &observer_pipeline.stages {
                let (tx, rx) = mpsc::channel::<BranchMessage>(4);
                observer_senders.push((stage.id().to_string(), tx));
                let branch_name = format!(
                    "{}:{}",
                    source_observer_branch_name(&file_plan.source),
                    stage.id()
                );
                let task = task.clone();
                let plan = file_plan.clone();
                let stage = Arc::clone(stage);
                let handle = tokio::spawn(async move {
                    debug!("starting {}", branch_name);
                    Self::source_observer_branch_task(task, plan, stage, rx).await
                });
                observer_handles.push(handle);
            }
        }

        let mut buffer = vec![0_u8; options.buffer_size_bytes.max(1)];
        let mut offset = 0_u64;
        let mut stopped = None;

        loop {
            match self.current_run_outcome().await? {
                RunOutcome::Continue => {}
                outcome => {
                    stopped = Some(outcome);
                    break;
                }
            }

            let read_len = reader.read(&mut buffer).await.map_err(|err| {
                io_error(
                    "SOURCE_READ_ERROR",
                    format!(
                        "failed to read source '{}': {}",
                        file_plan.source.display(),
                        err
                    ),
                )
            })?;
            if read_len == 0 {
                break;
            }

            let chunk = SourceChunk {
                offset,
                bytes: Arc::<[u8]>::from(buffer[..read_len].to_vec()),
            };
            offset = offset.saturating_add(read_len as u64);

            let mut writer_index = 0;
            while writer_index < writers.len() {
                let (_, tx) = &writers[writer_index];
                if tx.send(Some(chunk.clone())).await.is_err() {
                    writers.swap_remove(writer_index);
                    continue;
                }
                writer_index += 1;
            }

            let mut observer_index = 0;
            while observer_index < observer_senders.len() {
                let (_, tx) = &observer_senders[observer_index];
                if tx.send(Some(chunk.clone())).await.is_err() {
                    observer_senders.swap_remove(observer_index);
                    continue;
                }
                observer_index += 1;
            }
        }

        for (_, tx) in writers {
            let _ = tx.send(None).await;
        }
        for (_, tx) in observer_senders {
            let _ = tx.send(None).await;
        }

        let mut branch_results = Vec::new();
        while let Some(joined) = join_set.join_next().await {
            let (requested, actual, result) = joined.map_err(|err| CopyError {
                category: CopyErrorCategory::Interrupted,
                code: "WRITER_JOIN_ERROR",
                message: format!(
                    "writer branch for '{}' panicked or was cancelled: {}",
                    requested_placeholder(&err),
                    err
                ),
            })?;
            match result {
                Ok(bytes_written) => branch_results.push(DestinationBranchResult {
                    destination_path: requested,
                    actual_destination_path: Some(actual),
                    bytes_written,
                    copy_status: DestinationCopyStatus::Succeeded,
                    post_write_status: DestinationPostWriteStatus::Pending,
                    error: None,
                    retries: 0,
                }),
                Err(err) => branch_results.push(DestinationBranchResult {
                    destination_path: requested,
                    actual_destination_path: Some(actual),
                    bytes_written: 0,
                    copy_status: DestinationCopyStatus::Failed,
                    post_write_status: DestinationPostWriteStatus::NotRun,
                    error: Some(err),
                    retries: 0,
                }),
            }
        }

        let mut observer_artifacts = PipelineArtifacts::new();
        for handle in observer_handles {
            let (stage_id, artifacts) = handle.await.map_err(|err| CopyError {
                category: CopyErrorCategory::Interrupted,
                code: "SOURCE_OBSERVER_JOIN_ERROR",
                message: format!(
                    "source observer for '{}' panicked or was cancelled: {}",
                    file_plan.source.display(),
                    err
                ),
            })??;
            observer_artifacts.insert_stage_output(stage_id, artifacts);
        }

        if let Some(outcome) = stopped {
            return Ok(FanOutAttemptResult::Stopped(outcome));
        }

        Ok(FanOutAttemptResult::Completed {
            observer_artifacts,
            branch_results,
        })
    }

    async fn run_post_write_pipeline(
        &self,
        source: &Path,
        branches: Vec<DestinationBranchResult>,
        source_artifacts: &PipelineArtifacts,
        pipeline: Option<&PostWritePipeline>,
    ) -> Result<Vec<DestinationBranchResult>, CopyError> {
        let snapshot = self.snapshot_task()?;
        match snapshot.flow.post_write_pipeline_mode() {
            PostWritePipelineMode::SerialAfterWrite => {}
        }

        let Some(pipeline) = pipeline else {
            let mut results = Vec::with_capacity(branches.len());
            for mut branch in branches {
                let actual_destination = branch
                    .actual_destination_path
                    .clone()
                    .unwrap_or_else(|| branch.destination_path.clone());
                let expected_bytes = branch.bytes_written;
                self.update_checkpoint(
                    source,
                    &branch.destination_path,
                    FileCheckpointStatus::Completed,
                    DestinationCopyStatus::Succeeded,
                    DestinationPostWriteStatus::NotRun,
                    branch.bytes_written.max(expected_bytes),
                    expected_bytes,
                    Some(&actual_destination),
                    None,
                )
                .await?;
                branch.post_write_status = DestinationPostWriteStatus::NotRun;
                results.push(branch);
            }
            return Ok(results);
        };

        let mut join_set = JoinSet::new();
        for branch in branches {
            let requested = branch.destination_path.clone();
            let actual = branch
                .actual_destination_path
                .clone()
                .unwrap_or_else(|| requested.clone());
            let expected_bytes = branch.bytes_written.max(
                fs::metadata(source)
                    .await
                    .map(|metadata| metadata.len())
                    .unwrap_or(branch.bytes_written),
            );
            self.update_checkpoint(
                source,
                &requested,
                FileCheckpointStatus::PostWriteRunning,
                DestinationCopyStatus::Succeeded,
                DestinationPostWriteStatus::Pending,
                branch.bytes_written,
                expected_bytes,
                Some(&actual),
                None,
            )
            .await?;

            self.artifact_store
                .delete_destination_artifacts(&self.id, source, &actual)
                .await?;

            let source_path = source.to_path_buf();
            let pipeline_artifacts = source_artifacts.clone();
            let task_id = self.id.clone();
            let stages = pipeline.stages.clone();
            join_set.spawn(async move {
                let ctx = PostWriteContext {
                    task_id,
                    source_path,
                    requested_destination_path: requested,
                    actual_destination_path: actual,
                    bytes_written: branch.bytes_written,
                    expected_bytes,
                    pipeline_artifacts,
                };
                let mut stage_outputs = PipelineArtifacts::new();
                let mut result = Ok(());
                for stage in stages {
                    let stage_key = stage.id();
                    match stage.execute(&ctx).await {
                        Ok(artifacts) => {
                            stage_outputs.insert_stage_output(stage_key, artifacts);
                        }
                        Err(err) => {
                            result = Err(err);
                            break;
                        }
                    }
                }
                (ctx, stage_outputs, result)
            });
        }

        let mut results = Vec::new();
        while let Some(joined) = join_set.join_next().await {
            let (ctx, stage_outputs, result) = joined.map_err(|err| CopyError {
                category: CopyErrorCategory::Interrupted,
                code: "POST_WRITE_JOIN_ERROR",
                message: format!(
                    "post-write branch for '{}' panicked or was cancelled: {}",
                    source.display(),
                    err
                ),
            })?;

            self.artifact_store
                .save_destination_artifacts(
                    &self.id,
                    &ctx.source_path,
                    &ctx.actual_destination_path,
                    &stage_outputs,
                )
                .await?;

            match result {
                Ok(()) => {
                    self.update_checkpoint(
                        &ctx.source_path,
                        &ctx.requested_destination_path,
                        FileCheckpointStatus::Completed,
                        DestinationCopyStatus::Succeeded,
                        DestinationPostWriteStatus::Succeeded,
                        ctx.bytes_written.max(ctx.expected_bytes),
                        ctx.expected_bytes,
                        Some(&ctx.actual_destination_path),
                        None,
                    )
                    .await?;
                    results.push(DestinationBranchResult {
                        destination_path: ctx.requested_destination_path,
                        actual_destination_path: Some(ctx.actual_destination_path),
                        bytes_written: ctx.bytes_written,
                        copy_status: DestinationCopyStatus::Succeeded,
                        post_write_status: DestinationPostWriteStatus::Succeeded,
                        error: None,
                        retries: 0,
                    });
                }
                Err(err) => {
                    self.update_checkpoint(
                        &ctx.source_path,
                        &ctx.requested_destination_path,
                        FileCheckpointStatus::Failed,
                        DestinationCopyStatus::Succeeded,
                        DestinationPostWriteStatus::Failed,
                        ctx.bytes_written,
                        ctx.expected_bytes,
                        Some(&ctx.actual_destination_path),
                        Some(&err.message),
                    )
                    .await?;
                    results.push(DestinationBranchResult {
                        destination_path: ctx.requested_destination_path,
                        actual_destination_path: Some(ctx.actual_destination_path),
                        bytes_written: ctx.bytes_written,
                        copy_status: DestinationCopyStatus::Succeeded,
                        post_write_status: DestinationPostWriteStatus::Failed,
                        error: Some(err),
                        retries: 0,
                    });
                }
            }
        }

        Ok(results)
    }

    async fn execute_file_session(
        &self,
        file_plan: &FilePlan,
        rows: &[DbFileCheckpointRow],
    ) -> Result<FileSessionResult, CopyError> {
        let snapshot = self.snapshot_task()?;
        let options = snapshot.options.clone();
        let source = &file_plan.source;
        let expected_bytes = file_plan.expected_size_bytes.unwrap_or(0);
        let source_observer_pipeline = self.build_effective_source_observer_pipeline(&snapshot);
        let post_write_pipeline = self.build_effective_post_write_pipeline(&snapshot);
        let (mut copy_branches, mut results) = self
            .resolve_destination_branches(source, rows, options.overwrite_policy)
            .await?;

        if copy_branches.is_empty() {
            self.refresh_progress_from_checkpoints().await?;
            return Ok(FileSessionResult::Completed(results));
        }

        let mut source_artifacts = self
            .load_reusable_source_artifacts(source)
            .await?
            .unwrap_or_default();
        if let Some(source_observer_pipeline) = source_observer_pipeline.as_ref() {
            if source_artifacts.is_empty()
                && source_observer_pipeline.mode == SourcePipelineMode::SerialBeforeFanOut
            {
                debug!(
                    "task '{}' executing source observer pre-pass for '{}'",
                    self.id,
                    source.display()
                );
                source_artifacts = self
                    .run_source_observer_serial(&snapshot, file_plan, source_observer_pipeline)
                    .await?;
                self.persist_source_artifacts(source, &source_artifacts)
                    .await?;
            }
        }

        let mut attempt = 0_u32;
        let max_retries = options.retry_policy.max_retries;
        while !copy_branches.is_empty() {
            let mut ready_branches = Vec::with_capacity(copy_branches.len());
            for branch in &copy_branches {
                if let Some(parent) = branch.actual_destination.parent() {
                    if let Err(err) = self.file_system.create_dir_all(parent).await {
                        self.update_checkpoint(
                            source,
                            &branch.requested_destination,
                            FileCheckpointStatus::Failed,
                            DestinationCopyStatus::Failed,
                            DestinationPostWriteStatus::NotRun,
                            0,
                            branch.expected_bytes,
                            Some(&branch.actual_destination),
                            Some(&err.message),
                        )
                        .await?;
                        results.push(DestinationBranchResult {
                            destination_path: branch.requested_destination.clone(),
                            actual_destination_path: Some(branch.actual_destination.clone()),
                            bytes_written: 0,
                            copy_status: DestinationCopyStatus::Failed,
                            post_write_status: DestinationPostWriteStatus::NotRun,
                            error: Some(err),
                            retries: attempt,
                        });
                        continue;
                    }
                }
                self.update_checkpoint(
                    source,
                    &branch.requested_destination,
                    FileCheckpointStatus::Writing,
                    DestinationCopyStatus::Pending,
                    DestinationPostWriteStatus::Pending,
                    0,
                    branch.expected_bytes,
                    Some(&branch.actual_destination),
                    None,
                )
                .await?;
                ready_branches.push(branch.clone());
            }

            if ready_branches.is_empty() {
                break;
            }
            copy_branches = ready_branches;

            let concurrent_source_observers = if source_artifacts.is_empty() {
                source_observer_pipeline
                    .as_ref()
                    .filter(|pipeline| pipeline.mode == SourcePipelineMode::ConcurrentWithFanOut)
            } else {
                None
            };

            match self
                .fan_out_copy_attempt(
                    &snapshot,
                    file_plan,
                    &copy_branches,
                    concurrent_source_observers,
                )
                .await?
            {
                FanOutAttemptResult::Stopped(outcome) => {
                    for branch in &copy_branches {
                        self.update_checkpoint(
                            source,
                            &branch.requested_destination,
                            FileCheckpointStatus::Pending,
                            DestinationCopyStatus::Pending,
                            DestinationPostWriteStatus::Pending,
                            0,
                            branch.expected_bytes,
                            Some(&branch.actual_destination),
                            None,
                        )
                        .await?;
                    }
                    self.refresh_progress_from_checkpoints().await?;
                    return Ok(FileSessionResult::Stopped(outcome));
                }
                FanOutAttemptResult::Completed {
                    observer_artifacts,
                    branch_results,
                } => {
                    if source_artifacts.is_empty() {
                        source_artifacts = observer_artifacts;
                        self.persist_source_artifacts(source, &source_artifacts)
                            .await?;
                    }
                    let mut failed_retry_branches = Vec::new();

                    for mut result in branch_results {
                        result.retries = attempt;
                        match result.copy_status {
                            DestinationCopyStatus::Succeeded => {
                                results.push(result);
                            }
                            DestinationCopyStatus::Failed => {
                                if attempt < max_retries {
                                    self.update_checkpoint(
                                        source,
                                        &result.destination_path,
                                        FileCheckpointStatus::Pending,
                                        DestinationCopyStatus::Pending,
                                        DestinationPostWriteStatus::Pending,
                                        0,
                                        expected_bytes,
                                        result.actual_destination_path.as_deref(),
                                        result.error.as_ref().map(|err| err.message.as_str()),
                                    )
                                    .await?;
                                    failed_retry_branches.push(DestinationBranchPlan {
                                        requested_destination: result.destination_path.clone(),
                                        actual_destination: result
                                            .actual_destination_path
                                            .clone()
                                            .unwrap_or_else(|| result.destination_path.clone()),
                                        expected_bytes,
                                    });
                                } else {
                                    self.update_checkpoint(
                                        source,
                                        &result.destination_path,
                                        FileCheckpointStatus::Failed,
                                        DestinationCopyStatus::Failed,
                                        DestinationPostWriteStatus::NotRun,
                                        0,
                                        expected_bytes,
                                        result.actual_destination_path.as_deref(),
                                        result.error.as_ref().map(|err| err.message.as_str()),
                                    )
                                    .await?;
                                    results.push(result);
                                }
                            }
                            _ => {}
                        }
                    }

                    if failed_retry_branches.is_empty() {
                        break;
                    }

                    attempt = attempt.saturating_add(1);
                    let factor = options.retry_policy.exponential_factor.max(1) as u64;
                    let backoff = options
                        .retry_policy
                        .initial_backoff_ms
                        .saturating_mul(factor.saturating_pow(attempt.saturating_sub(1)));
                    warn!(
                        "task '{}' retrying {} destination branches for '{}' in {} ms",
                        self.id,
                        failed_retry_branches.len(),
                        source.display(),
                        backoff
                    );
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    copy_branches = failed_retry_branches;
                    continue;
                }
            }
        }

        let mut copied_results = Vec::new();
        let mut final_results = Vec::new();
        for result in results {
            match result.copy_status {
                DestinationCopyStatus::Succeeded
                    if result.post_write_status == DestinationPostWriteStatus::Pending =>
                {
                    copied_results.push(result);
                }
                _ => final_results.push(result),
            }
        }

        let post_write_results = self
            .run_post_write_pipeline(
                source,
                copied_results,
                &source_artifacts,
                post_write_pipeline.as_ref(),
            )
            .await?;
        final_results.extend(post_write_results);

        self.refresh_progress_from_checkpoints().await?;
        Ok(FileSessionResult::Completed(final_results))
    }

    async fn build_report(&self) -> Result<CopyReport, CopyError> {
        let checkpoints = self.load_checkpoints(false).await?;
        let runtime = self
            .runtime
            .read()
            .map_err(|_| lock_poisoned_error("PersistentTask::build_report"))?;

        let mut destinations = Vec::with_capacity(checkpoints.len());
        let mut failures = Vec::new();
        let mut by_source: HashMap<PathBuf, bool> = HashMap::new();

        for row in checkpoints {
            let source_path = PathBuf::from(row.source_path);
            let destination_path = PathBuf::from(row.destination_path);
            let actual_destination_path = row.actual_destination_path.map(PathBuf::from);
            let copy_status = copy_status_from_db_value(&row.copy_status).ok_or(CopyError {
                category: CopyErrorCategory::Unknown,
                code: "INVALID_COPY_STATUS",
                message: format!("unknown copy status '{}'", row.copy_status),
            })?;
            let post_write_status =
                post_write_status_from_db_value(&row.post_write_status).ok_or(CopyError {
                    category: CopyErrorCategory::Unknown,
                    code: "INVALID_POST_WRITE_STATUS",
                    message: format!("unknown post-write status '{}'", row.post_write_status),
                })?;
            let error = row.last_error.map(|message| CopyError {
                category: CopyErrorCategory::Unknown,
                code: "TASK_FAILURE",
                message,
            });

            if let Some(error) = error.clone() {
                failures.push(FileFailure {
                    path: actual_destination_path
                        .clone()
                        .unwrap_or_else(|| destination_path.clone()),
                    error,
                    retries: 0,
                });
            }

            let failed = copy_status == DestinationCopyStatus::Failed
                || post_write_status == DestinationPostWriteStatus::Failed;
            by_source
                .entry(source_path.clone())
                .and_modify(|value| *value |= failed)
                .or_insert(failed);

            destinations.push(DestinationReport {
                source_path,
                destination_path,
                actual_destination_path,
                bytes_written: row.bytes_copied.max(0) as u64,
                copy_status,
                post_write_status,
                error,
            });
        }

        let failed_files = by_source.values().filter(|failed| **failed).count() as u64;
        let total_files = runtime.snapshot.file_plans.len() as u64;
        let succeeded_files = total_files.saturating_sub(failed_files);

        Ok(CopyReport {
            snapshot: runtime.snapshot.clone(),
            total_files,
            succeeded_files,
            failed_files,
            total_bytes: runtime.progress.total_bytes,
            complete_bytes: runtime.progress.complete_bytes,
            duration_ms: 0,
            retry_count: 0,
            failures,
            destinations,
        })
    }

    async fn execute_copy_workload(&self) -> Result<RunOutcome, CopyError> {
        let file_plans = self.ensure_file_plans().await?;
        let rows = self.load_checkpoints(true).await?;
        let mut grouped_rows = BTreeMap::<PathBuf, Vec<DbFileCheckpointRow>>::new();
        for row in rows {
            grouped_rows
                .entry(PathBuf::from(&row.source_path))
                .or_default()
                .push(row);
        }

        let mut destination_results = Vec::new();
        for plan in &file_plans {
            let Some(rows) = grouped_rows.get(&plan.source) else {
                continue;
            };

            match self.current_run_outcome().await? {
                RunOutcome::Continue => {}
                outcome => return Ok(outcome),
            }

            match self.execute_file_session(plan, rows).await? {
                FileSessionResult::Completed(results) => {
                    destination_results.extend(results);
                }
                FileSessionResult::Stopped(outcome) => return Ok(outcome),
            }
        }

        let successful_destinations = destination_results
            .iter()
            .filter(|result| {
                !matches!(result.copy_status, DestinationCopyStatus::Failed)
                    && !matches!(result.post_write_status, DestinationPostWriteStatus::Failed)
            })
            .count();
        let failed_destinations = destination_results
            .iter()
            .filter(|result| {
                matches!(result.copy_status, DestinationCopyStatus::Failed)
                    || matches!(result.post_write_status, DestinationPostWriteStatus::Failed)
            })
            .count();

        let final_state = if failed_destinations == 0 {
            TaskState::Completed
        } else if successful_destinations == 0 {
            TaskState::Failed
        } else {
            TaskState::PartialFailed
        };

        if self.state().await? == TaskState::Running {
            self.transition_state(final_state).await?;
        }

        Ok(RunOutcome::Continue)
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
            TaskState::Completed
            | TaskState::PartialFailed
            | TaskState::Failed
            | TaskState::Cancelled => {
                return Err(invalid_state_error(&self.id, current, TaskState::Running));
            }
        }

        let file_plans = self.ensure_file_plans().await?;
        self.ensure_checkpoints_seeded(&file_plans).await?;
        self.refresh_progress_from_checkpoints().await?;

        match self.execute_copy_workload().await {
            Ok(RunOutcome::Continue) => Ok(()),
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

fn requested_placeholder(err: &tokio::task::JoinError) -> String {
    if err.is_cancelled() {
        "unknown writer branch".to_string()
    } else {
        "writer branch".to_string()
    }
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
        let report = self.build_report().await?;
        let mut runtime = self
            .runtime
            .write()
            .map_err(|_| lock_poisoned_error("PersistentTask::report"))?;
        runtime.report = Some(report.clone());
        Ok(report)
    }
}
