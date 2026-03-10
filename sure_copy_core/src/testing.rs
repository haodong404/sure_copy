#![doc(hidden)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sqlx::SqlitePool;

use crate::domain::{CopyError, CopyReport, CopyTask, TaskOptions, TaskProgress, TaskState};
use crate::infrastructure::{ChecksumProvider, LocalFileSystem};
use crate::orchestrator::artifact_store::ArtifactStore;
use crate::orchestrator::orchestrator_sqlite::{
    TASKS_SCHEMA_SQL, TASK_FILE_CHECKPOINTS_SCHEMA_SQL,
};
use crate::orchestrator::persistent_task::PersistentTask;
use crate::orchestrator::sqlite_artifact_store::{
    SqliteArtifactStore, TASK_DESTINATION_ARTIFACTS_SCHEMA_SQL, TASK_SOURCE_ARTIFACTS_SCHEMA_SQL,
    TASK_STAGE_STATES_SCHEMA_SQL,
};
use crate::pipeline::{
    DestinationChecksumVerifyStage, PostWritePipeline, PostWritePipelineMode, SourceHashStage,
    SourceObserverPipeline, SourcePipelineMode, TaskFlowPlan,
};
use crate::{Task, TaskStream};

pub struct PersistentTaskHarness {
    task: PersistentTask,
    pool: SqlitePool,
}

impl PersistentTaskHarness {
    pub async fn run(&self) -> Result<(), CopyError> {
        self.task.run().await
    }

    pub async fn state(&self) -> Result<TaskState, CopyError> {
        self.task.state().await
    }

    pub async fn report(&self) -> Result<CopyReport, CopyError> {
        self.task.report().await
    }

    pub fn snapshot(&self) -> CopyTask {
        self.task.snapshot()
    }

    pub fn subscribe(&self) -> Result<TaskStream, CopyError> {
        self.task.subscribe()
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

pub async fn create_sqlite_persistent_task(
    task_id: &str,
    source_root: &Path,
    destination_roots: Vec<PathBuf>,
    state: TaskState,
    options: TaskOptions,
    flow: TaskFlowPlan,
    checksum_provider: Arc<dyn ChecksumProvider>,
) -> PersistentTaskHarness {
    create_sqlite_persistent_task_with_flow(
        task_id,
        source_root,
        destination_roots,
        state,
        options,
        flow,
        checksum_provider,
    )
    .await
}

async fn create_sqlite_persistent_task_with_flow(
    task_id: &str,
    source_root: &Path,
    destination_roots: Vec<PathBuf>,
    state: TaskState,
    options: TaskOptions,
    flow: TaskFlowPlan,
    _checksum_provider: Arc<dyn ChecksumProvider>,
) -> PersistentTaskHarness {
    let pool = SqlitePool::connect("sqlite::memory:")
        .await
        .expect("sqlite in-memory database should connect");

    sqlx::query(TASKS_SCHEMA_SQL)
        .execute(&pool)
        .await
        .expect("tasks table should be created");

    sqlx::query(TASK_FILE_CHECKPOINTS_SCHEMA_SQL)
        .execute(&pool)
        .await
        .expect("checkpoints table should be created");

    sqlx::query(TASK_SOURCE_ARTIFACTS_SCHEMA_SQL)
        .execute(&pool)
        .await
        .expect("source artifacts table should be created");

    sqlx::query(TASK_DESTINATION_ARTIFACTS_SCHEMA_SQL)
        .execute(&pool)
        .await
        .expect("destination artifacts table should be created");

    sqlx::query(TASK_STAGE_STATES_SCHEMA_SQL)
        .execute(&pool)
        .await
        .expect("stage states table should be created");

    sqlx::query(
        "INSERT INTO tasks (id, source_root, destinations_json, spec_json, file_plans_json, state, total_bytes, complete_bytes, retry_count, started_at_ms, finished_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(task_id)
    .bind(source_root.to_string_lossy().to_string())
    .bind(
        serde_json::to_string(
            &destination_roots
                .iter()
                .map(|path| path.to_string_lossy().to_string())
                .collect::<Vec<_>>(),
        )
        .expect("destinations json should encode"),
    )
    .bind("{}")
    .bind("[]")
    .bind(match state {
        TaskState::Created => "created",
        TaskState::Planned => "planned",
        TaskState::Preparing => "preparing",
        TaskState::Running => "running",
        TaskState::Completed => "completed",
        TaskState::PartialFailed => "partial_failed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
        TaskState::Paused => "paused",
    })
    .bind(0_i64)
    .bind(0_i64)
    .bind(0_i64)
    .bind(Option::<i64>::None)
    .bind(Option::<i64>::None)
    .execute(&pool)
    .await
    .expect("task row should be inserted");

    let task = CopyTask::new(
        task_id,
        source_root.to_path_buf(),
        destination_roots,
        options,
    )
    .with_state(state)
    .with_flow(flow);

    let artifact_store: Arc<dyn ArtifactStore> = Arc::new(SqliteArtifactStore::new(pool.clone()));

    PersistentTaskHarness {
        task: PersistentTask::new(
            task,
            TaskProgress::default(),
            8,
            pool.clone(),
            Arc::new(LocalFileSystem::new()),
            _checksum_provider,
            artifact_store,
            None,
        ),
        pool,
    }
}

pub fn checksum_verification_flow(
    checksum_provider: Arc<dyn ChecksumProvider>,
    source_mode: SourcePipelineMode,
) -> TaskFlowPlan {
    TaskFlowPlan::new()
        .with_source_observer(
            SourceObserverPipeline::new(source_mode).with_stage(Arc::new(SourceHashStage::new(
                Arc::clone(&checksum_provider),
            ))),
        )
        .with_post_write(
            PostWritePipeline::new(PostWritePipelineMode::SerialAfterWrite).with_stage(Arc::new(
                DestinationChecksumVerifyStage::new(checksum_provider),
            )),
        )
}

pub async fn create_sqlite_persistent_task_with_verification_stages(
    task_id: &str,
    source_root: &Path,
    destination_roots: Vec<PathBuf>,
    state: TaskState,
    options: TaskOptions,
    flow: TaskFlowPlan,
    checksum_provider: Arc<dyn ChecksumProvider>,
) -> PersistentTaskHarness {
    let source_mode = flow.source_pipeline_mode();
    let flow = flow
        .with_source_observer(
            SourceObserverPipeline::new(source_mode).with_stage(Arc::new(SourceHashStage::new(
                Arc::clone(&checksum_provider),
            ))),
        )
        .with_post_write(
            PostWritePipeline::new(PostWritePipelineMode::SerialAfterWrite).with_stage(Arc::new(
                DestinationChecksumVerifyStage::new(Arc::clone(&checksum_provider)),
            )),
        );

    create_sqlite_persistent_task_with_flow(
        task_id,
        source_root,
        destination_roots,
        state,
        options,
        flow,
        checksum_provider,
    )
    .await
}
