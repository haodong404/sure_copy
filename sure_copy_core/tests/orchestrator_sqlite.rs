mod common;

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use sqlx::Row;
use sure_copy_core::pipeline::{
    SourceObserverPipeline, SourceObserverStage, SourcePipelineMode, StageArtifacts, StageId,
    StageRegistry, StageSpec, StageStateSpec, TaskFlowPlan,
};
use sure_copy_core::{
    CopyError, CopyTask, OrchestratorConfig, PostWritePipelineMode, SqliteTaskOrchestrator,
    TaskOptions, TaskOrchestrator, TaskState,
};

use common::{init_test_logging, unique_temp_dir, unique_temp_file};

fn write_text(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("parent directory should be creatable");
    }
    std::fs::write(path, text).expect("file should be writable");
}

async fn new_orchestrator(db_path: &Path) -> SqliteTaskOrchestrator {
    SqliteTaskOrchestrator::new(db_path, OrchestratorConfig::default())
        .await
        .expect("SQLite orchestrator should initialize")
}

#[derive(Default)]
struct DummyObserverStage;

#[async_trait]
impl SourceObserverStage for DummyObserverStage {
    fn id(&self) -> StageId {
        "dummy-observer"
    }

    async fn observe_chunk(
        &self,
        _task: &CopyTask,
        _plan: &sure_copy_core::FilePlan,
        _chunk: &sure_copy_core::SourceChunk,
    ) -> Result<(), CopyError> {
        Ok(())
    }
}

struct CountingObserverStage {
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl SourceObserverStage for CountingObserverStage {
    fn id(&self) -> StageId {
        "counting-observer"
    }

    fn spec(&self) -> Option<StageSpec> {
        Some(StageSpec::new("counting-observer"))
    }

    async fn observe_chunk(
        &self,
        _task: &CopyTask,
        _plan: &sure_copy_core::FilePlan,
        _chunk: &sure_copy_core::SourceChunk,
    ) -> Result<(), CopyError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct TestStageRegistry {
    build_calls: Arc<AtomicUsize>,
    observed_chunks: Arc<AtomicUsize>,
}

impl TestStageRegistry {
    fn new() -> Self {
        Self {
            build_calls: Arc::new(AtomicUsize::new(0)),
            observed_chunks: Arc::new(AtomicUsize::new(0)),
        }
    }
}

struct ResumableObserverStage {
    seen_bytes: Mutex<u64>,
    restored: AtomicUsize,
}

impl ResumableObserverStage {
    fn new() -> Self {
        Self {
            seen_bytes: Mutex::new(0),
            restored: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl SourceObserverStage for ResumableObserverStage {
    fn id(&self) -> StageId {
        "resumable-observer"
    }

    fn spec(&self) -> Option<StageSpec> {
        Some(StageSpec::new("resumable-observer"))
    }

    fn snapshot_state(&self) -> Option<StageStateSpec> {
        let seen_bytes = *self
            .seen_bytes
            .lock()
            .expect("resumable stage lock should not be poisoned");
        Some(StageStateSpec::new("resumable-observer").with_state(json!({
            "seen_bytes": seen_bytes,
        })))
    }

    fn restore_state(&self, state: &StageStateSpec) -> Result<(), CopyError> {
        let seen_bytes = state
            .state
            .get("seen_bytes")
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        *self
            .seen_bytes
            .lock()
            .expect("resumable stage lock should not be poisoned") = seen_bytes;
        self.restored.store(1, Ordering::Relaxed);
        Ok(())
    }

    async fn observe_chunk(
        &self,
        _task: &CopyTask,
        _plan: &sure_copy_core::FilePlan,
        chunk: &sure_copy_core::SourceChunk,
    ) -> Result<(), CopyError> {
        let mut seen = self
            .seen_bytes
            .lock()
            .expect("resumable stage lock should not be poisoned");
        let chunk_end = chunk.offset.saturating_add(chunk.len() as u64);
        if chunk_end <= *seen {
            return Ok(());
        }
        let unprocessed_start = (*seen).max(chunk.offset);
        *seen = (*seen).saturating_add(chunk_end.saturating_sub(unprocessed_start));
        Ok(())
    }

    async fn finish(
        &self,
        _task: &CopyTask,
        _plan: &sure_copy_core::FilePlan,
    ) -> Result<StageArtifacts, CopyError> {
        let seen = *self
            .seen_bytes
            .lock()
            .expect("resumable stage lock should not be poisoned");
        Ok(StageArtifacts::new()
            .with_value("seen_bytes", seen as i64)
            .with_value("restored", self.restored.load(Ordering::Relaxed) > 0))
    }
}

impl StageRegistry for TestStageRegistry {
    fn build_source_observer_stage(
        &self,
        spec: &StageSpec,
    ) -> Result<Option<Arc<dyn SourceObserverStage>>, CopyError> {
        self.build_calls.fetch_add(1, Ordering::Relaxed);
        if spec.kind != "counting-observer" {
            if spec.kind == "resumable-observer" {
                return Ok(Some(Arc::new(ResumableObserverStage::new())));
            }
            return Ok(None);
        }

        Ok(Some(Arc::new(CountingObserverStage {
            count: Arc::clone(&self.observed_chunks),
        })))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_persists_flow_modes_across_restart() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_flow_modes.db");
    let src_dir = unique_temp_dir("sqlite_flow_modes_src");
    let dst_dir = unique_temp_dir("sqlite_flow_modes_dst");
    std::fs::create_dir_all(&src_dir).expect("source dir should be creatable");
    std::fs::create_dir_all(&dst_dir).expect("destination dir should be creatable");

    {
        let orchestrator = new_orchestrator(&db_path).await;
        let task = CopyTask::new(
            "flow-modes-task",
            src_dir.clone(),
            vec![dst_dir.clone()],
            TaskOptions::default(),
        )
        .with_flow(
            TaskFlowPlan::new()
                .with_source_pipeline_mode(SourcePipelineMode::SerialBeforeFanOut)
                .with_post_write_pipeline_mode(PostWritePipelineMode::SerialAfterWrite),
        );

        orchestrator
            .submit(task)
            .await
            .expect("task submit should succeed");
    }

    let orchestrator = new_orchestrator(&db_path).await;
    let loaded = orchestrator
        .get_task("flow-modes-task")
        .await
        .expect("task lookup should work");

    assert_eq!(
        loaded.snapshot().flow.source_pipeline_mode(),
        SourcePipelineMode::SerialBeforeFanOut
    );
    assert_eq!(
        loaded.snapshot().flow.post_write_pipeline_mode(),
        PostWritePipelineMode::SerialAfterWrite
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_rejects_runtime_stages_without_persistable_specs() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_runtime_stage_accept.db");
    let src_dir = unique_temp_dir("sqlite_runtime_stage_src");
    let dst_dir = unique_temp_dir("sqlite_runtime_stage_dst");
    std::fs::create_dir_all(&src_dir).expect("source dir should be creatable");
    std::fs::create_dir_all(&dst_dir).expect("destination dir should be creatable");

    let orchestrator = new_orchestrator(&db_path).await;
    let task = CopyTask::new(
        "runtime-stage-task",
        src_dir,
        vec![dst_dir],
        TaskOptions::default(),
    )
    .with_flow(
        TaskFlowPlan::new().with_source_observer(
            SourceObserverPipeline::new(SourcePipelineMode::ConcurrentWithFanOut)
                .with_stage(Arc::new(DummyObserverStage)),
        ),
    );

    let err = match orchestrator.submit(task).await {
        Ok(_) => panic!("runtime stage attachments without specs should be rejected"),
        Err(err) => err,
    };
    assert_eq!(err.code, "STAGE_SPEC_MISSING");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_rehydrates_bound_runtime_flows_after_restart() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_runtime_stage_rehydrate.db");
    let src_dir = unique_temp_dir("sqlite_runtime_stage_rehydrate_src");
    let dst_dir = unique_temp_dir("sqlite_runtime_stage_rehydrate_dst");
    write_text(&src_dir.join("file.txt"), "rehydrate me");

    {
        let orchestrator = new_orchestrator(&db_path).await;
        let task = CopyTask::new(
            "runtime-stage-rehydrate-task",
            src_dir.clone(),
            vec![dst_dir.clone()],
            TaskOptions::default(),
        )
        .with_flow(TaskFlowPlan::new().with_source_observer(
            SourceObserverPipeline::new(SourcePipelineMode::SerialBeforeFanOut).with_stage(
                Arc::new(CountingObserverStage {
                    count: Arc::new(AtomicUsize::new(0)),
                }),
            ),
        ));

        orchestrator
            .submit(task)
            .await
            .expect("task submit should succeed");
    }

    let registry = Arc::new(TestStageRegistry::new());
    let orchestrator = SqliteTaskOrchestrator::new_with_stage_registry(
        &db_path,
        OrchestratorConfig::default(),
        Some(registry.clone()),
    )
    .await
    .expect("SQLite orchestrator with stage registry should initialize");

    let handle = orchestrator
        .get_task("runtime-stage-rehydrate-task")
        .await
        .expect("task lookup should work");
    handle.run().await.expect("reloaded task should run");

    assert!(registry.build_calls.load(Ordering::Relaxed) >= 1);
    assert!(registry.observed_chunks.load(Ordering::Relaxed) >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_restores_persisted_stage_state_after_restart() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_stage_state_restore.db");
    let src_dir = unique_temp_dir("sqlite_stage_state_restore_src");
    let dst_dir = unique_temp_dir("sqlite_stage_state_restore_dst");
    let source_path = src_dir.join("file.txt");
    write_text(&source_path, "123456789");

    {
        let orchestrator = new_orchestrator(&db_path).await;
        let task = CopyTask::new(
            "stage-state-restore-task",
            src_dir.clone(),
            vec![dst_dir.clone()],
            TaskOptions::default(),
        )
        .with_flow(
            TaskFlowPlan::new().with_source_observer(
                SourceObserverPipeline::new(SourcePipelineMode::SerialBeforeFanOut)
                    .with_stage(Arc::new(ResumableObserverStage::new())),
            ),
        );

        orchestrator
            .submit(task)
            .await
            .expect("task submit should succeed");
    }

    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("test should connect to sqlite database");
    sqlx::query(
        "INSERT OR REPLACE INTO task_stage_states (task_id, source_path, destination_path, stage_key, state_json, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind("stage-state-restore-task")
    .bind(source_path.to_string_lossy().to_string())
    .bind("")
    .bind("resumable-observer")
    .bind(
        serde_json::to_string(&StageStateSpec::new("resumable-observer").with_state(json!({
            "seen_bytes": 5_u64,
        })))
        .expect("stage state json should encode"),
    )
    .bind(0_i64)
    .execute(&pool)
    .await
    .expect("stage state row should be insertable");
    drop(pool);

    let registry = Arc::new(TestStageRegistry::new());
    let orchestrator = SqliteTaskOrchestrator::new_with_stage_registry(
        &db_path,
        OrchestratorConfig::default(),
        Some(registry),
    )
    .await
    .expect("SQLite orchestrator with stage registry should initialize");

    let handle = orchestrator
        .get_task("stage-state-restore-task")
        .await
        .expect("task lookup should work");
    handle.run().await.expect("reloaded task should run");

    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("test should reconnect to sqlite database");
    let row = sqlx::query(
        "SELECT artifacts_json FROM task_source_artifacts WHERE task_id = ? AND source_path = ? AND stage_key = ?",
    )
    .bind("stage-state-restore-task")
    .bind(source_path.to_string_lossy().to_string())
    .bind("resumable-observer")
    .fetch_one(&pool)
    .await
    .expect("source artifacts row should exist");

    let artifacts_json = row
        .try_get::<String, _>("artifacts_json")
        .expect("artifacts json should load");
    let artifacts: StageArtifacts =
        serde_json::from_str(&artifacts_json).expect("artifacts should decode");
    assert_eq!(
        artifacts.get("restored").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        artifacts.get("seen_bytes").and_then(|v| v.as_i64()),
        Some(9)
    );

    let remaining_states = sqlx::query(
        "SELECT COUNT(*) as count FROM task_stage_states WHERE task_id = ? AND source_path = ? AND stage_key = ?",
    )
    .bind("stage-state-restore-task")
    .bind(source_path.to_string_lossy().to_string())
    .bind("resumable-observer")
    .fetch_one(&pool)
    .await
    .expect("stage state rows should load")
    .try_get::<i64, _>("count")
    .expect("count should load");
    assert_eq!(remaining_states, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_rejects_overlapping_source_and_destination() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_overlap_validation.db");
    let src_dir = unique_temp_dir("sqlite_overlap_validation_src");
    std::fs::create_dir_all(src_dir.join("dest")).expect("nested destination should be creatable");

    let orchestrator = new_orchestrator(&db_path).await;
    let err = match orchestrator
        .submit(CopyTask::new(
            "overlap-task",
            src_dir.clone(),
            vec![src_dir.join("dest")],
            TaskOptions::default(),
        ))
        .await
    {
        Ok(_) => panic!("overlapping source and destination should fail validation"),
        Err(err) => err,
    };

    assert_eq!(err.code, "OVERLAPPING_SOURCE_DESTINATION");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_rejects_invalid_glob_patterns() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_glob_validation.db");
    let src_dir = unique_temp_dir("sqlite_glob_validation_src");
    let dst_dir = unique_temp_dir("sqlite_glob_validation_dst");
    std::fs::create_dir_all(&src_dir).expect("source dir should be creatable");
    std::fs::create_dir_all(&dst_dir).expect("destination dir should be creatable");

    let orchestrator = new_orchestrator(&db_path).await;
    let mut options = TaskOptions::default();
    options.include_patterns = vec!["[broken".to_string()];

    let err = match orchestrator
        .submit(CopyTask::new(
            "glob-validation-task",
            src_dir,
            vec![dst_dir],
            options,
        ))
        .await
    {
        Ok(_) => panic!("invalid glob pattern should fail validation"),
        Err(err) => err,
    };

    assert_eq!(err.code, "INVALID_GLOB_PATTERN");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_persists_file_manifest_across_restart() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_file_manifest_reload.db");
    let src_dir = unique_temp_dir("sqlite_file_manifest_reload_src");
    let dst_dir = unique_temp_dir("sqlite_file_manifest_reload_dst");
    write_text(&src_dir.join("kept.txt"), "keep me");

    {
        let orchestrator = new_orchestrator(&db_path).await;
        let handle = orchestrator
            .submit(CopyTask::new(
                "manifest-reload-task",
                src_dir.clone(),
                vec![dst_dir.clone()],
                TaskOptions::default(),
            ))
            .await
            .expect("task submit should succeed");
        handle.run().await.expect("task should run once");
    }

    write_text(&src_dir.join("new-after-run.txt"), "should not backfill");

    let orchestrator = new_orchestrator(&db_path).await;
    let reloaded = orchestrator
        .get_task("manifest-reload-task")
        .await
        .expect("task should reload after restart");

    assert_eq!(reloaded.snapshot().file_plans.len(), 1);
    assert_eq!(
        reloaded.snapshot().file_plans[0]
            .source
            .file_name()
            .and_then(|name| name.to_str()),
        Some("kept.txt")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_recovery_resets_writing_and_post_write_statuses() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_recovery_statuses.db");
    let src_dir = unique_temp_dir("sqlite_recovery_statuses_src");
    let dst_dir = unique_temp_dir("sqlite_recovery_statuses_dst");
    std::fs::create_dir_all(&src_dir).expect("source dir should be creatable");
    std::fs::create_dir_all(&dst_dir).expect("destination dir should be creatable");

    let orchestrator = new_orchestrator(&db_path).await;
    let task = CopyTask::new(
        "recovery-task",
        src_dir.clone(),
        vec![dst_dir.clone()],
        TaskOptions::default(),
    );
    orchestrator
        .submit(task)
        .await
        .expect("task submit should succeed");

    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("test should connect to sqlite database");
    sqlx::query("UPDATE tasks SET state = 'running' WHERE id = 'recovery-task'")
        .execute(&pool)
        .await
        .expect("task state update should succeed");
    sqlx::query(
        "INSERT OR REPLACE INTO task_file_checkpoints (task_id, source_path, destination_path, status, copy_status, post_write_status, bytes_copied, expected_bytes, actual_destination_path, last_error, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("recovery-task")
    .bind(src_dir.join("a.txt").to_string_lossy().to_string())
    .bind(dst_dir.join("a.txt").to_string_lossy().to_string())
    .bind("writing")
    .bind("succeeded")
    .bind("pending")
    .bind(5_i64)
    .bind(10_i64)
    .bind(dst_dir.join("a.txt").to_string_lossy().to_string())
    .bind(Option::<String>::None)
    .bind(0_i64)
    .execute(&pool)
    .await
    .expect("writing checkpoint should be insertable");
    sqlx::query(
        "INSERT OR REPLACE INTO task_file_checkpoints (task_id, source_path, destination_path, status, copy_status, post_write_status, bytes_copied, expected_bytes, actual_destination_path, last_error, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("recovery-task")
    .bind(src_dir.join("b.txt").to_string_lossy().to_string())
    .bind(dst_dir.join("b.txt").to_string_lossy().to_string())
    .bind("post_write_running")
    .bind("succeeded")
    .bind("pending")
    .bind(10_i64)
    .bind(10_i64)
    .bind(dst_dir.join("b.txt").to_string_lossy().to_string())
    .bind(Option::<String>::None)
    .bind(0_i64)
    .execute(&pool)
    .await
    .expect("post-write checkpoint should be insertable");
    drop(pool);
    drop(orchestrator);

    let orchestrator = new_orchestrator(&db_path).await;
    let task = orchestrator
        .get_task("recovery-task")
        .await
        .expect("task should load after recovery");
    assert_eq!(
        task.state().await.expect("state should load"),
        TaskState::Paused
    );

    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("test should reconnect to sqlite database");
    let checkpoints = sqlx::query(
        "SELECT status, copy_status, post_write_status FROM task_file_checkpoints WHERE task_id = ? ORDER BY source_path",
    )
    .bind("recovery-task")
    .fetch_all(&pool)
    .await
    .expect("recovered checkpoints should load");

    assert_eq!(checkpoints.len(), 2);
    for row in checkpoints {
        assert_eq!(row.get::<String, _>("status"), "pending");
        assert_eq!(row.get::<String, _>("copy_status"), "pending");
        assert_eq!(row.get::<String, _>("post_write_status"), "pending");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_partial_failed_state_persists_across_restart() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_partial_failed_reload.db");
    let src_dir = unique_temp_dir("sqlite_partial_failed_reload_src");
    let good_dst = unique_temp_dir("sqlite_partial_failed_reload_good_dst");
    let bad_root = unique_temp_file("sqlite_partial_failed_reload_bad_root");
    write_text(&src_dir.join("hello.txt"), "reload partial failure");
    write_text(&bad_root, "not a directory");

    {
        let orchestrator = new_orchestrator(&db_path).await;
        let options = TaskOptions::default();

        let handle = orchestrator
            .submit(CopyTask::new(
                "partial-failed-reload-task",
                src_dir.clone(),
                vec![good_dst.clone(), bad_root.clone()],
                options,
            ))
            .await
            .expect("task submit should succeed");
        handle.run().await.expect("task should run");
        assert_eq!(
            handle.state().await.expect("state should load"),
            TaskState::PartialFailed
        );
    }

    let orchestrator = new_orchestrator(&db_path).await;
    let reloaded = orchestrator
        .get_task("partial-failed-reload-task")
        .await
        .expect("task should reload after restart");
    assert_eq!(
        reloaded.state().await.expect("reloaded state should load"),
        TaskState::PartialFailed
    );
}
