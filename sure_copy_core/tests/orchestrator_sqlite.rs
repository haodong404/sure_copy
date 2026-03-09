mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::Row;
use sure_copy_core::pipeline::{
    SourceObserverPipeline, SourceObserverStage, SourcePipelineMode, StageId, TaskFlowPlan,
};
use sure_copy_core::{
    CopyError, CopyTask, DestinationCopyStatus, DestinationPostWriteStatus, OrchestratorConfig,
    OverwritePolicy, PostWritePipelineMode, SqliteTaskOrchestrator, TaskOptions, TaskOrchestrator,
    TaskState, VerificationPolicy,
};

use common::{init_test_logging, unique_temp_dir, unique_temp_file};

fn write_text(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("parent directory should be creatable");
    }
    std::fs::write(path, text).expect("file should be writable");
}

fn read_text(path: &Path) -> String {
    std::fs::read_to_string(path).expect("file should be readable")
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
async fn sqlite_rejects_runtime_stage_attachments() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_runtime_stage_reject.db");
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
        Ok(_) => panic!("durable runtime stages must be rejected"),
        Err(err) => err,
    };
    assert_eq!(err.code, "UNSUPPORTED_FEATURE");
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
async fn sqlite_run_fan_out_copies_to_multiple_destinations() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_fan_out.db");
    let src_dir = unique_temp_dir("sqlite_fan_out_src");
    let dst_a = unique_temp_dir("sqlite_fan_out_dst_a");
    let dst_b = unique_temp_dir("sqlite_fan_out_dst_b");
    let source_file = src_dir.join("nested").join("hello.txt");
    write_text(&source_file, "stream me once");

    let mut options = TaskOptions::default();
    options.verification_policy = VerificationPolicy::None;

    let orchestrator = new_orchestrator(&db_path).await;
    let handle = orchestrator
        .submit(CopyTask::new(
            "fan-out-task",
            src_dir.clone(),
            vec![dst_a.clone(), dst_b.clone()],
            options,
        ))
        .await
        .expect("task submit should succeed");
    handle.run().await.expect("task should run");

    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert_eq!(
        read_text(&dst_a.join("nested").join("hello.txt")),
        "stream me once"
    );
    assert_eq!(
        read_text(&dst_b.join("nested").join("hello.txt")),
        "stream me once"
    );

    let report = handle.report().await.expect("report should be available");
    assert_eq!(report.destinations.len(), 2);
    assert!(report
        .destinations
        .iter()
        .all(|entry| entry.copy_status == DestinationCopyStatus::Succeeded));
    assert!(report
        .destinations
        .iter()
        .all(|entry| entry.post_write_status == DestinationPostWriteStatus::NotRun));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_run_marks_partial_failed_when_one_destination_branch_errors() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_partial_failed.db");
    let src_dir = unique_temp_dir("sqlite_partial_failed_src");
    let good_dst = unique_temp_dir("sqlite_partial_failed_good_dst");
    let bad_root = unique_temp_file("sqlite_partial_failed_bad_root");
    write_text(&src_dir.join("hello.txt"), "partial failure");
    write_text(&bad_root, "not a directory");

    let mut options = TaskOptions::default();
    options.verification_policy = VerificationPolicy::None;

    let orchestrator = new_orchestrator(&db_path).await;
    let handle = orchestrator
        .submit(CopyTask::new(
            "partial-failed-task",
            src_dir.clone(),
            vec![good_dst.clone(), bad_root.clone()],
            options,
        ))
        .await
        .expect("task submit should succeed");
    handle
        .run()
        .await
        .expect("task should run to a terminal state");

    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::PartialFailed
    );
    assert_eq!(read_text(&good_dst.join("hello.txt")), "partial failure");

    let report = handle.report().await.expect("report should be available");
    assert_eq!(report.destinations.len(), 2);
    assert_eq!(
        report
            .destinations
            .iter()
            .filter(|entry| entry.copy_status == DestinationCopyStatus::Succeeded)
            .count(),
        1
    );
    assert_eq!(
        report
            .destinations
            .iter()
            .filter(|entry| entry.copy_status == DestinationCopyStatus::Failed)
            .count(),
        1
    );
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
        let mut options = TaskOptions::default();
        options.verification_policy = VerificationPolicy::None;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_skip_policy_is_best_effort_per_destination() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_skip_policy.db");
    let src_dir = unique_temp_dir("sqlite_skip_policy_src");
    let dst_a = unique_temp_dir("sqlite_skip_policy_dst_a");
    let dst_b = unique_temp_dir("sqlite_skip_policy_dst_b");
    write_text(&src_dir.join("file.txt"), "skip source");
    write_text(&dst_a.join("file.txt"), "keep me");

    let mut options = TaskOptions::default();
    options.verification_policy = VerificationPolicy::None;
    options.overwrite_policy = OverwritePolicy::Skip;

    let orchestrator = new_orchestrator(&db_path).await;
    let handle = orchestrator
        .submit(CopyTask::new(
            "skip-policy-task",
            src_dir.clone(),
            vec![dst_a.clone(), dst_b.clone()],
            options,
        ))
        .await
        .expect("task submit should succeed");
    handle.run().await.expect("task should run");

    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert_eq!(read_text(&dst_a.join("file.txt")), "keep me");
    assert_eq!(read_text(&dst_b.join("file.txt")), "skip source");

    let report = handle.report().await.expect("report should be available");
    assert_eq!(
        report
            .destinations
            .iter()
            .filter(|entry| entry.copy_status == DestinationCopyStatus::Skipped)
            .count(),
        1
    );
    assert_eq!(
        report
            .destinations
            .iter()
            .filter(|entry| entry.copy_status == DestinationCopyStatus::Succeeded)
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_overwrite_policy_overwrites_each_destination_branch() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_overwrite_policy.db");
    let src_dir = unique_temp_dir("sqlite_overwrite_policy_src");
    let dst_a = unique_temp_dir("sqlite_overwrite_policy_dst_a");
    let dst_b = unique_temp_dir("sqlite_overwrite_policy_dst_b");
    write_text(&src_dir.join("file.txt"), "overwrite source");
    write_text(&dst_a.join("file.txt"), "old a");
    write_text(&dst_b.join("file.txt"), "old b");

    let mut options = TaskOptions::default();
    options.verification_policy = VerificationPolicy::None;
    options.overwrite_policy = OverwritePolicy::Overwrite;

    let orchestrator = new_orchestrator(&db_path).await;
    let handle = orchestrator
        .submit(CopyTask::new(
            "overwrite-policy-task",
            src_dir.clone(),
            vec![dst_a.clone(), dst_b.clone()],
            options,
        ))
        .await
        .expect("task submit should succeed");
    handle.run().await.expect("task should run");

    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert_eq!(read_text(&dst_a.join("file.txt")), "overwrite source");
    assert_eq!(read_text(&dst_b.join("file.txt")), "overwrite source");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_newer_only_policy_decides_per_destination() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_newer_only_policy.db");
    let src_dir = unique_temp_dir("sqlite_newer_only_policy_src");
    let dst_old = unique_temp_dir("sqlite_newer_only_policy_dst_old");
    let dst_new = unique_temp_dir("sqlite_newer_only_policy_dst_new");

    write_text(&dst_old.join("file.txt"), "older target");
    std::thread::sleep(Duration::from_millis(1100));
    write_text(&src_dir.join("file.txt"), "newer source");
    std::thread::sleep(Duration::from_millis(1100));
    write_text(&dst_new.join("file.txt"), "newer target");

    let mut options = TaskOptions::default();
    options.verification_policy = VerificationPolicy::None;
    options.overwrite_policy = OverwritePolicy::NewerOnly;

    let orchestrator = new_orchestrator(&db_path).await;
    let handle = orchestrator
        .submit(CopyTask::new(
            "newer-only-policy-task",
            src_dir.clone(),
            vec![dst_old.clone(), dst_new.clone()],
            options,
        ))
        .await
        .expect("task submit should succeed");
    handle.run().await.expect("task should run");

    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert_eq!(read_text(&dst_old.join("file.txt")), "newer source");
    assert_eq!(read_text(&dst_new.join("file.txt")), "newer target");

    let report = handle.report().await.expect("report should be available");
    assert_eq!(
        report
            .destinations
            .iter()
            .filter(|entry| entry.copy_status == DestinationCopyStatus::Skipped)
            .count(),
        1
    );
    assert_eq!(
        report
            .destinations
            .iter()
            .filter(|entry| entry.copy_status == DestinationCopyStatus::Succeeded)
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_rename_policy_tracks_actual_destination_path_in_report() {
    init_test_logging();
    let db_path = unique_temp_file("sqlite_rename_policy.db");
    let src_dir = unique_temp_dir("sqlite_rename_policy_src");
    let dst_dir = unique_temp_dir("sqlite_rename_policy_dst");
    write_text(&src_dir.join("file.txt"), "rename me");
    write_text(&dst_dir.join("file.txt"), "existing");

    let mut options = TaskOptions::default();
    options.verification_policy = VerificationPolicy::None;
    options.overwrite_policy = OverwritePolicy::Rename;

    let orchestrator = new_orchestrator(&db_path).await;
    let handle = orchestrator
        .submit(CopyTask::new(
            "rename-policy-task",
            src_dir.clone(),
            vec![dst_dir.clone()],
            options,
        ))
        .await
        .expect("task submit should succeed");
    handle.run().await.expect("task should run");

    let report = handle.report().await.expect("report should be available");
    assert_eq!(report.destinations.len(), 1);
    let destination = &report.destinations[0];
    let actual = destination
        .actual_destination_path
        .clone()
        .expect("rename policy should record actual destination");

    assert_eq!(destination.destination_path, dst_dir.join("file.txt"));
    assert_ne!(actual, destination.destination_path);
    assert_eq!(read_text(&actual), "rename me");
}
