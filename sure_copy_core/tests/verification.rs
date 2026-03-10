mod common;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use sqlx::Row;
use sure_copy_core::infrastructure::{ChecksumProvider, Sha256ChecksumProvider, StreamingChecksum};
use sure_copy_core::pipeline::{SourcePipelineMode, StageArtifacts, TaskFlowPlan};
use sure_copy_core::testing::create_sqlite_persistent_task_with_verification_stages;
use sure_copy_core::{CopyError, DestinationPostWriteStatus, TaskOptions, TaskState};

use common::{init_test_logging, unique_temp_dir};

fn write_text(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("parent directory should be creatable");
    }
    std::fs::write(path, text).expect("file should be writable");
}

struct CountingChecksumProvider {
    file_checksums: AtomicUsize,
    stream_starts: AtomicUsize,
    delegate: Sha256ChecksumProvider,
}

impl CountingChecksumProvider {
    fn new() -> Self {
        Self {
            file_checksums: AtomicUsize::new(0),
            stream_starts: AtomicUsize::new(0),
            delegate: Sha256ChecksumProvider::new(),
        }
    }
}

#[async_trait]
impl ChecksumProvider for CountingChecksumProvider {
    fn algorithm(&self) -> &'static str {
        self.delegate.algorithm()
    }

    fn begin_stream(&self) -> Result<Box<dyn StreamingChecksum>, CopyError> {
        self.stream_starts.fetch_add(1, Ordering::Relaxed);
        self.delegate.begin_stream()
    }

    async fn checksum_file(&self, path: &Path) -> Result<String, CopyError> {
        self.file_checksums.fetch_add(1, Ordering::Relaxed);
        self.delegate.checksum_file(path).await
    }
}

struct SelectiveMismatchChecksumProvider {
    mismatch_targets: HashSet<PathBuf>,
    delegate: Sha256ChecksumProvider,
}

impl SelectiveMismatchChecksumProvider {
    fn new(mismatch_targets: impl IntoIterator<Item = PathBuf>) -> Self {
        Self {
            mismatch_targets: mismatch_targets.into_iter().collect(),
            delegate: Sha256ChecksumProvider::new(),
        }
    }
}

#[async_trait]
impl ChecksumProvider for SelectiveMismatchChecksumProvider {
    fn algorithm(&self) -> &'static str {
        self.delegate.algorithm()
    }

    fn begin_stream(&self) -> Result<Box<dyn StreamingChecksum>, CopyError> {
        self.delegate.begin_stream()
    }

    async fn checksum_file(&self, path: &Path) -> Result<String, CopyError> {
        if self.mismatch_targets.contains(path) {
            return Ok("force-mismatch".to_string());
        }
        self.delegate.checksum_file(path).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serial_before_fan_out_runs_verification_through_pipeline_stages() {
    let source_root = unique_temp_dir("verification_serial_src");
    let destination_root = unique_temp_dir("verification_serial_dst");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");
    std::fs::write(source_root.join("file.txt"), "serial mode")
        .expect("source file should be writable");

    let options = TaskOptions::default();

    let checksum_provider = Arc::new(CountingChecksumProvider::new());
    let harness = create_sqlite_persistent_task_with_verification_stages(
        "verification-serial-task",
        &source_root,
        vec![destination_root.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new().with_source_pipeline_mode(SourcePipelineMode::SerialBeforeFanOut),
        checksum_provider.clone(),
    )
    .await;

    harness.run().await.expect("task should run");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert!(checksum_provider.stream_starts.load(Ordering::Relaxed) >= 1);
    assert!(checksum_provider.file_checksums.load(Ordering::Relaxed) >= 1);
    let report = harness.report().await.expect("report should be available");
    assert_eq!(report.destinations.len(), 1);
    assert_eq!(
        report.destinations[0].post_write_status,
        DestinationPostWriteStatus::Succeeded
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_with_fan_out_runs_verification_through_pipeline_stages() {
    let source_root = unique_temp_dir("verification_concurrent_src");
    let destination_root = unique_temp_dir("verification_concurrent_dst");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");
    std::fs::write(source_root.join("file.txt"), "concurrent mode")
        .expect("source file should be writable");

    let options = TaskOptions::default();

    let checksum_provider = Arc::new(CountingChecksumProvider::new());
    let harness = create_sqlite_persistent_task_with_verification_stages(
        "verification-concurrent-task",
        &source_root,
        vec![destination_root.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new().with_source_pipeline_mode(SourcePipelineMode::ConcurrentWithFanOut),
        checksum_provider.clone(),
    )
    .await;

    harness.run().await.expect("task should run");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert!(checksum_provider.stream_starts.load(Ordering::Relaxed) >= 1);
    assert!(checksum_provider.file_checksums.load(Ordering::Relaxed) >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_replays_source_observer_when_no_persisted_artifacts_exist() {
    let source_root = unique_temp_dir("verification_resume_replay_src");
    let destination_root = unique_temp_dir("verification_resume_replay_dst");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");
    std::fs::write(source_root.join("file.txt"), "resume source")
        .expect("source file should be writable");

    let options = TaskOptions::default();

    let checksum_provider = Arc::new(CountingChecksumProvider::new());
    let harness = create_sqlite_persistent_task_with_verification_stages(
        "verification-resume-replay-task",
        &source_root,
        vec![destination_root.clone()],
        TaskState::Paused,
        options,
        TaskFlowPlan::new().with_source_pipeline_mode(SourcePipelineMode::ConcurrentWithFanOut),
        checksum_provider.clone(),
    )
    .await;

    sqlx::query(
        "INSERT INTO task_file_checkpoints (task_id, source_path, destination_path, status, copy_status, post_write_status, bytes_copied, expected_bytes, actual_destination_path, last_error, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("verification-resume-replay-task")
    .bind(source_root.join("file.txt").to_string_lossy().to_string())
    .bind(destination_root.join("file.txt").to_string_lossy().to_string())
    .bind("pending")
    .bind("pending")
    .bind("pending")
    .bind(0_i64)
    .bind(13_i64)
    .bind(Option::<String>::None)
    .bind(Option::<String>::None)
    .bind(0_i64)
    .execute(harness.pool())
    .await
    .expect("checkpoint seed should succeed");

    harness.run().await.expect("task resume should run");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert!(checksum_provider.stream_starts.load(Ordering::Relaxed) >= 1);
    assert!(checksum_provider.file_checksums.load(Ordering::Relaxed) >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_reuses_persisted_source_artifacts_when_fingerprint_matches() {
    let source_root = unique_temp_dir("verification_resume_reuse_src");
    let destination_root = unique_temp_dir("verification_resume_reuse_dst");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");
    let source_path = source_root.join("file.txt");
    std::fs::write(&source_path, "resume source").expect("source file should be writable");

    let options = TaskOptions::default();

    let checksum_provider = Arc::new(CountingChecksumProvider::new());
    let harness = create_sqlite_persistent_task_with_verification_stages(
        "verification-resume-reuse-task",
        &source_root,
        vec![destination_root.clone()],
        TaskState::Paused,
        options,
        TaskFlowPlan::new().with_source_pipeline_mode(SourcePipelineMode::ConcurrentWithFanOut),
        checksum_provider.clone(),
    )
    .await;

    let destination_path = destination_root.join("file.txt");
    sqlx::query(
        "INSERT INTO task_file_checkpoints (task_id, source_path, destination_path, status, copy_status, post_write_status, bytes_copied, expected_bytes, actual_destination_path, last_error, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("verification-resume-reuse-task")
    .bind(source_path.to_string_lossy().to_string())
    .bind(destination_path.to_string_lossy().to_string())
    .bind("pending")
    .bind("pending")
    .bind("pending")
    .bind(0_i64)
    .bind(13_i64)
    .bind(Option::<String>::None)
    .bind(Option::<String>::None)
    .bind(0_i64)
    .execute(harness.pool())
    .await
    .expect("checkpoint seed should succeed");

    let artifacts_json =
        serde_json::to_string(&StageArtifacts::new().with_value("checksum", "wrong-checksum"))
            .expect("artifacts json should encode");
    let fingerprint = serde_json::json!({
        "size_bytes": std::fs::metadata(&source_path).expect("source metadata").len(),
        "modified_unix_ms": std::fs::metadata(&source_path)
            .expect("source metadata")
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis() as i64),
    });
    sqlx::query(
        "INSERT INTO task_source_artifacts (task_id, source_path, stage_key, artifacts_json, source_fingerprint_json, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind("verification-resume-reuse-task")
    .bind(source_path.to_string_lossy().to_string())
    .bind("builtin-source-hash")
    .bind(artifacts_json)
    .bind(fingerprint.to_string())
    .bind(0_i64)
    .execute(harness.pool())
    .await
    .expect("source artifacts should be insertable");

    harness
        .run()
        .await
        .expect("task should run to terminal state");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::Failed
    );
    assert_eq!(checksum_provider.stream_starts.load(Ordering::Relaxed), 0);
    assert!(checksum_provider.file_checksums.load(Ordering::Relaxed) >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_write_verification_failure_only_fails_the_affected_destination_branch() {
    let source_root = unique_temp_dir("verification_branch_src");
    let destination_good = unique_temp_dir("verification_branch_good");
    let destination_bad = unique_temp_dir("verification_branch_bad");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");
    std::fs::write(source_root.join("file.txt"), "branch checksum")
        .expect("source file should be writable");

    let options = TaskOptions::default();

    let bad_path = destination_bad.join("file.txt");
    let checksum_provider: Arc<dyn ChecksumProvider> =
        Arc::new(SelectiveMismatchChecksumProvider::new([bad_path]));
    let harness = create_sqlite_persistent_task_with_verification_stages(
        "verification-branch-task",
        &source_root,
        vec![destination_good.clone(), destination_bad.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new().with_source_pipeline_mode(SourcePipelineMode::ConcurrentWithFanOut),
        checksum_provider,
    )
    .await;

    harness.run().await.expect("task should run");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::PartialFailed
    );
    let report = harness.report().await.expect("report should be available");
    assert_eq!(report.destinations.len(), 2);
    assert_eq!(
        report
            .destinations
            .iter()
            .filter(|entry| entry.post_write_status == DestinationPostWriteStatus::Succeeded)
            .count(),
        1
    );
    assert_eq!(
        report
            .destinations
            .iter()
            .filter(|entry| entry.post_write_status == DestinationPostWriteStatus::Failed)
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_source_hash_and_post_write_verification_completes_in_serial_mode() {
    init_test_logging();
    let src_dir = unique_temp_dir("sqlite_source_hash_and_post_write_src");
    let dst_dir = unique_temp_dir("sqlite_source_hash_and_post_write_dst");
    write_text(&src_dir.join("verify.txt"), "verify me");

    let options = TaskOptions::default();
    let checksum_provider = Arc::new(Sha256ChecksumProvider::new());
    let harness = create_sqlite_persistent_task_with_verification_stages(
        "verify-task",
        &src_dir,
        vec![dst_dir.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new().with_source_pipeline_mode(SourcePipelineMode::SerialBeforeFanOut),
        checksum_provider,
    )
    .await;

    harness.run().await.expect("task should run");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::Completed
    );
    let report = harness.report().await.expect("report should be available");
    assert_eq!(report.destinations.len(), 1);
    assert_eq!(
        report.destinations[0].post_write_status,
        DestinationPostWriteStatus::Succeeded
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_persists_source_artifacts_for_builtin_source_hash_stage() {
    init_test_logging();
    let src_dir = unique_temp_dir("sqlite_source_artifacts_src");
    let dst_dir = unique_temp_dir("sqlite_source_artifacts_dst");
    let source_path = src_dir.join("verify.txt");
    write_text(&source_path, "persist my checksum");

    let options = TaskOptions::default();
    let checksum_provider = Arc::new(Sha256ChecksumProvider::new());
    let harness = create_sqlite_persistent_task_with_verification_stages(
        "source-artifacts-task",
        &src_dir,
        vec![dst_dir.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new().with_source_pipeline_mode(SourcePipelineMode::SerialBeforeFanOut),
        checksum_provider,
    )
    .await;

    harness.run().await.expect("task should run");

    let row = sqlx::query(
        "SELECT stage_key, artifacts_json FROM task_source_artifacts WHERE task_id = ? AND source_path = ?",
    )
    .bind("source-artifacts-task")
    .bind(source_path.to_string_lossy().to_string())
    .fetch_one(harness.pool())
    .await
    .expect("source artifacts should persist");

    assert_eq!(row.get::<String, _>("stage_key"), "builtin-source-hash");
    let artifacts_json = row.get::<String, _>("artifacts_json");
    let artifacts =
        serde_json::from_str::<StageArtifacts>(&artifacts_json).expect("artifacts should decode");
    assert!(artifacts.get_string("checksum").is_some());
}
