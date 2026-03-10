mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use filetime::{set_file_times, FileTime};
use sqlx::Row;
use sure_copy_core::infrastructure::Sha256ChecksumProvider;
use sure_copy_core::pipeline::{
    PostWriteContext, PostWritePipeline, PostWritePipelineMode, PostWriteStage, SourceChunk,
    SourceObserverPipeline, SourceObserverStage, SourcePipelineMode, StageArtifacts, StageId,
    TaskFlowPlan,
};
use sure_copy_core::testing::{
    create_sqlite_persistent_task, create_sqlite_persistent_task_with_verification_stages,
};
use sure_copy_core::{
    CopyError, CopyTask, DestinationCopyStatus, DestinationPostWriteStatus, FilePlan,
    OverwritePolicy, TaskOptions, TaskState, TaskUpdate, TransferPhase,
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

struct ArtifactEmittingPostWriteStage;

#[async_trait]
impl PostWriteStage for ArtifactEmittingPostWriteStage {
    fn id(&self) -> sure_copy_core::StageId {
        "artifact-post-write"
    }

    async fn execute(&self, _ctx: &PostWriteContext) -> Result<StageArtifacts, CopyError> {
        Ok(StageArtifacts::new().with_value("outcome", "verified"))
    }
}

struct DisconnectDestinationAfterFirstPostWriteStage {
    destination_root: std::path::PathBuf,
    disconnected: Mutex<bool>,
}

impl DisconnectDestinationAfterFirstPostWriteStage {
    fn new(destination_root: std::path::PathBuf) -> Self {
        Self {
            destination_root,
            disconnected: Mutex::new(false),
        }
    }
}

#[async_trait]
impl PostWriteStage for DisconnectDestinationAfterFirstPostWriteStage {
    fn id(&self) -> StageId {
        "disconnect-destination-after-first-post-write"
    }

    async fn execute(&self, _ctx: &PostWriteContext) -> Result<StageArtifacts, CopyError> {
        let mut disconnected = self
            .disconnected
            .lock()
            .expect("disconnect stage lock should not be poisoned");

        if !*disconnected {
            if self.destination_root.exists() {
                std::fs::remove_dir_all(&self.destination_root).map_err(|err| CopyError {
                    category: sure_copy_core::CopyErrorCategory::Io,
                    code: "SIMULATED_DISCONNECT_REMOVE_DESTINATION_ROOT_ERROR",
                    message: format!(
                        "failed to simulate destination disconnect by removing '{}': {}",
                        self.destination_root.display(),
                        err
                    ),
                })?;
            }

            std::fs::write(&self.destination_root, "destination disk disconnected").map_err(
                |err| CopyError {
                    category: sure_copy_core::CopyErrorCategory::Io,
                    code: "SIMULATED_DISCONNECT_REPLACE_DESTINATION_ROOT_ERROR",
                    message: format!(
                        "failed to simulate destination disconnect by replacing '{}' with file: {}",
                        self.destination_root.display(),
                        err
                    ),
                },
            )?;

            *disconnected = true;
        }

        Ok(StageArtifacts::new())
    }
}

#[derive(Default)]
struct ArtifactEmittingSourceObserverStage {
    seen_bytes: Mutex<u64>,
}

#[async_trait]
impl SourceObserverStage for ArtifactEmittingSourceObserverStage {
    fn id(&self) -> StageId {
        "artifact-source-observer"
    }

    async fn observe_chunk(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
        chunk: &SourceChunk,
    ) -> Result<(), CopyError> {
        let mut seen = self
            .seen_bytes
            .lock()
            .expect("source observer lock should not be poisoned");
        *seen = seen.saturating_add(chunk.len() as u64);
        Ok(())
    }

    async fn finish(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
    ) -> Result<StageArtifacts, CopyError> {
        let seen = *self
            .seen_bytes
            .lock()
            .expect("source observer lock should not be poisoned");
        Ok(StageArtifacts::new().with_value("seen_bytes", seen as i64))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_write_stage_artifacts_are_persisted_per_destination() {
    let source_root = unique_temp_dir("persistent_task_post_write_artifacts_src");
    let destination_root = unique_temp_dir("persistent_task_post_write_artifacts_dst");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");
    std::fs::write(source_root.join("file.txt"), "artifact payload")
        .expect("source file should be writable");

    let options = TaskOptions::default();

    let harness = create_sqlite_persistent_task(
        "post-write-artifacts-task",
        &source_root,
        vec![destination_root.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new().with_post_write(
            PostWritePipeline::new(PostWritePipelineMode::SerialAfterWrite)
                .with_stage(Arc::new(ArtifactEmittingPostWriteStage)),
        ),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    let row = sqlx::query(
        "SELECT artifacts_json FROM task_destination_artifacts WHERE task_id = ? AND source_path = ? AND destination_path = ? AND stage_key = ?",
    )
    .bind("post-write-artifacts-task")
    .bind(source_root.join("file.txt").to_string_lossy().to_string())
    .bind(destination_root.join("file.txt").to_string_lossy().to_string())
    .bind("artifact-post-write")
    .fetch_one(harness.pool())
    .await
    .expect("destination artifacts should persist");

    let artifacts_json = row
        .try_get::<String, _>("artifacts_json")
        .expect("artifacts_json should decode");
    let artifacts =
        serde_json::from_str::<StageArtifacts>(&artifacts_json).expect("artifacts should decode");
    assert_eq!(artifacts.get_string("outcome"), Some("verified"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_observer_stage_artifacts_are_persisted_for_source_file() {
    let source_root = unique_temp_dir("persistent_task_source_observer_artifacts_src");
    let destination_root = unique_temp_dir("persistent_task_source_observer_artifacts_dst");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");
    let source_path = source_root.join("file.txt");
    let payload = "observer payload";
    std::fs::write(&source_path, payload).expect("source file should be writable");

    let options = TaskOptions::default();

    let harness = create_sqlite_persistent_task(
        "source-observer-artifacts-task",
        &source_root,
        vec![destination_root.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new().with_source_observer(
            SourceObserverPipeline::new(SourcePipelineMode::ConcurrentWithFanOut)
                .with_stage(Arc::new(ArtifactEmittingSourceObserverStage::default())),
        ),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    let destination_path = destination_root.join("file.txt");
    let copied = std::fs::read_to_string(&destination_path)
        .expect("destination file should be readable after run");
    assert_eq!(copied, payload);

    let row = sqlx::query(
        "SELECT artifacts_json FROM task_source_artifacts WHERE task_id = ? AND source_path = ? AND stage_key = ?",
    )
    .bind("source-observer-artifacts-task")
    .bind(source_path.to_string_lossy().to_string())
    .bind("artifact-source-observer")
    .fetch_one(harness.pool())
    .await
    .expect("source artifacts should persist");

    let artifacts_json = row
        .try_get::<String, _>("artifacts_json")
        .expect("artifacts_json should decode");
    let artifacts =
        serde_json::from_str::<StageArtifacts>(&artifacts_json).expect("artifacts should decode");
    assert_eq!(
        artifacts.get("seen_bytes").and_then(|value| value.as_i64()),
        Some(payload.len() as i64)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_copy_copies_all_source_files_to_multiple_destinations() {
    let source_root = unique_temp_dir("persistent_task_basic_copy_src");
    let destination_a = unique_temp_dir("persistent_task_basic_copy_dst_a");
    let destination_b = unique_temp_dir("persistent_task_basic_copy_dst_b");

    std::fs::create_dir_all(source_root.join("nested/deeper"))
        .expect("nested source directories should be creatable");
    let files = [
        ("root.txt", "root payload"),
        ("nested/child.txt", "child payload"),
        ("nested/deeper/leaf.txt", "leaf payload"),
    ];
    for (relative, content) in files {
        std::fs::write(source_root.join(relative), content)
            .expect("source test file should be writable");
    }

    let options = TaskOptions::default();

    let harness = create_sqlite_persistent_task(
        "basic-copy-multi-destination-task",
        &source_root,
        vec![destination_a.clone(), destination_b.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    assert_eq!(
        harness.state().await.expect("state should be readable"),
        TaskState::Completed
    );

    for destination in [&destination_a, &destination_b] {
        for (relative, content) in files {
            let actual = std::fs::read_to_string(destination.join(relative))
                .expect("destination file should be readable");
            assert_eq!(actual, content, "copied content should match source");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_task_filters_sources_with_include_and_exclude_patterns() {
    let source_root = unique_temp_dir("persistent_task_pattern_filter_src");
    let destination_root = unique_temp_dir("persistent_task_pattern_filter_dst");

    write_text(&source_root.join("keep.txt"), "keep");
    write_text(&source_root.join("skip.log"), "skip by include");
    write_text(&source_root.join("ignored/nested.txt"), "skip by exclude");

    let mut options = TaskOptions::default();
    options.include_patterns = vec!["**/*.txt".to_string()];
    options.exclude_patterns = vec!["ignored/**".to_string()];

    let harness = create_sqlite_persistent_task(
        "pattern-filter-task",
        &source_root,
        vec![destination_root.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    assert_eq!(read_text(&destination_root.join("keep.txt")), "keep");
    assert!(
        !destination_root.join("skip.log").exists(),
        "non-matching include pattern should skip file"
    );
    assert!(
        !destination_root.join("ignored/nested.txt").exists(),
        "exclude pattern should skip nested file"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copied_file_metadata_matches_source_for_multiple_destinations() {
    let source_root = unique_temp_dir("persistent_task_metadata_copy_src");
    let destination_a = unique_temp_dir("persistent_task_metadata_copy_dst_a");
    let destination_b = unique_temp_dir("persistent_task_metadata_copy_dst_b");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");

    let source_path = source_root.join("meta.txt");
    std::fs::write(&source_path, "metadata payload").expect("source file should be writable");

    let mut source_permissions = std::fs::metadata(&source_path)
        .expect("source metadata should be readable")
        .permissions();
    source_permissions.set_readonly(true);
    std::fs::set_permissions(&source_path, source_permissions)
        .expect("source permissions should be settable");

    let fixed_time = FileTime::from_unix_time(1_700_000_000, 123_000_000);
    set_file_times(&source_path, fixed_time, fixed_time)
        .expect("source timestamps should be settable");

    let mut options = TaskOptions::default();
    options.preserve_permissions = true;
    options.preserve_timestamps = true;

    let harness = create_sqlite_persistent_task(
        "metadata-copy-task",
        &source_root,
        vec![destination_a.clone(), destination_b.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    let source_metadata = std::fs::metadata(&source_path).expect("source metadata should exist");
    let source_modified = source_metadata
        .modified()
        .expect("source modified time should be readable")
        .duration_since(UNIX_EPOCH)
        .expect("source modified time should be after unix epoch")
        .as_secs();

    for destination in [&destination_a, &destination_b] {
        let destination_path = destination.join("meta.txt");
        let destination_metadata =
            std::fs::metadata(&destination_path).expect("destination metadata should exist");

        assert_eq!(
            destination_metadata.permissions().readonly(),
            source_metadata.permissions().readonly(),
            "destination readonly flag should match source"
        );

        let destination_modified = destination_metadata
            .modified()
            .expect("destination modified time should be readable")
            .duration_since(UNIX_EPOCH)
            .expect("destination modified time should be after unix epoch")
            .as_secs();
        assert_eq!(
            destination_modified, source_modified,
            "destination modified timestamp should match source (seconds precision)"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copied_file_metadata_preserves_permissions_without_forcing_timestamps() {
    let source_root = unique_temp_dir("persistent_task_metadata_permissions_only_src");
    let destination_a = unique_temp_dir("persistent_task_metadata_permissions_only_dst_a");
    let destination_b = unique_temp_dir("persistent_task_metadata_permissions_only_dst_b");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");

    let source_path = source_root.join("meta.txt");
    std::fs::write(&source_path, "metadata payload").expect("source file should be writable");

    let mut source_permissions = std::fs::metadata(&source_path)
        .expect("source metadata should be readable")
        .permissions();
    source_permissions.set_readonly(true);
    std::fs::set_permissions(&source_path, source_permissions)
        .expect("source permissions should be settable");

    let fixed_time = FileTime::from_unix_time(1_500_000_000, 0);
    set_file_times(&source_path, fixed_time, fixed_time)
        .expect("source timestamps should be settable");

    let mut options = TaskOptions::default();
    options.preserve_permissions = true;
    options.preserve_timestamps = false;

    let harness = create_sqlite_persistent_task(
        "metadata-permissions-only-task",
        &source_root,
        vec![destination_a.clone(), destination_b.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    let source_metadata = std::fs::metadata(&source_path).expect("source metadata should exist");
    let source_modified_secs = source_metadata
        .modified()
        .expect("source modified time should be readable")
        .duration_since(UNIX_EPOCH)
        .expect("source modified time should be after unix epoch")
        .as_secs();
    let source_readonly = source_metadata.permissions().readonly();

    for destination in [&destination_a, &destination_b] {
        let destination_path = destination.join("meta.txt");
        let destination_metadata =
            std::fs::metadata(&destination_path).expect("destination metadata should exist");

        assert_eq!(
            destination_metadata.permissions().readonly(),
            source_readonly,
            "destination readonly flag should match source when preserving permissions"
        );

        let destination_modified_secs = destination_metadata
            .modified()
            .expect("destination modified time should be readable")
            .duration_since(UNIX_EPOCH)
            .expect("destination modified time should be after unix epoch")
            .as_secs();

        assert_ne!(
            destination_modified_secs, source_modified_secs,
            "destination timestamp should not be forced to source when timestamp preservation is disabled"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copied_file_metadata_preserves_timestamps_without_forcing_permissions() {
    let source_root = unique_temp_dir("persistent_task_metadata_timestamps_only_src");
    let destination_a = unique_temp_dir("persistent_task_metadata_timestamps_only_dst_a");
    let destination_b = unique_temp_dir("persistent_task_metadata_timestamps_only_dst_b");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");

    let source_path = source_root.join("meta.txt");
    std::fs::write(&source_path, "metadata payload").expect("source file should be writable");

    let mut source_permissions = std::fs::metadata(&source_path)
        .expect("source metadata should be readable")
        .permissions();
    source_permissions.set_readonly(true);
    std::fs::set_permissions(&source_path, source_permissions)
        .expect("source permissions should be settable");

    let fixed_time = FileTime::from_unix_time(1_500_000_000, 0);
    set_file_times(&source_path, fixed_time, fixed_time)
        .expect("source timestamps should be settable");

    let mut options = TaskOptions::default();
    options.preserve_permissions = false;
    options.preserve_timestamps = true;

    let harness = create_sqlite_persistent_task(
        "metadata-timestamps-only-task",
        &source_root,
        vec![destination_a.clone(), destination_b.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    let source_metadata = std::fs::metadata(&source_path).expect("source metadata should exist");
    let source_modified_secs = source_metadata
        .modified()
        .expect("source modified time should be readable")
        .duration_since(UNIX_EPOCH)
        .expect("source modified time should be after unix epoch")
        .as_secs();
    let source_readonly = source_metadata.permissions().readonly();

    for destination in [&destination_a, &destination_b] {
        let destination_path = destination.join("meta.txt");
        let destination_metadata =
            std::fs::metadata(&destination_path).expect("destination metadata should exist");

        let destination_modified_secs = destination_metadata
            .modified()
            .expect("destination modified time should be readable")
            .duration_since(UNIX_EPOCH)
            .expect("destination modified time should be after unix epoch")
            .as_secs();

        assert_eq!(
            destination_modified_secs, source_modified_secs,
            "destination timestamp should match source when preserving timestamps"
        );

        assert_ne!(
            destination_metadata.permissions().readonly(),
            source_readonly,
            "destination readonly flag should not be forced to source when permission preservation is disabled"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copied_file_metadata_is_not_forced_when_preserve_options_are_disabled() {
    let source_root = unique_temp_dir("persistent_task_metadata_no_preserve_src");
    let destination_a = unique_temp_dir("persistent_task_metadata_no_preserve_dst_a");
    let destination_b = unique_temp_dir("persistent_task_metadata_no_preserve_dst_b");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");

    let source_path = source_root.join("meta.txt");
    std::fs::write(&source_path, "metadata payload").expect("source file should be writable");

    let mut source_permissions = std::fs::metadata(&source_path)
        .expect("source metadata should be readable")
        .permissions();
    source_permissions.set_readonly(true);
    std::fs::set_permissions(&source_path, source_permissions)
        .expect("source permissions should be settable");

    let fixed_time = FileTime::from_unix_time(1_500_000_000, 0);
    set_file_times(&source_path, fixed_time, fixed_time)
        .expect("source timestamps should be settable");

    let mut options = TaskOptions::default();
    options.preserve_permissions = false;
    options.preserve_timestamps = false;

    let harness = create_sqlite_persistent_task(
        "metadata-no-preserve-task",
        &source_root,
        vec![destination_a.clone(), destination_b.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    let source_metadata = std::fs::metadata(&source_path).expect("source metadata should exist");
    let source_modified_secs = source_metadata
        .modified()
        .expect("source modified time should be readable")
        .duration_since(UNIX_EPOCH)
        .expect("source modified time should be after unix epoch")
        .as_secs();
    let source_readonly = source_metadata.permissions().readonly();

    for destination in [&destination_a, &destination_b] {
        let destination_path = destination.join("meta.txt");
        let destination_metadata =
            std::fs::metadata(&destination_path).expect("destination metadata should exist");

        let destination_modified_secs = destination_metadata
            .modified()
            .expect("destination modified time should be readable")
            .duration_since(UNIX_EPOCH)
            .expect("destination modified time should be after unix epoch")
            .as_secs();
        let destination_readonly = destination_metadata.permissions().readonly();

        assert!(
            destination_modified_secs != source_modified_secs
                || destination_readonly != source_readonly,
            "when preserve options are disabled, destination metadata should not be forced to match source"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_task_run_fan_out_copies_to_multiple_destinations() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_fan_out_src");
    let dst_a = unique_temp_dir("persistent_task_fan_out_dst_a");
    let dst_b = unique_temp_dir("persistent_task_fan_out_dst_b");
    let source_file = src_dir.join("nested").join("hello.txt");
    write_text(&source_file, "stream me once");

    let options = TaskOptions::default();

    let harness = create_sqlite_persistent_task(
        "fan-out-task",
        &src_dir,
        vec![dst_a.clone(), dst_b.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    assert_eq!(
        harness.state().await.expect("state should load"),
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

    let report = harness.report().await.expect("report should be available");
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
async fn persistent_task_run_marks_partial_failed_when_one_destination_branch_errors() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_partial_failed_src");
    let good_dst = unique_temp_dir("persistent_task_partial_failed_good_dst");
    let bad_root = unique_temp_file("persistent_task_partial_failed_bad_root");
    write_text(&src_dir.join("hello.txt"), "partial failure");
    write_text(&bad_root, "not a directory");

    let options = TaskOptions::default();

    let harness = create_sqlite_persistent_task(
        "partial-failed-task",
        &src_dir,
        vec![good_dst.clone(), bad_root.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness
        .run()
        .await
        .expect("task should run to a terminal state");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::PartialFailed
    );
    assert_eq!(read_text(&good_dst.join("hello.txt")), "partial failure");

    let report = harness.report().await.expect("report should be available");
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
    assert_eq!(report.retry_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_task_marks_partial_failed_when_destination_disconnects_during_run() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_disconnect_src");
    let stable_dst = unique_temp_dir("persistent_task_disconnect_stable_dst");
    let unstable_dst = unique_temp_dir("persistent_task_disconnect_unstable_dst");

    write_text(&src_dir.join("a-first.txt"), "first payload");
    write_text(&src_dir.join("nested/b-second.txt"), "second payload");

    let options = TaskOptions::default();

    let harness = create_sqlite_persistent_task(
        "destination-disconnect-task",
        &src_dir,
        vec![stable_dst.clone(), unstable_dst.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new().with_post_write(
            PostWritePipeline::new(PostWritePipelineMode::SerialAfterWrite).with_stage(Arc::new(
                DisconnectDestinationAfterFirstPostWriteStage::new(unstable_dst.clone()),
            )),
        ),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness
        .run()
        .await
        .expect("task should finish with a terminal state");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::PartialFailed
    );

    assert_eq!(read_text(&stable_dst.join("a-first.txt")), "first payload");
    assert_eq!(
        read_text(&stable_dst.join("nested/b-second.txt")),
        "second payload"
    );

    let report = harness.report().await.expect("report should be available");
    assert!(report.destinations.iter().any(|entry| {
        entry.destination_path.starts_with(&unstable_dst)
            && entry.copy_status == DestinationCopyStatus::Failed
    }));
    assert!(report.destinations.iter().any(|entry| {
        entry.destination_path.starts_with(&stable_dst)
            && entry.copy_status == DestinationCopyStatus::Succeeded
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_task_skip_policy_is_best_effort_per_destination() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_skip_policy_src");
    let dst_a = unique_temp_dir("persistent_task_skip_policy_dst_a");
    let dst_b = unique_temp_dir("persistent_task_skip_policy_dst_b");
    write_text(&src_dir.join("file.txt"), "skip source");
    write_text(&dst_a.join("file.txt"), "keep me");

    let mut options = TaskOptions::default();
    options.overwrite_policy = OverwritePolicy::Skip;

    let harness = create_sqlite_persistent_task(
        "skip-policy-task",
        &src_dir,
        vec![dst_a.clone(), dst_b.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert_eq!(read_text(&dst_a.join("file.txt")), "keep me");
    assert_eq!(read_text(&dst_b.join("file.txt")), "skip source");

    let report = harness.report().await.expect("report should be available");
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
async fn persistent_task_overwrite_policy_overwrites_each_destination_branch() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_overwrite_policy_src");
    let dst_a = unique_temp_dir("persistent_task_overwrite_policy_dst_a");
    let dst_b = unique_temp_dir("persistent_task_overwrite_policy_dst_b");
    write_text(&src_dir.join("file.txt"), "overwrite source");
    write_text(&dst_a.join("file.txt"), "old a");
    write_text(&dst_b.join("file.txt"), "old b");

    let mut options = TaskOptions::default();
    options.overwrite_policy = OverwritePolicy::Overwrite;

    let harness = create_sqlite_persistent_task(
        "overwrite-policy-task",
        &src_dir,
        vec![dst_a.clone(), dst_b.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert_eq!(read_text(&dst_a.join("file.txt")), "overwrite source");
    assert_eq!(read_text(&dst_b.join("file.txt")), "overwrite source");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_task_newer_only_policy_decides_per_destination() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_newer_only_policy_src");
    let dst_old = unique_temp_dir("persistent_task_newer_only_policy_dst_old");
    let dst_new = unique_temp_dir("persistent_task_newer_only_policy_dst_new");

    write_text(&dst_old.join("file.txt"), "older target");
    std::thread::sleep(Duration::from_millis(1100));
    write_text(&src_dir.join("file.txt"), "newer source");
    std::thread::sleep(Duration::from_millis(1100));
    write_text(&dst_new.join("file.txt"), "newer target");

    let mut options = TaskOptions::default();
    options.overwrite_policy = OverwritePolicy::NewerOnly;

    let harness = create_sqlite_persistent_task(
        "newer-only-policy-task",
        &src_dir,
        vec![dst_old.clone(), dst_new.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    assert_eq!(
        harness.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert_eq!(read_text(&dst_old.join("file.txt")), "newer source");
    assert_eq!(read_text(&dst_new.join("file.txt")), "newer target");

    let report = harness.report().await.expect("report should be available");
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
async fn persistent_task_rename_policy_tracks_actual_destination_path_in_report() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_rename_policy_src");
    let dst_dir = unique_temp_dir("persistent_task_rename_policy_dst");
    write_text(&src_dir.join("file.txt"), "rename me");
    write_text(&dst_dir.join("file.txt"), "existing");

    let mut options = TaskOptions::default();
    options.overwrite_policy = OverwritePolicy::Rename;

    let harness = create_sqlite_persistent_task(
        "rename-policy-task",
        &src_dir,
        vec![dst_dir.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    let report = harness.report().await.expect("report should be available");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_task_progress_is_monotonic_and_converges_after_completion() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_progress_monotonic_src");
    let dst_dir = unique_temp_dir("persistent_task_progress_monotonic_dst");
    write_text(&src_dir.join("big.txt"), &"p".repeat(2 * 1024 * 1024));

    let mut options = TaskOptions::default();
    options.buffer_size_bytes = 32 * 1024;

    let harness = create_sqlite_persistent_task(
        "progress-monotonic-task",
        &src_dir,
        vec![dst_dir.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;
    let mut updates = harness.subscribe().expect("subscribe should succeed");

    let monitor = async {
        let mut saw_copying = false;
        let mut saw_active_transfers = false;
        let mut previous_complete = 0_u64;
        let mut last_progress = None;

        for _ in 0..1024 {
            let update =
                match tokio::time::timeout(Duration::from_millis(200), updates.recv()).await {
                    Ok(Ok(update)) => update,
                    Ok(Err(_)) => continue,
                    Err(_) => break,
                };

            if let TaskUpdate::Progress(progress) = update {
                assert!(
                    progress.complete_bytes >= previous_complete,
                    "complete_bytes should be monotonic"
                );
                assert!(
                    progress.complete_bytes <= progress.total_bytes,
                    "complete_bytes should not exceed total_bytes"
                );

                if !progress.active_transfers.is_empty() {
                    saw_active_transfers = true;
                }
                if progress
                    .active_transfers
                    .iter()
                    .any(|transfer| transfer.phase == TransferPhase::Copying)
                {
                    saw_copying = true;
                }

                previous_complete = progress.complete_bytes;
                last_progress = Some(progress);
            }
        }

        (saw_copying, saw_active_transfers, last_progress)
    };

    let run = async {
        harness.run().await.expect("run should succeed");
    };

    let ((saw_copying, saw_active_transfers, last_progress), ()) = tokio::join!(monitor, run);

    let last = last_progress.expect("should receive at least one progress update");
    assert!(
        saw_active_transfers,
        "expected active transfer progress updates"
    );
    assert!(
        saw_copying,
        "expected to observe copying phase in progress updates"
    );
    assert_eq!(last.complete_bytes, last.total_bytes);
    assert!(
        last.active_transfers.is_empty(),
        "final progress update should clear active transfers"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_task_progress_totals_match_multi_destination_expected_bytes() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_progress_totals_src");
    let dst_a = unique_temp_dir("persistent_task_progress_totals_dst_a");
    let dst_b = unique_temp_dir("persistent_task_progress_totals_dst_b");
    let payload = "m".repeat(512 * 1024);
    write_text(&src_dir.join("sized.bin"), &payload);

    let mut options = TaskOptions::default();
    options.buffer_size_bytes = 16 * 1024;

    let harness = create_sqlite_persistent_task(
        "progress-totals-task",
        &src_dir,
        vec![dst_a.clone(), dst_b.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;
    let mut updates = harness.subscribe().expect("subscribe should succeed");

    let expected_total = (payload.len() as u64) * 2;

    let monitor = async {
        let mut last_progress = None;
        for _ in 0..1024 {
            let update =
                match tokio::time::timeout(Duration::from_millis(200), updates.recv()).await {
                    Ok(Ok(update)) => update,
                    Ok(Err(_)) => continue,
                    Err(_) => break,
                };

            if let TaskUpdate::Progress(progress) = update {
                last_progress = Some(progress);
            }
        }
        last_progress
    };

    let run = async {
        harness.run().await.expect("run should succeed");
    };

    let (last_progress, ()) = tokio::join!(monitor, run);
    let final_progress = last_progress.expect("should receive progress updates");

    assert_eq!(
        final_progress.total_bytes, expected_total,
        "total progress bytes should account for every destination branch"
    );
    assert_eq!(
        final_progress.complete_bytes, expected_total,
        "complete progress bytes should reach expected total at completion"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_task_emits_detailed_progress_with_active_transfer_and_stage_updates() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_detailed_progress_src");
    let dst_dir = unique_temp_dir("persistent_task_detailed_progress_dst");
    write_text(&src_dir.join("big.txt"), &"x".repeat(2 * 1024 * 1024));

    let mut options = TaskOptions::default();
    options.buffer_size_bytes = 64 * 1024;

    let harness = create_sqlite_persistent_task_with_verification_stages(
        "detailed-progress-task",
        &src_dir,
        vec![dst_dir.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;
    let mut updates = harness.subscribe().expect("subscribe should succeed");

    let mut seen_active_transfer = false;
    let mut seen_stage_progress = false;

    let monitor = async {
        for _ in 0..512 {
            let update =
                match tokio::time::timeout(Duration::from_millis(200), updates.recv()).await {
                    Ok(Ok(update)) => update,
                    Ok(Err(_)) => continue,
                    Err(_) => break,
                };

            if let TaskUpdate::Progress(progress) = update {
                if progress.active_transfers.iter().any(|transfer| {
                    transfer.source_path.ends_with("big.txt")
                        && transfer.destination_path.ends_with("big.txt")
                        && transfer.phase != TransferPhase::Pending
                }) {
                    seen_active_transfer = true;
                }

                if !progress.stage_progresses.is_empty() {
                    seen_stage_progress = true;
                }

                if seen_active_transfer && seen_stage_progress {
                    break;
                }
            }
        }
    };

    let run = async {
        harness.run().await.expect("run should succeed");
    };

    tokio::join!(monitor, run);

    assert!(
        seen_active_transfer,
        "expected progress updates to include current file transfer details"
    );
    assert!(
        seen_stage_progress,
        "expected progress updates to include pipeline stage details"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_task_emits_preparing_state_before_running() {
    init_test_logging();
    let src_dir = unique_temp_dir("persistent_task_preparing_state_src");
    let dst_dir = unique_temp_dir("persistent_task_preparing_state_dst");
    write_text(&src_dir.join("small.txt"), "hello preparing");

    let options = TaskOptions::default();

    let harness = create_sqlite_persistent_task(
        "preparing-state-task",
        &src_dir,
        vec![dst_dir],
        TaskState::Created,
        options,
        TaskFlowPlan::new(),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;
    let mut updates = harness.subscribe().expect("subscribe should succeed");

    harness.run().await.expect("run should succeed");

    let mut seen_preparing = false;
    let mut seen_running_after_preparing = false;

    for _ in 0..128 {
        let update = match tokio::time::timeout(Duration::from_millis(100), updates.recv()).await {
            Ok(Ok(update)) => update,
            Ok(Err(_)) => continue,
            Err(_) => break,
        };

        if let TaskUpdate::State(state) = update {
            if state == TaskState::Preparing {
                seen_preparing = true;
            }

            if seen_preparing && state == TaskState::Running {
                seen_running_after_preparing = true;
                break;
            }
        }
    }

    assert!(seen_preparing, "expected to observe Preparing state update");
    assert!(
        seen_running_after_preparing,
        "expected to observe Running state update after Preparing"
    );
}
