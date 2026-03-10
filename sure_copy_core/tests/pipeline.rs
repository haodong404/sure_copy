use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use sure_copy_core::pipeline::{
    NoopPostWriteStage, NoopSourceObserverStage, PipelineArtifacts, PostWriteContext,
    PostWritePipeline, PostWritePipelineMode, PostWriteStage, SourceChunk, SourceObserverPipeline,
    SourceObserverStage, SourcePipelineMode, StageArtifacts, StageId, StageRegistry,
    StageRuntimeProgress, StageSpec, StageStateSpec, TaskFlowPlan, TaskFlowSpec,
};
use sure_copy_core::{CopyError, CopyTask, FilePlan, StageProgressStatus, TaskOptions};

#[derive(Default)]
struct CountingObserverStage {
    seen_bytes: Mutex<u64>,
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
        _plan: &FilePlan,
        chunk: &SourceChunk,
    ) -> Result<(), CopyError> {
        let mut seen = self.seen_bytes.lock().expect("lock should not be poisoned");
        *seen += chunk.len() as u64;
        Ok(())
    }

    async fn finish(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
    ) -> Result<StageArtifacts, CopyError> {
        let seen = *self.seen_bytes.lock().expect("lock should not be poisoned");
        Ok(StageArtifacts::new().with_value("seen_bytes", seen.to_string()))
    }
}

#[derive(Default)]
struct RecordingPostWriteStage {
    invocations: Mutex<Vec<(PathBuf, u64)>>,
}

#[derive(Default)]
struct ProgressAwareObserverStage {
    seen_bytes: Mutex<u64>,
}

struct MissingSpecObserverStage;

#[async_trait]
impl SourceObserverStage for MissingSpecObserverStage {
    fn id(&self) -> StageId {
        "missing-spec-observer"
    }
}

#[async_trait]
impl SourceObserverStage for ProgressAwareObserverStage {
    fn id(&self) -> StageId {
        "progress-aware-observer"
    }

    fn reset_progress(&self, _total_bytes: Option<u64>) {
        if let Ok(mut seen) = self.seen_bytes.lock() {
            *seen = 0;
        }
    }

    fn progress_state(&self) -> Option<StageRuntimeProgress> {
        let seen = self.seen_bytes.lock().ok().map(|value| *value).unwrap_or(0);
        Some(StageRuntimeProgress {
            status: StageProgressStatus::Running,
            processed_bytes: seen,
            total_bytes: Some(16),
        })
    }

    async fn observe_chunk(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
        chunk: &SourceChunk,
    ) -> Result<(), CopyError> {
        let mut seen = self.seen_bytes.lock().expect("lock should not be poisoned");
        *seen += chunk.len() as u64;
        Ok(())
    }
}

#[async_trait]
impl PostWriteStage for RecordingPostWriteStage {
    fn id(&self) -> StageId {
        "recording-post-write"
    }

    fn spec(&self) -> Option<StageSpec> {
        Some(StageSpec::new("recording-post-write"))
    }

    async fn execute(&self, ctx: &PostWriteContext) -> Result<StageArtifacts, CopyError> {
        self.invocations
            .lock()
            .expect("lock should not be poisoned")
            .push((ctx.actual_destination_path.clone(), ctx.bytes_written));
        Ok(StageArtifacts::new().with_value("post_write", "ran"))
    }
}

#[test]
fn flow_plan_defaults_to_streaming_source_mode() {
    let flow = TaskFlowPlan::default();
    assert!(flow.source_observer.is_none());
    assert!(flow.post_write.is_none());
    assert_eq!(
        flow.source_pipeline_mode(),
        SourcePipelineMode::ConcurrentWithFanOut
    );
    assert_eq!(
        flow.post_write_pipeline_mode(),
        PostWritePipelineMode::SerialAfterWrite
    );
}

#[test]
fn flow_plan_tracks_runtime_stage_attachments() {
    let flow = TaskFlowPlan::new()
        .with_source_observer(
            SourceObserverPipeline::new(SourcePipelineMode::SerialBeforeFanOut)
                .with_stage(Arc::new(CountingObserverStage::default())),
        )
        .with_post_write(
            PostWritePipeline::new(PostWritePipelineMode::SerialAfterWrite)
                .with_stage(Arc::new(RecordingPostWriteStage::default())),
        );

    assert!(flow.has_runtime_stages());
    assert_eq!(
        flow.source_pipeline_mode(),
        SourcePipelineMode::SerialBeforeFanOut
    );
    assert_eq!(
        flow.post_write_pipeline_mode(),
        PostWritePipelineMode::SerialAfterWrite
    );
}

#[test]
fn flow_plan_can_be_converted_into_durable_spec() {
    let flow = TaskFlowPlan::new()
        .with_source_observer(
            SourceObserverPipeline::new(SourcePipelineMode::SerialBeforeFanOut)
                .with_stage(Arc::new(CountingObserverStage::default())),
        )
        .with_post_write(
            PostWritePipeline::new(PostWritePipelineMode::SerialAfterWrite)
                .with_stage(Arc::new(RecordingPostWriteStage::default())),
        );

    let spec = flow
        .try_to_spec()
        .expect("flow spec conversion should succeed")
        .expect("flow spec should exist");

    assert_eq!(
        spec.source_observer
            .as_ref()
            .map(|pipeline| pipeline.stages[0].kind.as_str()),
        Some("counting-observer")
    );
    assert_eq!(
        spec.post_write
            .as_ref()
            .map(|pipeline| pipeline.stages[0].kind.as_str()),
        Some("recording-post-write")
    );
}

#[test]
fn flow_plan_to_spec_rejects_stage_without_spec() {
    let flow = TaskFlowPlan::new().with_source_observer(
        SourceObserverPipeline::new(SourcePipelineMode::SerialBeforeFanOut)
            .with_stage(Arc::new(MissingSpecObserverStage)),
    );

    let err = flow
        .try_to_spec()
        .expect_err("stages without durable specs should be rejected");
    assert_eq!(err.code, "STAGE_SPEC_MISSING");
}

#[test]
fn task_flow_spec_is_empty_only_when_no_stage_specs_exist() {
    assert!(TaskFlowSpec::default().is_empty());

    let spec = TaskFlowSpec {
        source_observer: Some(sure_copy_core::SourceObserverPipelineSpec {
            mode: SourcePipelineMode::SerialBeforeFanOut,
            stages: vec![StageSpec::new("observer")],
        }),
        post_write: None,
    };

    assert!(!spec.is_empty());
}

#[test]
fn stage_spec_and_stage_state_spec_builders_preserve_json_payloads() {
    let stage_spec = StageSpec::new("demo-stage").with_config(json!({"answer": 42}));
    assert_eq!(stage_spec.kind, "demo-stage");
    assert_eq!(
        stage_spec.config.get("answer").and_then(|v| v.as_i64()),
        Some(42)
    );

    let state_spec = StageStateSpec::new("demo-stage-state").with_state(json!({"offset": 7}));
    assert_eq!(state_spec.kind, "demo-stage-state");
    assert_eq!(
        state_spec.state.get("offset").and_then(|v| v.as_i64()),
        Some(7)
    );
}

#[test]
fn pipeline_artifacts_ignore_empty_stage_outputs() {
    let mut artifacts = PipelineArtifacts::new();
    artifacts.insert_stage_output("empty-stage", StageArtifacts::new());
    assert!(artifacts.is_empty());
}

#[test]
fn default_stage_registry_returns_none_for_unknown_specs() {
    struct DefaultRegistry;
    impl StageRegistry for DefaultRegistry {}

    let registry = DefaultRegistry;
    let source = registry
        .build_source_observer_stage(&StageSpec::new("unknown-source"))
        .expect("default source registry should succeed");
    assert!(source.is_none());

    let post_write = registry
        .build_post_write_stage(&StageSpec::new("unknown-post-write"))
        .expect("default post-write registry should succeed");
    assert!(post_write.is_none());
}

#[test]
fn source_chunk_empty_detection_matches_buffer_length() {
    let empty = SourceChunk {
        offset: 0,
        bytes: Arc::<[u8]>::from(&b""[..]),
    };
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_observer_stage_can_collect_chunk_artifacts() {
    let stage = CountingObserverStage::default();
    let task = CopyTask::new(
        "task-observer",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        TaskOptions::default(),
    );
    let plan = FilePlan {
        source: PathBuf::from("/src/file.txt"),
        destinations: vec![PathBuf::from("/dst/file.txt")],
        expected_size_bytes: Some(8),
        expected_checksum: None,
    };

    stage
        .observe_chunk(
            &task,
            &plan,
            &SourceChunk {
                offset: 0,
                bytes: Arc::<[u8]>::from(&b"hello"[..]),
            },
        )
        .await
        .expect("observer should accept first chunk");
    stage
        .observe_chunk(
            &task,
            &plan,
            &SourceChunk {
                offset: 5,
                bytes: Arc::<[u8]>::from(&b"rust"[..]),
            },
        )
        .await
        .expect("observer should accept second chunk");

    let artifacts = stage
        .finish(&task, &plan)
        .await
        .expect("finish should work");
    assert_eq!(artifacts.get_string("seen_bytes"), Some("9"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_write_stage_receives_destination_context() {
    let stage = RecordingPostWriteStage::default();
    let mut pipeline_artifacts = PipelineArtifacts::new();
    pipeline_artifacts.insert_stage_output(
        "builtin-source-hash",
        StageArtifacts::new().with_value("checksum", "abc"),
    );
    let ctx = PostWriteContext {
        task_id: "task-post-write".to_string(),
        source_path: PathBuf::from("/src/file.txt"),
        requested_destination_path: PathBuf::from("/dst/file.txt"),
        actual_destination_path: PathBuf::from("/dst/file (1).txt"),
        bytes_written: 128,
        expected_bytes: 128,
        pipeline_artifacts,
    };

    let artifacts = stage.execute(&ctx).await.expect("execute should work");
    assert_eq!(artifacts.get_string("post_write"), Some("ran"));
    assert_eq!(
        stage
            .invocations
            .lock()
            .expect("lock should not be poisoned")
            .as_slice(),
        &[(PathBuf::from("/dst/file (1).txt"), 128)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_stages_are_safe_defaults() {
    let task = CopyTask::new(
        "task-noop",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        TaskOptions::default(),
    );
    let plan = FilePlan {
        source: PathBuf::from("/src/file.txt"),
        destinations: vec![PathBuf::from("/dst/file.txt")],
        expected_size_bytes: Some(4),
        expected_checksum: None,
    };

    NoopSourceObserverStage
        .observe_chunk(
            &task,
            &plan,
            &SourceChunk {
                offset: 0,
                bytes: Arc::<[u8]>::from(&b"noop"[..]),
            },
        )
        .await
        .expect("noop observer should accept chunks");
    assert_eq!(
        NoopSourceObserverStage
            .finish(&task, &plan)
            .await
            .expect("noop observer should finish")
            .values
            .is_empty(),
        true
    );

    assert_eq!(
        NoopPostWriteStage
            .execute(&PostWriteContext {
                task_id: "task-noop".to_string(),
                source_path: PathBuf::from("/src/file.txt"),
                requested_destination_path: PathBuf::from("/dst/file.txt"),
                actual_destination_path: PathBuf::from("/dst/file.txt"),
                bytes_written: 4,
                expected_bytes: 4,
                pipeline_artifacts: PipelineArtifacts::default(),
            })
            .await
            .expect("noop post-write should execute")
            .values
            .is_empty(),
        true
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stage_progress_is_accessible_through_trait_api() {
    let stage: Arc<dyn SourceObserverStage> = Arc::new(ProgressAwareObserverStage::default());
    let task = CopyTask::new(
        "task-stage-progress",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        TaskOptions::default(),
    );
    let plan = FilePlan {
        source: PathBuf::from("/src/file.txt"),
        destinations: vec![PathBuf::from("/dst/file.txt")],
        expected_size_bytes: Some(16),
        expected_checksum: None,
    };

    stage.reset_progress(plan.expected_size_bytes);
    stage
        .observe_chunk(
            &task,
            &plan,
            &SourceChunk {
                offset: 0,
                bytes: Arc::<[u8]>::from(&b"progress"[..]),
            },
        )
        .await
        .expect("progress-aware observer should accept chunk");

    let progress = stage
        .progress_state()
        .expect("stage should expose progress state");
    assert_eq!(progress.status, StageProgressStatus::Running);
    assert_eq!(progress.processed_bytes, 8);
    assert_eq!(progress.total_bytes, Some(16));
}
