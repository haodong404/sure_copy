use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sure_copy_core::pipeline::{
    NoopPostWriteStage, NoopSourceObserverStage, PipelineArtifacts, PostWriteContext,
    PostWritePipeline, PostWritePipelineMode, PostWriteStage, SourceChunk, SourceObserverPipeline,
    SourceObserverStage, SourcePipelineMode, StageArtifacts, StageId, TaskFlowPlan,
};
use sure_copy_core::{CopyError, CopyTask, FilePlan, TaskOptions};

#[derive(Default)]
struct CountingObserverStage {
    seen_bytes: Mutex<u64>,
}

#[async_trait]
impl SourceObserverStage for CountingObserverStage {
    fn id(&self) -> StageId {
        "counting-observer"
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

#[async_trait]
impl PostWriteStage for RecordingPostWriteStage {
    fn id(&self) -> StageId {
        "recording-post-write"
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
