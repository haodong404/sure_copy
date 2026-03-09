use std::path::PathBuf;
use std::sync::Arc;

use sure_copy_core::pipeline::NoopStage;
use sure_copy_core::{
    BoxStageStream, CopyError, CopyTask, FilePlan, InMemoryStageStream, Pipeline, PipelineKind,
    PreCopyPipelineMode, ProcessingStage, StageCapability, StageChunk, StageExecution,
    StageExecutionHint, StageId, StageNode, StageStream, TaskOptions, TaskPipelinePlan, TaskState,
};

struct DummyStage;

impl ProcessingStage for DummyStage {
    fn id(&self) -> StageId {
        "dummy"
    }

    fn capability(&self) -> StageCapability {
        StageCapability {
            parallelizable: true,
            output_size_changing: false,
            reversible: false,
            cpu_intensive: false,
        }
    }
}

#[test]
fn can_build_pipeline_with_parallel_group() {
    let stages = vec![StageNode::Stage(Box::new(DummyStage))];
    let pipeline = Pipeline::new(PipelineKind::PreCopy, StageExecution::Parallel, stages);

    assert_eq!(pipeline.kind, PipelineKind::PreCopy);
    match pipeline.root {
        StageNode::Group {
            execution,
            children,
        } => {
            assert_eq!(execution, StageExecution::Parallel);
            assert_eq!(children.len(), 1);
        }
        StageNode::Stage(_) => panic!("expected group root"),
    }
}

#[test]
fn execution_hint_defaults_to_io_bound_for_non_cpu_stage() {
    let stage = DummyStage;
    assert_eq!(stage.execution_hint(), StageExecutionHint::IoBound);
}

struct CpuHeavyStage;

impl ProcessingStage for CpuHeavyStage {
    fn id(&self) -> StageId {
        "cpu-heavy"
    }

    fn capability(&self) -> StageCapability {
        StageCapability {
            parallelizable: true,
            output_size_changing: false,
            reversible: false,
            cpu_intensive: true,
        }
    }
}

#[test]
fn execution_hint_is_cpu_bound_for_cpu_intensive_stage() {
    let stage = CpuHeavyStage;
    assert_eq!(stage.execution_hint(), StageExecutionHint::CpuBound);
}

#[test]
fn task_pipeline_plan_defaults_to_serial_pre_copy_mode() {
    let plan = TaskPipelinePlan::default();
    assert!(plan.pre_copy_pipeline.is_none());
    assert!(plan.post_copy_pipeline.is_none());
    assert_eq!(plan.pre_copy_mode, PreCopyPipelineMode::SerialBeforeCopy);
}

#[test]
fn task_pipeline_plan_can_be_customized_at_creation() {
    let pre = Arc::new(Pipeline::new(
        PipelineKind::PreCopy,
        StageExecution::Parallel,
        vec![StageNode::Stage(Box::new(DummyStage))],
    ));
    let post = Arc::new(Pipeline::new(
        PipelineKind::PostCopy,
        StageExecution::Serial,
        vec![StageNode::Stage(Box::new(DummyStage))],
    ));

    let plan = TaskPipelinePlan::new()
        .with_pre_copy_pipeline(pre)
        .with_post_copy_pipeline(post)
        .with_pre_copy_mode(PreCopyPipelineMode::ConcurrentWithCopy);

    assert!(plan.pre_copy_pipeline.is_some());
    assert!(plan.post_copy_pipeline.is_some());
    assert_eq!(plan.pre_copy_mode, PreCopyPipelineMode::ConcurrentWithCopy);
}

struct UppercaseStage;

#[async_trait::async_trait]
impl ProcessingStage for UppercaseStage {
    fn id(&self) -> StageId {
        "uppercase"
    }

    fn capability(&self) -> StageCapability {
        StageCapability {
            parallelizable: true,
            output_size_changing: false,
            reversible: false,
            cpu_intensive: false,
        }
    }

    async fn execute_stream(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
        mut input: Option<BoxStageStream>,
    ) -> Result<BoxStageStream, CopyError> {
        let mut out = Vec::new();
        let input = input
            .as_mut()
            .ok_or_else(|| CopyError::not_implemented("UppercaseStage::requires_input"))?;

        while let Some(item) = input.next().await {
            let mut chunk = item?;
            chunk.bytes = chunk.bytes.iter().map(|b| b.to_ascii_uppercase()).collect();
            out.push(Ok(chunk));
        }

        Ok(Box::new(InMemoryStageStream::new(out)))
    }
}

struct PrefixStage;

#[async_trait::async_trait]
impl ProcessingStage for PrefixStage {
    fn id(&self) -> StageId {
        "prefix"
    }

    fn capability(&self) -> StageCapability {
        StageCapability {
            parallelizable: true,
            output_size_changing: true,
            reversible: false,
            cpu_intensive: false,
        }
    }

    async fn execute_stream(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
        mut input: Option<BoxStageStream>,
    ) -> Result<BoxStageStream, CopyError> {
        let mut out = Vec::new();
        let input = input
            .as_mut()
            .ok_or_else(|| CopyError::not_implemented("PrefixStage::requires_input"))?;

        while let Some(item) = input.next().await {
            let mut chunk = item?;
            let mut prefixed = b"P:".to_vec();
            prefixed.extend(chunk.bytes);
            chunk.bytes = prefixed;
            out.push(Ok(chunk));
        }

        Ok(Box::new(InMemoryStageStream::new(out)))
    }
}

async fn collect_stream(mut stream: BoxStageStream) -> Result<Vec<StageChunk>, CopyError> {
    let mut chunks = Vec::new();
    while let Some(item) = stream.next().await {
        chunks.push(item?);
    }
    Ok(chunks)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serial_stage_can_stream_from_previous_stage_output() {
    let task = CopyTask {
        id: "task-001".to_string(),
        source_root: PathBuf::from("/src"),
        destinations: vec![PathBuf::from("/dst")],
        options: TaskOptions::default(),
        pipelines: TaskPipelinePlan::default(),
        state: TaskState::Created,
        file_plans: vec![],
    };
    let plan = FilePlan {
        source: PathBuf::from("/src/file.txt"),
        destinations: vec![PathBuf::from("/dst/file.txt")],
        expected_size_bytes: None,
        expected_checksum: None,
    };

    let seed_stream = InMemoryStageStream::new(vec![
        Ok(StageChunk {
            bytes: b"hello".to_vec(),
            metadata: vec![],
        }),
        Ok(StageChunk {
            bytes: b"world".to_vec(),
            metadata: vec![],
        }),
    ]);

    let uppercase = UppercaseStage;
    let prefix = PrefixStage;

    let stage1_out = uppercase
        .execute_stream(&task, &plan, Some(Box::new(seed_stream)))
        .await
        .expect("stage1 should process stream");
    let stage2_out = prefix
        .execute_stream(&task, &plan, Some(stage1_out))
        .await
        .expect("stage2 should read stage1 stream");

    let out = collect_stream(stage2_out)
        .await
        .expect("should collect final output");
    let payloads: Vec<Vec<u8>> = out.into_iter().map(|c| c.bytes).collect();

    assert_eq!(payloads, vec![b"P:HELLO".to_vec(), b"P:WORLD".to_vec()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_stage_stream_is_fifo_and_exhaustible() {
    let mut stream = InMemoryStageStream::new(vec![
        Ok(StageChunk {
            bytes: b"a".to_vec(),
            metadata: vec![("k1".to_string(), "v1".to_string())],
        }),
        Ok(StageChunk {
            bytes: b"b".to_vec(),
            metadata: vec![("k2".to_string(), "v2".to_string())],
        }),
    ]);

    let first = stream
        .next()
        .await
        .expect("first item should exist")
        .expect("first item should be ok");
    let second = stream
        .next()
        .await
        .expect("second item should exist")
        .expect("second item should be ok");
    let end = stream.next().await;

    assert_eq!(first.bytes, b"a".to_vec());
    assert_eq!(second.bytes, b"b".to_vec());
    assert!(end.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_stage_passes_through_input_stream() {
    let stage = NoopStage;
    let task = CopyTask::new(
        "task-noop",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        TaskOptions::default(),
    );
    let plan = FilePlan {
        source: PathBuf::from("/src/file.txt"),
        destinations: vec![PathBuf::from("/dst/file.txt")],
        expected_size_bytes: None,
        expected_checksum: None,
    };

    let input = InMemoryStageStream::new(vec![Ok(StageChunk {
        bytes: b"noop".to_vec(),
        metadata: vec![("k".to_string(), "v".to_string())],
    })]);

    let out = stage
        .execute_stream(&task, &plan, Some(Box::new(input)))
        .await
        .expect("noop should pass through");
    let chunks = collect_stream(out).await.expect("should collect output");

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].bytes, b"noop".to_vec());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_stage_without_input_returns_empty_stream() {
    let stage = NoopStage;
    let task = CopyTask::new(
        "task-noop-empty",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        TaskOptions::default(),
    );
    let plan = FilePlan {
        source: PathBuf::from("/src/file.txt"),
        destinations: vec![PathBuf::from("/dst/file.txt")],
        expected_size_bytes: None,
        expected_checksum: None,
    };

    let out = stage
        .execute_stream(&task, &plan, None)
        .await
        .expect("noop should handle none input");
    let chunks = collect_stream(out).await.expect("should collect output");

    assert!(chunks.is_empty());
}
