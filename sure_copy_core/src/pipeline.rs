use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::domain::{CopyError, CopyTask, FilePlan};

/// Stable identifier for a processing stage.
pub type StageId = &'static str;

/// Describes whether this is a pre-copy or post-copy pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineKind {
    PreCopy,
    PostCopy,
}

/// Defines how the pre-copy pipeline runs relative to copy execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreCopyPipelineMode {
    /// Pre-copy stages run concurrently while copy is running.
    ConcurrentWithCopy,
    /// Pre-copy stages complete before copy starts.
    SerialBeforeCopy,
}

/// Stage capabilities used for scheduling and topology validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageCapability {
    pub parallelizable: bool,
    pub output_size_changing: bool,
    pub reversible: bool,
    pub cpu_intensive: bool,
}

/// Scheduling hint for runtime execution strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageExecutionHint {
    IoBound,
    CpuBound,
}

/// Input execution mode for stage groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageExecution {
    Serial,
    Parallel,
}

/// Unified stage output placeholder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageOutput {
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub metadata: Vec<(String, String)>,
}

/// One streaming chunk produced by a stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageChunk {
    pub bytes: Vec<u8>,
    pub metadata: Vec<(String, String)>,
}

/// Stream item type between stages.
pub type StageItem = Result<StageChunk, CopyError>;

/// Async stream contract used by pipeline stages.
#[async_trait]
pub trait StageStream: Send {
    async fn next(&mut self) -> Option<StageItem>;
}

/// Boxed stage stream trait object.
pub type BoxStageStream = Box<dyn StageStream>;

/// In-memory stream implementation useful for tests and skeleton wiring.
pub struct InMemoryStageStream {
    items: VecDeque<StageItem>,
}

impl InMemoryStageStream {
    pub fn new(items: Vec<StageItem>) -> Self {
        Self {
            items: items.into(),
        }
    }
}

#[async_trait]
impl StageStream for InMemoryStageStream {
    async fn next(&mut self) -> Option<StageItem> {
        self.items.pop_front()
    }
}

#[async_trait]
pub trait ProcessingStage: Send + Sync {
    fn id(&self) -> StageId;
    fn capability(&self) -> StageCapability;

    /// Provides optional scheduler hint.
    fn execution_hint(&self) -> StageExecutionHint {
        if self.capability().cpu_intensive {
            StageExecutionHint::CpuBound
        } else {
            StageExecutionHint::IoBound
        }
    }

    /// Executes the stage with an optional upstream stream.
    ///
    /// In serial mode, a stage receives the previous stage's stream as `input`.
    /// For source stages, `input` can be `None` and the stage should create its own stream.
    ///
    /// This skeleton returns a structured not-implemented error by default.
    async fn execute_stream(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
        _input: Option<BoxStageStream>,
    ) -> Result<BoxStageStream, CopyError> {
        Err(CopyError::not_implemented("ProcessingStage::execute_stream"))
    }

    /// Optionally returns aggregated stage metrics.
    async fn output_summary(&self) -> Result<StageOutput, CopyError> {
        Err(CopyError::not_implemented("ProcessingStage::output_summary"))
    }
}

/// Stage topology node supporting mixed serial/parallel composition.
pub enum StageNode {
    Stage(Box<dyn ProcessingStage>),
    Group {
        execution: StageExecution,
        children: Vec<StageNode>,
    },
}

/// Pipeline definition independent from concrete execution engine.
pub struct Pipeline {
    pub kind: PipelineKind,
    pub root: StageNode,
}

impl Pipeline {
    /// Creates a pipeline with a root stage group.
    pub fn new(kind: PipelineKind, execution: StageExecution, children: Vec<StageNode>) -> Self {
        Self {
            kind,
            root: StageNode::Group {
                execution,
                children,
            },
        }
    }
}

/// Task-level pipeline customization injected at task creation time.
#[derive(Clone)]
pub struct TaskPipelinePlan {
    pub pre_copy_pipeline: Option<Arc<Pipeline>>,
    pub post_copy_pipeline: Option<Arc<Pipeline>>,
    pub pre_copy_mode: PreCopyPipelineMode,
}

impl Default for TaskPipelinePlan {
    fn default() -> Self {
        Self {
            pre_copy_pipeline: None,
            post_copy_pipeline: None,
            pre_copy_mode: PreCopyPipelineMode::ConcurrentWithCopy,
        }
    }
}

impl TaskPipelinePlan {
    /// Creates a plan with defaults and no optional pipelines.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the pre-copy pipeline.
    pub fn with_pre_copy_pipeline(mut self, pipeline: Arc<Pipeline>) -> Self {
        self.pre_copy_pipeline = Some(pipeline);
        self
    }

    /// Sets the post-copy pipeline.
    pub fn with_post_copy_pipeline(mut self, pipeline: Arc<Pipeline>) -> Self {
        self.post_copy_pipeline = Some(pipeline);
        self
    }

    /// Sets pre-copy scheduling mode.
    pub fn with_pre_copy_mode(mut self, mode: PreCopyPipelineMode) -> Self {
        self.pre_copy_mode = mode;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{CopyTask, TaskOptions, TaskState};
    use std::path::PathBuf;

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

    #[test]
    fn task_pipeline_plan_defaults_to_parallel_pre_copy_mode() {
        let plan = TaskPipelinePlan::default();
        assert!(plan.pre_copy_pipeline.is_none());
        assert!(plan.post_copy_pipeline.is_none());
        assert_eq!(plan.pre_copy_mode, PreCopyPipelineMode::ConcurrentWithCopy);
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

    #[async_trait]
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

    #[async_trait]
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
}
