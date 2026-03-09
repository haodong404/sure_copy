use super::stage::ProcessingStage;

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
    /// Reserved for a future runtime that can execute the pre-copy pipeline concurrently.
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

impl Default for StageCapability {
    fn default() -> Self {
        Self {
            parallelizable: true,
            output_size_changing: false,
            reversible: false,
            cpu_intensive: false,
        }
    }
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
pub type StageItem = Result<StageChunk, crate::domain::CopyError>;

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
