//! Core APIs for defining copy tasks, executing them, and observing progress.
//!
//! `sure_copy_core` is split into five layers:
//! - `domain`: task specs, options, reports, and error types.
//! - `interface`: stable facade APIs for Tauri or CLI callers.
//! - `orchestrator`: in-memory and SQLite-backed task runtimes.
//! - `pipeline`: source-observer and post-write flow contracts.
//! - `infrastructure`: filesystem and checksum adapters used by the runtime.
//!
//! See `sure_copy_core/README.md` for usage examples and a fuller design guide.

pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod orchestrator;
pub mod pipeline;
#[doc(hidden)]
pub mod testing;

pub use domain::{
    ActiveTransferProgress, CopyError, CopyErrorCategory, CopyReport, CopyTask, CopyTaskId,
    DestinationCopyStatus, DestinationPostWriteStatus, DestinationReport, FileFailure, FilePlan,
    OverwritePolicy, RetryPolicy, StageProgress, StageProgressStatus, TaskOptions, TaskProgress,
    TaskSpec, TaskState, TaskTemplate, TransferPhase, VerificationPolicy,
};
pub use interface::SureCopyCoreApi;
pub use orchestrator::{
    InMemoryTask, InMemoryTaskOrchestrator, OrchestratorConfig, SqliteTaskOrchestrator, Task,
    TaskOrchestrator, TaskStream, TaskUpdate,
};
pub use pipeline::{
    ArtifactValue, NoopPostWriteStage, NoopSourceObserverStage, PipelineArtifacts,
    PostWriteContext, PostWritePipeline, PostWritePipelineMode, PostWriteStage, SourceChunk,
    SourceObserverPipeline, SourceObserverStage, SourcePipelineMode, StageArtifacts, StageId,
    StageRuntimeProgress, TaskFlowPlan,
};
