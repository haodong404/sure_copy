//! Core APIs for defining copy tasks, executing them, and observing progress.
//!
//! `sure_copy_core` is split into five layers:
//! - `domain`: task specs, options, reports, and error types.
//! - `interface`: stable facade APIs for Tauri or CLI callers.
//! - `orchestrator`: in-memory and SQLite-backed task runtimes.
//! - `pipeline`: stage contracts and serial stream composition primitives.
//! - `infrastructure`: filesystem and checksum adapters used by the runtime.
//!
//! See `sure_copy_core/README.md` for usage examples and a fuller design guide.

pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod orchestrator;
pub mod pipeline;

pub use domain::{
    CopyError, CopyErrorCategory, CopyReport, CopyTask, CopyTaskId, FileFailure, FilePlan,
    OverwritePolicy, RetryPolicy, TaskOptions, TaskProgress, TaskSpec, TaskState, TaskTemplate,
    VerificationPolicy,
};
pub use interface::SureCopyCoreApi;
pub use orchestrator::{
    InMemoryTask, InMemoryTaskOrchestrator, OrchestratorConfig, SqliteTaskOrchestrator, Task,
    TaskOrchestrator, TaskStream, TaskUpdate,
};
pub use pipeline::{
    BoxStageStream, InMemoryStageStream, Pipeline, PipelineKind, PreCopyPipelineMode,
    ProcessingStage, StageCapability, StageChunk, StageExecution, StageExecutionHint, StageId,
    StageItem, StageNode, StageOutput, StageStream, TaskPipelinePlan,
};
