//! sure_copy_core public API skeleton.
//!
//! This crate exposes a layered architecture for reliable copy workflows.
//! Implementations are intentionally omitted in this skeleton.

pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod orchestrator;
pub mod pipeline;

pub use domain::{
    CopyError, CopyErrorCategory, CopyReport, CopyTask, CopyTaskId, FileFailure, FilePlan,
    OverwritePolicy, RetryPolicy, TaskOptions, TaskProgress, TaskState, TaskTemplate,
    VerificationPolicy,
};
pub use interface::SureCopyCoreApi;
pub use orchestrator::{
    OrchestratorConfig, Task, TaskOrchestrator, TaskStream, TaskUpdate,
};
pub use pipeline::{
    BoxStageStream, InMemoryStageStream, Pipeline, PipelineKind, PreCopyPipelineMode,
    ProcessingStage, StageCapability, StageChunk, StageExecution, StageExecutionHint, StageId,
    StageItem, StageNode, StageOutput, StageStream, TaskPipelinePlan,
};
