use async_trait::async_trait;

use crate::domain::{CopyError, CopyTask, FilePlan};

use super::stream::{BoxStageStream, InMemoryStageStream};
use super::types::{StageCapability, StageExecutionHint, StageId, StageOutput};

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
        Err(CopyError::not_implemented(
            "ProcessingStage::execute_stream",
        ))
    }

    /// Optionally returns aggregated stage metrics.
    async fn output_summary(&self) -> Result<StageOutput, CopyError> {
        Err(CopyError::not_implemented(
            "ProcessingStage::output_summary",
        ))
    }
}

/// Default no-op stage implementation.
///
/// - If input stream exists: pass-through unchanged.
/// - If input stream is absent: emit an empty stream.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopStage;

#[async_trait]
impl ProcessingStage for NoopStage {
    fn id(&self) -> StageId {
        "noop"
    }

    fn capability(&self) -> StageCapability {
        StageCapability::default()
    }

    async fn execute_stream(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
        input: Option<BoxStageStream>,
    ) -> Result<BoxStageStream, CopyError> {
        Ok(match input {
            Some(stream) => stream,
            None => Box::new(InMemoryStageStream::new(Vec::new())),
        })
    }

    async fn output_summary(&self) -> Result<StageOutput, CopyError> {
        Ok(StageOutput {
            bytes_in: 0,
            bytes_out: 0,
            metadata: Vec::new(),
        })
    }
}
