use async_trait::async_trait;

use crate::domain::{CopyError, CopyTask, FilePlan};

use super::types::{PostWriteContext, SourceChunk, StageArtifacts, StageId};

#[async_trait]
pub trait SourceObserverStage: Send + Sync {
    fn id(&self) -> StageId;

    async fn observe_chunk(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
        _chunk: &SourceChunk,
    ) -> Result<(), CopyError> {
        Err(CopyError::not_implemented(
            "SourceObserverStage::observe_chunk",
        ))
    }

    async fn finish(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
    ) -> Result<StageArtifacts, CopyError> {
        Ok(StageArtifacts::default())
    }
}

#[async_trait]
pub trait PostWriteStage: Send + Sync {
    fn id(&self) -> StageId;

    async fn execute(&self, _ctx: &PostWriteContext) -> Result<StageArtifacts, CopyError> {
        Err(CopyError::not_implemented("PostWriteStage::execute"))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopSourceObserverStage;

#[async_trait]
impl SourceObserverStage for NoopSourceObserverStage {
    fn id(&self) -> StageId {
        "noop-source-observer"
    }

    async fn observe_chunk(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
        _chunk: &SourceChunk,
    ) -> Result<(), CopyError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPostWriteStage;

#[async_trait]
impl PostWriteStage for NoopPostWriteStage {
    fn id(&self) -> StageId {
        "noop-post-write"
    }

    async fn execute(&self, _ctx: &PostWriteContext) -> Result<StageArtifacts, CopyError> {
        Ok(StageArtifacts::default())
    }
}
