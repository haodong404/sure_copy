use async_trait::async_trait;

use crate::domain::{CopyError, CopyTask, FilePlan, StageProgressStatus};

use super::types::{
    PostWriteContext, SourceChunk, StageArtifacts, StageId, StageSpec, StageStateSpec,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageRuntimeProgress {
    pub status: StageProgressStatus,
    pub processed_bytes: u64,
    pub total_bytes: Option<u64>,
}

impl StageRuntimeProgress {
    pub fn pending(total_bytes: Option<u64>) -> Self {
        Self {
            status: StageProgressStatus::Pending,
            processed_bytes: 0,
            total_bytes,
        }
    }
}

#[async_trait]
pub trait SourceObserverStage: Send + Sync {
    fn id(&self) -> StageId;

    fn spec(&self) -> Option<StageSpec> {
        None
    }

    fn snapshot_state(&self) -> Option<StageStateSpec> {
        None
    }

    fn restore_state(&self, _state: &StageStateSpec) -> Result<(), CopyError> {
        Ok(())
    }

    fn reset_progress(&self, _total_bytes: Option<u64>) {}

    fn progress_state(&self) -> Option<StageRuntimeProgress> {
        None
    }

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

    fn spec(&self) -> Option<StageSpec> {
        None
    }

    fn snapshot_state(&self) -> Option<StageStateSpec> {
        None
    }

    fn restore_state(&self, _state: &StageStateSpec) -> Result<(), CopyError> {
        Ok(())
    }

    fn reset_progress(&self, _total_bytes: Option<u64>) {}

    fn progress_state(&self) -> Option<StageRuntimeProgress> {
        None
    }

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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use async_trait::async_trait;

    use crate::domain::StageProgressStatus;
    use crate::pipeline::PipelineArtifacts;

    use super::*;

    struct DefaultObserver;

    #[async_trait]
    impl SourceObserverStage for DefaultObserver {
        fn id(&self) -> StageId {
            "default-observer"
        }
    }

    struct DefaultPostWrite;

    #[async_trait]
    impl PostWriteStage for DefaultPostWrite {
        fn id(&self) -> StageId {
            "default-post-write"
        }
    }

    #[test]
    fn pending_progress_starts_from_zero() {
        let progress = StageRuntimeProgress::pending(Some(42));
        assert_eq!(progress.status, StageProgressStatus::Pending);
        assert_eq!(progress.processed_bytes, 0);
        assert_eq!(progress.total_bytes, Some(42));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_stage_trait_methods_are_safe() {
        let observer = DefaultObserver;
        observer.reset_progress(Some(12));
        assert!(observer.progress_state().is_none());
        assert!(observer.snapshot_state().is_none());
        observer
            .restore_state(&StageStateSpec::new("default-observer"))
            .expect("default restore should be a no-op");

        let task = CopyTask::new(
            "default-stage",
            PathBuf::from("/src"),
            vec![PathBuf::from("/dst")],
            Default::default(),
        );
        let plan = FilePlan {
            source: PathBuf::from("/src/a.txt"),
            destinations: vec![PathBuf::from("/dst/a.txt")],
            expected_size_bytes: Some(1),
            expected_checksum: None,
        };

        let err = observer
            .observe_chunk(
                &task,
                &plan,
                &SourceChunk {
                    offset: 0,
                    bytes: std::sync::Arc::<[u8]>::from(&b"a"[..]),
                },
            )
            .await
            .expect_err("default observer should be not implemented");
        assert_eq!(err.code, "NOT_IMPLEMENTED");

        let post_write = DefaultPostWrite;
        post_write.reset_progress(Some(10));
        assert!(post_write.progress_state().is_none());
        assert!(post_write.snapshot_state().is_none());
        post_write
            .restore_state(&StageStateSpec::new("default-post-write"))
            .expect("default restore should be a no-op");

        let err = post_write
            .execute(&PostWriteContext {
                task_id: "task".to_string(),
                source_path: PathBuf::from("/src/a.txt"),
                requested_destination_path: PathBuf::from("/dst/a.txt"),
                actual_destination_path: PathBuf::from("/dst/a.txt"),
                bytes_written: 1,
                expected_bytes: 1,
                pipeline_artifacts: PipelineArtifacts::new(),
            })
            .await
            .expect_err("default post-write should be not implemented");
        assert_eq!(err.code, "NOT_IMPLEMENTED");
    }
}
