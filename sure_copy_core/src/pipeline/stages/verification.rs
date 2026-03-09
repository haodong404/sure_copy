use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::domain::{CopyError, CopyTask, FilePlan, StageProgressStatus};
use crate::infrastructure::{ChecksumProvider, StreamingChecksum};
use crate::pipeline::{PostWriteStage, SourceObserverStage, StageRuntimeProgress};

use super::super::types::{PostWriteContext, SourceChunk, StageArtifacts, StageId};

const SOURCE_HASH_STAGE_ID: StageId = "builtin-source-hash";
const DESTINATION_CHECKSUM_VERIFY_STAGE_ID: StageId = "builtin-destination-checksum-verify";
const CHECKSUM_ARTIFACT_KEY: &str = "checksum";

pub(crate) struct SourceHashStage {
    checksum_provider: Arc<dyn ChecksumProvider>,
    checksum: Mutex<Option<Box<dyn StreamingChecksum>>>,
    progress: Mutex<StageRuntimeProgress>,
}

impl SourceHashStage {
    pub(crate) fn new(checksum_provider: Arc<dyn ChecksumProvider>) -> Self {
        Self {
            checksum_provider,
            checksum: Mutex::new(None),
            progress: Mutex::new(StageRuntimeProgress::pending(None)),
        }
    }

    fn mark_failed(&self) {
        if let Ok(mut progress) = self.progress.lock() {
            progress.status = StageProgressStatus::Failed;
        }
    }
}

#[async_trait]
impl SourceObserverStage for SourceHashStage {
    fn id(&self) -> StageId {
        SOURCE_HASH_STAGE_ID
    }

    fn reset_progress(&self, total_bytes: Option<u64>) {
        if let Ok(mut progress) = self.progress.lock() {
            *progress = StageRuntimeProgress::pending(total_bytes);
        }
    }

    fn progress_state(&self) -> Option<StageRuntimeProgress> {
        self.progress.lock().ok().map(|progress| progress.clone())
    }

    async fn observe_chunk(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
        chunk: &SourceChunk,
    ) -> Result<(), CopyError> {
        let mut checksum = self.checksum.lock().map_err(|_| {
            self.mark_failed();
            CopyError::not_implemented("SourceHashStage::lock")
        })?;
        if checksum.is_none() {
            match self.checksum_provider.begin_stream() {
                Ok(stream) => {
                    *checksum = Some(stream);
                }
                Err(err) => {
                    self.mark_failed();
                    return Err(err);
                }
            }
        }
        let hasher = checksum
            .as_mut()
            .expect("checksum stream must exist after lazy initialization");
        hasher.update(chunk.bytes.as_ref());
        if let Ok(mut progress) = self.progress.lock() {
            progress.status = StageProgressStatus::Running;
            progress.processed_bytes = progress.processed_bytes.saturating_add(chunk.len() as u64);
            if let Some(total) = progress.total_bytes {
                progress.processed_bytes = progress.processed_bytes.min(total);
            }
        }
        Ok(())
    }

    async fn finish(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
    ) -> Result<StageArtifacts, CopyError> {
        let mut checksum = self.checksum.lock().map_err(|_| {
            self.mark_failed();
            CopyError::not_implemented("SourceHashStage::lock")
        })?;
        let digest = match checksum.take() {
            Some(stream) => stream.finalize(),
            None => match self.checksum_provider.begin_stream() {
                Ok(stream) => stream.finalize(),
                Err(err) => {
                    self.mark_failed();
                    return Err(err);
                }
            },
        };
        if let Ok(mut progress) = self.progress.lock() {
            progress.status = StageProgressStatus::Completed;
            if let Some(total) = progress.total_bytes {
                progress.processed_bytes = total;
            }
        }
        Ok(StageArtifacts::new().with_value(CHECKSUM_ARTIFACT_KEY, digest))
    }
}

pub(crate) struct DestinationChecksumVerifyStage {
    checksum_provider: Arc<dyn ChecksumProvider>,
    progress: Mutex<StageRuntimeProgress>,
}

impl DestinationChecksumVerifyStage {
    pub(crate) fn new(checksum_provider: Arc<dyn ChecksumProvider>) -> Self {
        Self {
            checksum_provider,
            progress: Mutex::new(StageRuntimeProgress::pending(None)),
        }
    }

    fn mark_failed(&self) {
        if let Ok(mut progress) = self.progress.lock() {
            progress.status = StageProgressStatus::Failed;
        }
    }
}

fn checksum_mismatch_error(source: &Path, destination: &Path) -> CopyError {
    CopyError {
        category: crate::domain::CopyErrorCategory::Checksum,
        code: "CHECKSUM_MISMATCH",
        message: format!(
            "copy verification failed because '{}' and '{}' do not match",
            source.display(),
            destination.display()
        ),
    }
}

#[async_trait]
impl PostWriteStage for DestinationChecksumVerifyStage {
    fn id(&self) -> StageId {
        DESTINATION_CHECKSUM_VERIFY_STAGE_ID
    }

    fn reset_progress(&self, total_bytes: Option<u64>) {
        if let Ok(mut progress) = self.progress.lock() {
            *progress = StageRuntimeProgress::pending(total_bytes);
        }
    }

    fn progress_state(&self) -> Option<StageRuntimeProgress> {
        self.progress.lock().ok().map(|progress| progress.clone())
    }

    async fn execute(&self, ctx: &PostWriteContext) -> Result<StageArtifacts, CopyError> {
        if let Ok(mut progress) = self.progress.lock() {
            progress.status = StageProgressStatus::Running;
            progress.total_bytes = Some(ctx.expected_bytes);
            progress.processed_bytes = ctx.bytes_written.min(ctx.expected_bytes);
        }

        let source_checksum = match ctx
            .pipeline_artifacts
            .get_string(SOURCE_HASH_STAGE_ID, CHECKSUM_ARTIFACT_KEY)
        {
            Some(checksum) => checksum,
            None => {
                self.mark_failed();
                return Err(CopyError {
                    category: crate::domain::CopyErrorCategory::Checksum,
                    code: "SOURCE_CHECKSUM_MISSING",
                    message: format!(
                        "post-write verification requires a source checksum for '{}'",
                        ctx.source_path.display()
                    ),
                });
            }
        };
        let destination_checksum = match self
            .checksum_provider
            .checksum_file(&ctx.actual_destination_path)
            .await
        {
            Ok(checksum) => checksum,
            Err(err) => {
                self.mark_failed();
                return Err(err);
            }
        };

        if destination_checksum == *source_checksum {
            if let Ok(mut progress) = self.progress.lock() {
                progress.status = StageProgressStatus::Completed;
                progress.processed_bytes = progress.total_bytes.unwrap_or(ctx.expected_bytes);
            }
            Ok(StageArtifacts::default())
        } else {
            if let Ok(mut progress) = self.progress.lock() {
                progress.status = StageProgressStatus::Failed;
            }
            Err(checksum_mismatch_error(
                &ctx.source_path,
                &ctx.actual_destination_path,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use async_trait::async_trait;

    use crate::domain::CopyErrorCategory;
    use crate::infrastructure::ChecksumProvider;
    use crate::pipeline::{PipelineArtifacts, PostWriteContext};

    use super::*;

    struct FailingChecksumProvider;

    #[async_trait]
    impl ChecksumProvider for FailingChecksumProvider {
        fn algorithm(&self) -> &'static str {
            "test"
        }

        fn begin_stream(
            &self,
        ) -> Result<Box<dyn crate::infrastructure::StreamingChecksum>, CopyError> {
            Err(CopyError {
                category: CopyErrorCategory::Checksum,
                code: "BEGIN_STREAM_FAILED",
                message: "begin stream failed".to_string(),
            })
        }

        async fn checksum_file(&self, _path: &Path) -> Result<String, CopyError> {
            Err(CopyError {
                category: CopyErrorCategory::Checksum,
                code: "CHECKSUM_FILE_FAILED",
                message: "checksum file failed".to_string(),
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_hash_stage_marks_failed_when_stream_initialization_fails() {
        let stage = SourceHashStage::new(Arc::new(FailingChecksumProvider));
        stage.reset_progress(Some(8));

        let task = CopyTask::new(
            "source-hash-fail",
            PathBuf::from("/src"),
            vec![PathBuf::from("/dst")],
            Default::default(),
        );
        let plan = FilePlan {
            source: PathBuf::from("/src/a.txt"),
            destinations: vec![PathBuf::from("/dst/a.txt")],
            expected_size_bytes: Some(8),
            expected_checksum: None,
        };

        let err = stage
            .observe_chunk(
                &task,
                &plan,
                &SourceChunk {
                    offset: 0,
                    bytes: Arc::<[u8]>::from(&b"chunk"[..]),
                },
            )
            .await
            .expect_err("begin_stream failure should propagate");
        assert_eq!(err.code, "BEGIN_STREAM_FAILED");

        let progress = stage
            .progress_state()
            .expect("progress should be visible for source hash stage");
        assert_eq!(progress.status, StageProgressStatus::Failed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn destination_verify_stage_marks_failed_when_source_checksum_missing() {
        let stage = DestinationChecksumVerifyStage::new(Arc::new(FailingChecksumProvider));
        stage.reset_progress(Some(16));

        let err = stage
            .execute(&PostWriteContext {
                task_id: "task".to_string(),
                source_path: PathBuf::from("/src/a.txt"),
                requested_destination_path: PathBuf::from("/dst/a.txt"),
                actual_destination_path: PathBuf::from("/dst/a.txt"),
                bytes_written: 8,
                expected_bytes: 16,
                pipeline_artifacts: PipelineArtifacts::new(),
            })
            .await
            .expect_err("missing source checksum should fail");
        assert_eq!(err.code, "SOURCE_CHECKSUM_MISSING");

        let progress = stage
            .progress_state()
            .expect("progress should be visible for destination verify stage");
        assert_eq!(progress.status, StageProgressStatus::Failed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn destination_verify_stage_marks_failed_when_destination_checksum_errors() {
        let stage = DestinationChecksumVerifyStage::new(Arc::new(FailingChecksumProvider));
        stage.reset_progress(Some(16));

        let mut artifacts = PipelineArtifacts::new();
        artifacts.insert_stage_output(
            "builtin-source-hash",
            StageArtifacts::new().with_value("checksum", "ok"),
        );

        let err = stage
            .execute(&PostWriteContext {
                task_id: "task".to_string(),
                source_path: PathBuf::from("/src/a.txt"),
                requested_destination_path: PathBuf::from("/dst/a.txt"),
                actual_destination_path: PathBuf::from("/dst/a.txt"),
                bytes_written: 8,
                expected_bytes: 16,
                pipeline_artifacts: artifacts,
            })
            .await
            .expect_err("destination checksum failure should propagate");
        assert_eq!(err.code, "CHECKSUM_FILE_FAILED");

        let progress = stage
            .progress_state()
            .expect("progress should be visible for destination verify stage");
        assert_eq!(progress.status, StageProgressStatus::Failed);
    }
}
