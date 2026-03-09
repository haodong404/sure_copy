use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::domain::{CopyError, CopyTask, FilePlan};
use crate::infrastructure::{ChecksumProvider, StreamingChecksum};
use crate::pipeline::{PostWriteStage, SourceObserverStage};

use super::super::types::{PostWriteContext, SourceChunk, StageArtifacts, StageId};

const SOURCE_HASH_STAGE_ID: StageId = "builtin-source-hash";
const DESTINATION_CHECKSUM_VERIFY_STAGE_ID: StageId = "builtin-destination-checksum-verify";
const CHECKSUM_ARTIFACT_KEY: &str = "checksum";

pub(crate) struct SourceHashStage {
    checksum_provider: Arc<dyn ChecksumProvider>,
    checksum: Mutex<Option<Box<dyn StreamingChecksum>>>,
}

impl SourceHashStage {
    pub(crate) fn new(checksum_provider: Arc<dyn ChecksumProvider>) -> Self {
        Self {
            checksum_provider,
            checksum: Mutex::new(None),
        }
    }
}

#[async_trait]
impl SourceObserverStage for SourceHashStage {
    fn id(&self) -> StageId {
        SOURCE_HASH_STAGE_ID
    }

    async fn observe_chunk(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
        chunk: &SourceChunk,
    ) -> Result<(), CopyError> {
        let mut checksum = self
            .checksum
            .lock()
            .map_err(|_| CopyError::not_implemented("SourceHashStage::lock"))?;
        let hasher = checksum.get_or_insert(self.checksum_provider.begin_stream()?);
        hasher.update(chunk.bytes.as_ref());
        Ok(())
    }

    async fn finish(
        &self,
        _task: &CopyTask,
        _plan: &FilePlan,
    ) -> Result<StageArtifacts, CopyError> {
        let mut checksum = self
            .checksum
            .lock()
            .map_err(|_| CopyError::not_implemented("SourceHashStage::lock"))?;
        let digest = checksum
            .take()
            .unwrap_or(self.checksum_provider.begin_stream()?)
            .finalize();
        Ok(StageArtifacts::new().with_value(CHECKSUM_ARTIFACT_KEY, digest))
    }
}

pub(crate) struct DestinationChecksumVerifyStage {
    checksum_provider: Arc<dyn ChecksumProvider>,
}

impl DestinationChecksumVerifyStage {
    pub(crate) fn new(checksum_provider: Arc<dyn ChecksumProvider>) -> Self {
        Self { checksum_provider }
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

    async fn execute(&self, ctx: &PostWriteContext) -> Result<StageArtifacts, CopyError> {
        let source_checksum = ctx
            .pipeline_artifacts
            .get_string(SOURCE_HASH_STAGE_ID, CHECKSUM_ARTIFACT_KEY)
            .ok_or(CopyError {
                category: crate::domain::CopyErrorCategory::Checksum,
                code: "SOURCE_CHECKSUM_MISSING",
                message: format!(
                    "post-write verification requires a source checksum for '{}'",
                    ctx.source_path.display()
                ),
            })?;
        let destination_checksum = self
            .checksum_provider
            .checksum_file(&ctx.actual_destination_path)
            .await?;

        if destination_checksum == *source_checksum {
            Ok(StageArtifacts::default())
        } else {
            Err(checksum_mismatch_error(
                &ctx.source_path,
                &ctx.actual_destination_path,
            ))
        }
    }
}
