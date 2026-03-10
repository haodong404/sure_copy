use std::sync::Arc;

use crate::domain::CopyError;

use super::stage::{PostWriteStage, SourceObserverStage};
use super::types::StageSpec;

/// Application-provided constructor registry that rebuilds runtime stage
/// instances from durably persisted `StageSpec` values.
pub trait StageRegistry: Send + Sync {
    fn build_source_observer_stage(
        &self,
        _spec: &StageSpec,
    ) -> Result<Option<Arc<dyn SourceObserverStage>>, CopyError> {
        Ok(None)
    }

    fn build_post_write_stage(
        &self,
        _spec: &StageSpec,
    ) -> Result<Option<Arc<dyn PostWriteStage>>, CopyError> {
        Ok(None)
    }
}
