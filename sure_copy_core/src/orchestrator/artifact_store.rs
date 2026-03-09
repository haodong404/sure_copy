use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::domain::CopyError;
use crate::pipeline::PipelineArtifacts;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFingerprint {
    pub size_bytes: u64,
    pub modified_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredSourceArtifacts {
    pub source_path: PathBuf,
    pub fingerprint: SourceFingerprint,
    pub artifacts: PipelineArtifacts,
}

#[async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn load_source_artifacts(
        &self,
        task_id: &str,
        source: &Path,
    ) -> Result<Option<StoredSourceArtifacts>, CopyError>;

    async fn save_source_artifacts(
        &self,
        task_id: &str,
        source: &Path,
        fingerprint: &SourceFingerprint,
        artifacts: &PipelineArtifacts,
    ) -> Result<(), CopyError>;

    async fn delete_source_artifacts(&self, task_id: &str, source: &Path) -> Result<(), CopyError>;

    async fn save_destination_artifacts(
        &self,
        task_id: &str,
        source: &Path,
        destination: &Path,
        artifacts: &PipelineArtifacts,
    ) -> Result<(), CopyError>;

    async fn delete_destination_artifacts(
        &self,
        task_id: &str,
        source: &Path,
        destination: &Path,
    ) -> Result<(), CopyError>;
}
