use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use crate::domain::CopyError;

/// Abstract filesystem adapter for platform-independent file operations.
#[async_trait]
pub trait FileSystemAdapter: Send + Sync {
    async fn exists(&self, _path: &Path) -> Result<bool, CopyError> {
        Err(CopyError::not_implemented("FileSystemAdapter::exists"))
    }

    async fn create_dir_all(&self, _path: &Path) -> Result<(), CopyError> {
        Err(CopyError::not_implemented("FileSystemAdapter::create_dir_all"))
    }

    async fn read_dir_recursive(&self, _path: &Path) -> Result<Vec<PathBuf>, CopyError> {
        Err(CopyError::not_implemented("FileSystemAdapter::read_dir_recursive"))
    }
}

/// Abstract checksum provider.
#[async_trait]
pub trait ChecksumProvider: Send + Sync {
    fn algorithm(&self) -> &'static str;

    async fn checksum_file(&self, _path: &Path) -> Result<String, CopyError> {
        Err(CopyError::not_implemented("ChecksumProvider::checksum_file"))
    }
}

/// Persisted state for resume support.
#[async_trait]
pub trait RuntimeStateStore: Send + Sync {
    async fn save_task_checkpoint(&self, _task_id: &str, _payload: &[u8]) -> Result<(), CopyError> {
        Err(CopyError::not_implemented(
            "RuntimeStateStore::save_task_checkpoint",
        ))
    }

    async fn load_task_checkpoint(&self, _task_id: &str) -> Result<Option<Vec<u8>>, CopyError> {
        Err(CopyError::not_implemented(
            "RuntimeStateStore::load_task_checkpoint",
        ))
    }
}

/// Structured logging abstraction.
pub trait StructuredLogger: Send + Sync {
    fn info(&self, _event: &str, _fields: &[(&str, String)]);
    fn warn(&self, _event: &str, _fields: &[(&str, String)]);
    fn error(&self, _event: &str, _fields: &[(&str, String)]);
}

/// Time abstraction for deterministic testing.
#[async_trait]
pub trait TimeProvider: Send + Sync {
    fn now(&self) -> SystemTime;

    async fn sleep(&self, _duration: Duration) {
        // The default implementation intentionally does nothing.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StubChecksum;

    impl ChecksumProvider for StubChecksum {
        fn algorithm(&self) -> &'static str {
            "SHA-256"
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checksum_provider_default_execute_is_not_implemented() {
        let provider = StubChecksum;
        let err = provider
            .checksum_file(Path::new("/tmp/file"))
            .await
            .expect_err("must be not implemented");

        assert_eq!(err.code, "NOT_IMPLEMENTED");
        assert!(err.message.contains("ChecksumProvider::checksum_file"));
    }
}
