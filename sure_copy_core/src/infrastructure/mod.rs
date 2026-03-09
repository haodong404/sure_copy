use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::domain::CopyError;

pub mod local_fs;
pub mod sha256_checksum;

pub use local_fs::LocalFileSystem;
pub use sha256_checksum::Sha256ChecksumProvider;

/// Abstract filesystem adapter for platform-independent file operations.
#[async_trait]
pub trait FileSystem: Send + Sync {
    async fn exists(&self, _path: &Path) -> Result<bool, CopyError> {
        Err(CopyError::not_implemented("FileSystem::exists"))
    }

    async fn create_dir_all(&self, _path: &Path) -> Result<(), CopyError> {
        Err(CopyError::not_implemented("FileSystem::create_dir_all"))
    }

    async fn read_dir_recursive(&self, _path: &Path) -> Result<Vec<PathBuf>, CopyError> {
        Err(CopyError::not_implemented("FileSystem::read_dir_recursive"))
    }
}

/// Abstract checksum provider.
#[async_trait]
pub trait ChecksumProvider: Send + Sync {
    fn algorithm(&self) -> &'static str;

    async fn checksum_file(&self, _path: &Path) -> Result<String, CopyError> {
        Err(CopyError::not_implemented(
            "ChecksumProvider::checksum_file",
        ))
    }
}
