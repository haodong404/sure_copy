use std::path::{Path, PathBuf};
use std::{io, io::ErrorKind};

use async_trait::async_trait;
use log::{debug, warn};

use crate::domain::{CopyError, CopyErrorCategory};
use crate::infrastructure::FileSystem;

/// Native local filesystem implementation backed by Tokio.
#[derive(Debug, Default, Clone, Copy)]
pub struct LocalFileSystem;

impl LocalFileSystem {
    pub fn new() -> Self {
        Self
    }

    fn map_io_error(err: io::Error, operation: &'static str, path: &Path) -> CopyError {
        let (category, code) = match err.kind() {
            ErrorKind::NotFound => (CopyErrorCategory::Path, "PATH_NOT_FOUND"),
            ErrorKind::PermissionDenied => (CopyErrorCategory::Permission, "PERMISSION_DENIED"),
            _ => (CopyErrorCategory::Io, "IO_ERROR"),
        };

        warn!(
            "filesystem operation '{}' failed for '{}': {}",
            operation,
            path.display(),
            err
        );

        CopyError {
            category,
            code,
            message: format!(
                "filesystem operation '{}' failed for '{}': {}",
                operation,
                path.display(),
                err
            ),
        }
    }
}

#[async_trait]
impl FileSystem for LocalFileSystem {
    async fn exists(&self, path: &Path) -> Result<bool, CopyError> {
        match tokio::fs::metadata(path).await {
            Ok(_) => Ok(true),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
            Err(err) => Err(Self::map_io_error(err, "exists", path)),
        }
    }

    async fn create_dir_all(&self, path: &Path) -> Result<(), CopyError> {
        debug!("creating directory tree '{}'", path.display());
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|err| Self::map_io_error(err, "create_dir_all", path))
    }

    async fn read_dir_recursive(&self, path: &Path) -> Result<Vec<PathBuf>, CopyError> {
        debug!("walking directory tree '{}'", path.display());
        let mut files = Vec::new();
        let mut pending = vec![path.to_path_buf()];

        while let Some(current_dir) = pending.pop() {
            let mut entries = tokio::fs::read_dir(&current_dir)
                .await
                .map_err(|err| Self::map_io_error(err, "read_dir", &current_dir))?;

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|err| Self::map_io_error(err, "read_dir_next", &current_dir))?
            {
                let entry_path = entry.path();
                let ty = entry
                    .file_type()
                    .await
                    .map_err(|err| Self::map_io_error(err, "file_type", &entry_path))?;

                if ty.is_dir() {
                    pending.push(entry_path);
                } else if ty.is_file() {
                    files.push(entry_path);
                }
            }
        }

        files.sort();
        debug!(
            "directory walk for '{}' discovered {} files",
            path.display(),
            files.len()
        );
        Ok(files)
    }
}
