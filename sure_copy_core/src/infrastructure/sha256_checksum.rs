use std::path::Path;
use std::{io, io::ErrorKind};

use async_trait::async_trait;
use log::{debug, warn};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::domain::{CopyError, CopyErrorCategory};
use crate::infrastructure::ChecksumProvider;

/// SHA-256 checksum provider backed by streaming Tokio file reads.
#[derive(Debug, Default, Clone, Copy)]
pub struct Sha256ChecksumProvider;

impl Sha256ChecksumProvider {
    pub fn new() -> Self {
        Self
    }

    fn map_io_error(err: io::Error, operation: &'static str, path: &Path) -> CopyError {
        let (category, code) = match err.kind() {
            ErrorKind::NotFound => (CopyErrorCategory::Path, "PATH_NOT_FOUND"),
            ErrorKind::PermissionDenied => (CopyErrorCategory::Permission, "PERMISSION_DENIED"),
            _ => (CopyErrorCategory::Checksum, "CHECKSUM_IO_ERROR"),
        };

        warn!(
            "checksum operation '{}' failed for '{}': {}",
            operation,
            path.display(),
            err
        );

        CopyError {
            category,
            code,
            message: format!(
                "checksum operation '{}' failed for '{}': {}",
                operation,
                path.display(),
                err
            ),
        }
    }
}

#[async_trait]
impl ChecksumProvider for Sha256ChecksumProvider {
    fn algorithm(&self) -> &'static str {
        "SHA-256"
    }

    async fn checksum_file(&self, path: &Path) -> Result<String, CopyError> {
        debug!("calculating SHA-256 for '{}'", path.display());
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|err| Self::map_io_error(err, "open", path))?;

        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 1024 * 64];

        loop {
            let read_len = file
                .read(&mut buffer)
                .await
                .map_err(|err| Self::map_io_error(err, "read", path))?;
            if read_len == 0 {
                break;
            }
            hasher.update(&buffer[..read_len]);
        }

        let digest = format!("{:x}", hasher.finalize());
        debug!("calculated SHA-256 for '{}'", path.display());
        Ok(digest)
    }
}
