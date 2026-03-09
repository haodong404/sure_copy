use std::path::Path;

use async_trait::async_trait;
use sure_copy_core::infrastructure::ChecksumProvider;
use sure_copy_core::infrastructure::FileSystem;

struct StubChecksum;

impl ChecksumProvider for StubChecksum {
    fn algorithm(&self) -> &'static str {
        "SHA-256"
    }
}

struct StubFs;

#[async_trait]
impl FileSystem for StubFs {}

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

#[test]
fn checksum_provider_default_begin_stream_is_not_implemented() {
    let provider = StubChecksum;
    let err = match provider.begin_stream() {
        Ok(_) => panic!("begin_stream should be not implemented by default"),
        Err(err) => err,
    };

    assert_eq!(err.code, "NOT_IMPLEMENTED");
    assert!(err.message.contains("ChecksumProvider::begin_stream"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filesystem_default_methods_are_not_implemented() {
    let fs = StubFs;

    let exists_err = fs
        .exists(Path::new("/tmp"))
        .await
        .expect_err("exists should be not implemented");
    assert_eq!(exists_err.code, "NOT_IMPLEMENTED");
    assert!(exists_err.message.contains("FileSystem::exists"));

    let mkdir_err = fs
        .create_dir_all(Path::new("/tmp/test"))
        .await
        .expect_err("create_dir_all should be not implemented");
    assert_eq!(mkdir_err.code, "NOT_IMPLEMENTED");
    assert!(mkdir_err.message.contains("FileSystem::create_dir_all"));

    let walk_err = fs
        .read_dir_recursive(Path::new("/tmp/test"))
        .await
        .expect_err("read_dir_recursive should be not implemented");
    assert_eq!(walk_err.code, "NOT_IMPLEMENTED");
    assert!(walk_err.message.contains("FileSystem::read_dir_recursive"));
}
