use std::path::Path;

use sure_copy_core::infrastructure::ChecksumProvider;

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
