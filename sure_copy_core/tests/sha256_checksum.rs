mod common;

use sure_copy_core::infrastructure::{ChecksumProvider, Sha256ChecksumProvider};
use sure_copy_core::CopyErrorCategory;

use common::{init_test_logging, unique_temp_file};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sha256_checksum_provider_returns_known_digest() {
    init_test_logging();
    let provider = Sha256ChecksumProvider::new();
    let file = unique_temp_file("sha256");
    tokio::fs::write(&file, b"abc")
        .await
        .expect("test file should be written");

    let digest = provider
        .checksum_file(&file)
        .await
        .expect("checksum should succeed");

    assert_eq!(provider.algorithm(), "SHA-256");
    assert_eq!(
        digest,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );

    tokio::fs::remove_file(&file)
        .await
        .expect("test file should be removed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sha256_checksum_provider_returns_path_error_for_missing_file() {
    init_test_logging();
    let provider = Sha256ChecksumProvider::new();
    let missing_file = unique_temp_file("sha256_missing");

    let err = provider
        .checksum_file(&missing_file)
        .await
        .expect_err("missing file should fail");

    assert_eq!(err.category, CopyErrorCategory::Path);
    assert_eq!(err.code, "PATH_NOT_FOUND");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sha256_checksum_provider_returns_empty_file_digest() {
    init_test_logging();
    let provider = Sha256ChecksumProvider::new();
    let file = unique_temp_file("sha256_empty");

    tokio::fs::write(&file, b"")
        .await
        .expect("empty test file should be written");

    let digest = provider
        .checksum_file(&file)
        .await
        .expect("checksum should succeed for empty file");

    assert_eq!(
        digest,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );

    tokio::fs::remove_file(&file)
        .await
        .expect("test file should be removed");
}
