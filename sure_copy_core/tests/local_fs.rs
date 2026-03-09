mod common;

use sure_copy_core::infrastructure::{FileSystem, LocalFileSystem};
use sure_copy_core::CopyErrorCategory;

use common::{init_test_logging, unique_temp_dir};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_fs_exists_returns_expected_value() {
    init_test_logging();
    let adapter = LocalFileSystem::new();
    let dir = unique_temp_dir("exists");
    let file = dir.join("present.txt");

    tokio::fs::create_dir_all(&dir)
        .await
        .expect("temp dir should be created");
    tokio::fs::write(&file, b"hello")
        .await
        .expect("temp file should be written");

    let exists = adapter.exists(&file).await.expect("exists should succeed");
    let missing = adapter
        .exists(&dir.join("missing.txt"))
        .await
        .expect("exists should return false for missing path");

    assert!(exists);
    assert!(!missing);

    tokio::fs::remove_dir_all(&dir)
        .await
        .expect("temp dir should be removed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_fs_can_create_nested_dirs_and_walk_files() {
    init_test_logging();
    let adapter = LocalFileSystem::new();
    let root = unique_temp_dir("walk");
    let nested = root.join("a/b/c");
    let file_a = root.join("root.txt");
    let file_b = nested.join("nested.txt");

    adapter
        .create_dir_all(&nested)
        .await
        .expect("nested directories should be created");
    tokio::fs::write(&file_a, b"root")
        .await
        .expect("root file should be written");
    tokio::fs::write(&file_b, b"nested")
        .await
        .expect("nested file should be written");

    let mut files = adapter
        .read_dir_recursive(&root)
        .await
        .expect("recursive read should succeed");
    files.sort();

    let mut expected = vec![file_a, file_b];
    expected.sort();

    assert_eq!(files, expected);

    tokio::fs::remove_dir_all(&root)
        .await
        .expect("temp dir should be removed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_fs_read_dir_recursive_returns_path_error_for_missing_root() {
    init_test_logging();
    let adapter = LocalFileSystem::new();
    let missing_root = unique_temp_dir("missing_root");

    let err = adapter
        .read_dir_recursive(&missing_root)
        .await
        .expect_err("missing root should return error");

    assert_eq!(err.category, CopyErrorCategory::Path);
    assert_eq!(err.code, "PATH_NOT_FOUND");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_fs_read_dir_recursive_returns_empty_for_empty_root() {
    init_test_logging();
    let adapter = LocalFileSystem::new();
    let root = unique_temp_dir("empty_walk");

    tokio::fs::create_dir_all(&root)
        .await
        .expect("temp dir should be created");

    let files = adapter
        .read_dir_recursive(&root)
        .await
        .expect("recursive read should succeed for empty directory");

    assert!(files.is_empty());

    tokio::fs::remove_dir_all(&root)
        .await
        .expect("temp dir should be removed");
}
