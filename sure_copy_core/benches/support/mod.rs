#![allow(dead_code)]

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::{Builder, TempDir};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

pub fn build_runtime() -> Runtime {
    RuntimeBuilder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime should build")
}

pub fn temp_dir(prefix: &str) -> TempDir {
    Builder::new()
        .prefix(&format!("sure_copy_core_bench_{prefix}_"))
        .tempdir()
        .expect("temporary benchmark directory should be created")
}

pub fn create_file(path: &Path, size_bytes: usize) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("parent directory should be created");
    }

    let mut file = File::create(path).expect("benchmark file should be created");
    let chunk = [0x5Au8; 8192];
    let mut remaining = size_bytes;

    while remaining > 0 {
        let write_len = remaining.min(chunk.len());
        file.write_all(&chunk[..write_len])
            .expect("benchmark file should be written");
        remaining -= write_len;
    }

    file.sync_all().expect("benchmark file should be flushed");
}

pub fn create_file_tree(root: &Path, file_count: usize, file_size_bytes: usize) -> u64 {
    fs::create_dir_all(root).expect("benchmark tree root should be created");

    for index in 0..file_count {
        let bucket = index % 32;
        let shard = (index / 32) % 32;
        let path = root
            .join(format!("bucket-{bucket:02}"))
            .join(format!("shard-{shard:02}"))
            .join(format!("file-{index:05}.bin"));
        create_file(&path, file_size_bytes);
    }

    (file_count as u64) * (file_size_bytes as u64)
}

pub async fn seed_recovery_database(
    db_path: &Path,
    task_count: usize,
    checkpoints_per_task: usize,
) {
    let connect_options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await
        .expect("benchmark database should open");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            source_root TEXT NOT NULL,
            destinations_json TEXT NOT NULL,
            spec_json TEXT NOT NULL DEFAULT '{}',
            state TEXT NOT NULL,
            total_bytes INTEGER NOT NULL DEFAULT 0,
            complete_bytes INTEGER NOT NULL DEFAULT 0
        )",
    )
    .execute(&pool)
    .await
    .expect("benchmark tasks schema should exist");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS task_file_checkpoints (
            task_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            destination_path TEXT NOT NULL,
            status TEXT NOT NULL,
            copy_status TEXT NOT NULL DEFAULT 'pending',
            post_write_status TEXT NOT NULL DEFAULT 'pending',
            bytes_copied INTEGER NOT NULL DEFAULT 0,
            expected_bytes INTEGER NOT NULL DEFAULT 0,
            actual_destination_path TEXT,
            last_error TEXT,
            updated_at_ms INTEGER NOT NULL,
            PRIMARY KEY (task_id, source_path, destination_path)
        )",
    )
    .execute(&pool)
    .await
    .expect("benchmark checkpoints schema should exist");

    let mut tx = pool
        .begin()
        .await
        .expect("benchmark transaction should begin");

    for task_index in 0..task_count {
        let task_id = format!("recovery-task-{task_index:05}");
        sqlx::query(
            "INSERT INTO tasks (id, source_root, destinations_json, spec_json, state, total_bytes, complete_bytes)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&task_id)
        .bind("/bench/src")
        .bind("[\"/bench/dst\"]")
        .bind("{\"source_root\":\"/bench/src\",\"destinations\":[\"/bench/dst\"],\"options\":{\"overwrite_policy\":\"Skip\",\"verification_policy\":\"PostWrite\",\"retry_policy\":{\"max_retries\":3,\"initial_backoff_ms\":200,\"exponential_factor\":2},\"max_concurrency\":4,\"per_file_pipeline_parallelism\":2,\"checksum_parallelism\":2,\"buffer_size_bytes\":1048576,\"preserve_timestamps\":true,\"preserve_permissions\":true,\"include_patterns\":[],\"exclude_patterns\":[]},\"source_pipeline_mode\":\"ConcurrentWithFanOut\",\"post_write_pipeline_mode\":\"SerialAfterWrite\"}")
        .bind(if task_index % 2 == 0 { "running" } else { "planned" })
        .bind(0_i64)
        .bind(0_i64)
        .execute(&mut *tx)
        .await
        .expect("benchmark task row should be inserted");

        for checkpoint_index in 0..checkpoints_per_task {
            let source_path = format!("/bench/src/file-{task_index:05}-{checkpoint_index:05}.bin");
            let destination_path =
                format!("/bench/dst/file-{task_index:05}-{checkpoint_index:05}.bin");
            sqlx::query(
                "INSERT INTO task_file_checkpoints (task_id, source_path, destination_path, status, copy_status, post_write_status, bytes_copied, expected_bytes, actual_destination_path, last_error, updated_at_ms)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&task_id)
            .bind(&source_path)
            .bind(&destination_path)
            .bind("writing")
            .bind("pending")
            .bind("pending")
            .bind(0_i64)
            .bind(4096_i64)
            .bind(Option::<String>::None)
            .bind(Option::<String>::None)
            .bind(0_i64)
            .execute(&mut *tx)
            .await
            .expect("benchmark checkpoint row should be inserted");
        }
    }

    tx.commit()
        .await
        .expect("benchmark transaction should commit");
    pool.close().await;
}

pub fn file_size_label(size_bytes: usize) -> String {
    if size_bytes >= 1024 * 1024 {
        format!("{}mb", size_bytes / (1024 * 1024))
    } else if size_bytes >= 1024 {
        format!("{}kb", size_bytes / 1024)
    } else {
        format!("{size_bytes}b")
    }
}

pub fn join_path(base: &Path, child: &str) -> PathBuf {
    base.join(child)
}
