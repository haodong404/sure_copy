mod common;

use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;

use sure_copy_core::{
    CopyTask, OrchestratorConfig, OverwritePolicy, Pipeline, PipelineKind, PreCopyPipelineMode,
    SqliteTaskOrchestrator, StageExecution, TaskOptions, TaskOrchestrator, TaskState,
    VerificationPolicy,
};

use common::unique_temp_file;

async fn write_file(path: &std::path::Path, content: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .expect("parent dir should be created");
    }
    fs::write(path, content)
        .await
        .expect("file should be written");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_can_submit_and_get_task() {
    let db_path = unique_temp_file("orchestrator_sqlite_submit_get");
    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");

    let task = CopyTask::new(
        "sqlite-task-1",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );

    let submitted = orchestrator.submit(task).await.expect("submit should work");
    let loaded = orchestrator
        .get_task("sqlite-task-1")
        .await
        .expect("get_task should work");

    assert_eq!(submitted.id(), "sqlite-task-1");
    assert_eq!(loaded.id(), "sqlite-task-1");

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_get_tasks_is_empty_for_fresh_database() {
    let db_path = unique_temp_file("orchestrator_sqlite_empty_get_tasks");
    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");

    let tasks = orchestrator
        .get_tasks()
        .await
        .expect("get_tasks should work");

    assert!(tasks.is_empty());

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_persists_state_across_instances() {
    let db_path = unique_temp_file("orchestrator_sqlite_persist");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_persist_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");
    write_file(&src.join("a.txt"), "hello").await;

    {
        let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
            .await
            .expect("sqlite orchestrator should initialize");

        let task = CopyTask::new(
            "sqlite-task-persist",
            src.clone(),
            vec![dst.clone()],
            Default::default(),
        );

        let handle = orchestrator.submit(task).await.expect("submit should work");
        handle.run().await.expect("run should work");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should re-initialize");
    let loaded = orchestrator
        .get_task("sqlite-task-persist")
        .await
        .expect("get_task should restore task from sqlite");

    // Running tasks are auto-recovered to paused on startup.
    assert_eq!(
        loaded.state().await.expect("state should load"),
        TaskState::Completed
    );
    let copied = fs::read_to_string(dst.join("a.txt"))
        .await
        .expect("copied file should exist");
    assert_eq!(copied, "hello");

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_persists_task_options_across_instances() {
    let db_path = unique_temp_file("orchestrator_sqlite_persist_options");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_persist_options_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    let mut options = TaskOptions::default();
    options.overwrite_policy = OverwritePolicy::Rename;
    options.verification_policy = VerificationPolicy::PreAndPostCopy;
    options.retry_policy.max_retries = 7;
    options.retry_policy.initial_backoff_ms = 25;

    {
        let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
            .await
            .expect("sqlite orchestrator should initialize");
        let task = CopyTask::new(
            "sqlite-task-options",
            src.clone(),
            vec![dst.clone()],
            options.clone(),
        );

        orchestrator.submit(task).await.expect("submit should work");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should re-initialize");
    let loaded = orchestrator
        .get_task("sqlite-task-options")
        .await
        .expect("task should restore task from sqlite");

    assert_eq!(loaded.snapshot().options, options);

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_rejects_custom_pipelines_for_durable_tasks() {
    let db_path = unique_temp_file("orchestrator_sqlite_reject_pipeline");
    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");

    let pipeline = Arc::new(Pipeline::new(
        PipelineKind::PreCopy,
        StageExecution::Parallel,
        vec![],
    ));
    let task = CopyTask::new(
        "sqlite-pipeline-task",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        TaskOptions::default(),
    )
    .with_pipelines(
        sure_copy_core::TaskPipelinePlan::new()
            .with_pre_copy_pipeline(pipeline)
            .with_pre_copy_mode(PreCopyPipelineMode::ConcurrentWithCopy),
    );

    let err = match orchestrator.submit(task).await {
        Ok(_) => panic!("durable tasks should reject custom pipelines"),
        Err(err) => err,
    };
    assert_eq!(err.code, "UNSUPPORTED_FEATURE");

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_recovers_planned_tasks_to_paused_on_startup() {
    let db_path = unique_temp_file("orchestrator_sqlite_recover_planned");

    {
        use sqlx::sqlite::SqliteConnectOptions;
        use sqlx::sqlite::SqlitePoolOptions;

        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("db should open");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                source_root TEXT NOT NULL,
                destinations_json TEXT NOT NULL,
                state TEXT NOT NULL,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                complete_bytes INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await
        .expect("schema should exist");

        sqlx::query(
            "INSERT INTO tasks (id, source_root, destinations_json, state, total_bytes, complete_bytes)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("sqlite-planned-recover")
        .bind("/src")
        .bind("[\"/dst\"]")
        .bind("planned")
        .bind(0_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("seed row should be inserted");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize and recover");
    let handle = orchestrator
        .get_task("sqlite-planned-recover")
        .await
        .expect("task should be available after recovery");

    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Paused
    );

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_recovers_running_checkpoints_to_pending_on_startup() {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::Row;

    let db_path = unique_temp_file("orchestrator_sqlite_recover_checkpoint_running");

    {
        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("db should open");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                source_root TEXT NOT NULL,
                destinations_json TEXT NOT NULL,
                state TEXT NOT NULL,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                complete_bytes INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await
        .expect("tasks schema should exist");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_file_checkpoints (
                task_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                status TEXT NOT NULL,
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
        .expect("checkpoint schema should exist");

        sqlx::query(
            "INSERT INTO tasks (id, source_root, destinations_json, state, total_bytes, complete_bytes)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("sqlite-recover-running-checkpoint")
        .bind("/src")
        .bind("[\"/dst\"]")
        .bind("running")
        .bind(0_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("task row should be inserted");

        sqlx::query(
            "INSERT INTO task_file_checkpoints (task_id, source_path, destination_path, status, bytes_copied, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("sqlite-recover-running-checkpoint")
        .bind("/src/a.txt")
        .bind("/dst/a.txt")
        .bind("running")
        .bind(42_i64)
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect("checkpoint row should be inserted");
    }

    let _ = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize and recover checkpoints");

    let connect_options = SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await
        .expect("db should reopen");

    let status: String = sqlx::query(
        "SELECT status FROM task_file_checkpoints WHERE task_id = ? AND source_path = ? AND destination_path = ?",
    )
    .bind("sqlite-recover-running-checkpoint")
    .bind("/src/a.txt")
    .bind("/dst/a.txt")
    .fetch_one(&pool)
    .await
    .expect("checkpoint row should exist")
    .get("status");

    assert_eq!(status, "pending");

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_migrates_legacy_schema_for_new_submissions() {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::Row;

    let db_path = unique_temp_file("orchestrator_sqlite_legacy_migration");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_legacy_migration_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    write_file(&src.join("a.txt"), "hello").await;

    {
        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("db should open");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                source_root TEXT NOT NULL,
                destinations_json TEXT NOT NULL,
                state TEXT NOT NULL,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                complete_bytes INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await
        .expect("legacy tasks schema should exist");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_file_checkpoints (
                task_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                status TEXT NOT NULL,
                bytes_copied INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (task_id, source_path, destination_path)
            )",
        )
        .execute(&pool)
        .await
        .expect("legacy checkpoints schema should exist");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should migrate legacy schema");

    let mut options = TaskOptions::default();
    options.overwrite_policy = OverwritePolicy::Rename;
    options.retry_policy.max_retries = 5;

    let task = CopyTask::new(
        "sqlite-legacy-migration-task",
        src.clone(),
        vec![dst.clone()],
        options.clone(),
    );
    let handle = orchestrator.submit(task).await.expect("submit should work");
    handle.run().await.expect("run should work after migration");

    let connect_options = SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await
        .expect("db should reopen");

    let spec_json: String = sqlx::query("SELECT spec_json FROM tasks WHERE id = ?")
        .bind("sqlite-legacy-migration-task")
        .fetch_one(&pool)
        .await
        .expect("task row should exist")
        .get("spec_json");
    let expected_bytes: i64 =
        sqlx::query("SELECT expected_bytes FROM task_file_checkpoints WHERE task_id = ? LIMIT 1")
            .bind("sqlite-legacy-migration-task")
            .fetch_one(&pool)
            .await
            .expect("checkpoint row should exist")
            .get("expected_bytes");

    assert!(spec_json.contains("\"overwrite_policy\":\"Rename\""));
    assert!(spec_json.contains("\"max_retries\":5"));
    assert_eq!(expected_bytes, 5);

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_rejects_duplicate_id_even_after_restart() {
    let db_path = unique_temp_file("orchestrator_sqlite_duplicate");

    {
        let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
            .await
            .expect("sqlite orchestrator should initialize");
        let task = CopyTask::new(
            "sqlite-task-dup",
            PathBuf::from("/src"),
            vec![PathBuf::from("/dst")],
            Default::default(),
        );
        orchestrator
            .submit(task)
            .await
            .expect("first submit should work");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should re-initialize");
    let duplicate = CopyTask::new(
        "sqlite-task-dup",
        PathBuf::from("/src2"),
        vec![PathBuf::from("/dst2")],
        Default::default(),
    );
    let err = match orchestrator.submit(duplicate).await {
        Ok(_) => panic!("duplicate task id should fail"),
        Err(err) => err,
    };

    assert_eq!(err.code, "TASK_ALREADY_EXISTS");

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_batch_cancel_does_not_override_completed_tasks() {
    let db_path = unique_temp_file("orchestrator_sqlite_cancel_all");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_cancel_all_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");
    write_file(&src.join("a.txt"), "a").await;
    write_file(&src.join("b.txt"), "b").await;

    {
        let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
            .await
            .expect("sqlite orchestrator should initialize");
        let t1 = CopyTask::new(
            "sqlite-cancel-1",
            src.clone(),
            vec![dst.clone()],
            Default::default(),
        );
        let t2 = CopyTask::new(
            "sqlite-cancel-2",
            src.clone(),
            vec![dst.clone()],
            Default::default(),
        );
        let t3 = CopyTask::new(
            "sqlite-cancel-3",
            src.clone(),
            vec![dst.clone()],
            Default::default(),
        );

        let h1 = orchestrator
            .submit(t1)
            .await
            .expect("submit t1 should work");
        let h2 = orchestrator
            .submit(t2)
            .await
            .expect("submit t2 should work");
        orchestrator
            .submit(t3)
            .await
            .expect("submit t3 should work");
        h1.run().await.expect("run t1 should work");
        h2.run().await.expect("run t2 should work");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should re-initialize");
    orchestrator
        .cancel_all()
        .await
        .expect("cancel_all should work");

    let h1 = orchestrator
        .get_task("sqlite-cancel-1")
        .await
        .expect("task 1 should exist");
    let h2 = orchestrator
        .get_task("sqlite-cancel-2")
        .await
        .expect("task 2 should exist");
    let h3 = orchestrator
        .get_task("sqlite-cancel-3")
        .await
        .expect("task 3 should exist");

    assert_eq!(
        h1.state().await.expect("state should work"),
        TaskState::Completed
    );
    assert_eq!(
        h2.state().await.expect("state should work"),
        TaskState::Completed
    );
    assert_eq!(
        h3.state().await.expect("state should work"),
        TaskState::Cancelled
    );

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_cancelled_task_cannot_run_again() {
    let db_path = unique_temp_file("orchestrator_sqlite_cancelled_cannot_run");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_cancelled_cannot_run_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    write_file(&src.join("a.txt"), "hello").await;

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");
    let task = CopyTask::new(
        "sqlite-cancelled-task",
        src.clone(),
        vec![dst.clone()],
        Default::default(),
    );

    let handle = orchestrator.submit(task).await.expect("submit should work");
    handle.cancel().await.expect("cancel should work");

    let err = match handle.run().await {
        Ok(_) => panic!("cancelled task should not run again"),
        Err(err) => err,
    };

    assert_eq!(err.code, "INVALID_STATE_TRANSITION");
    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Cancelled
    );
    assert!(
        fs::metadata(dst.join("a.txt")).await.is_err(),
        "cancelled task should not copy files"
    );

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_returns_not_found_for_missing_task() {
    let db_path = unique_temp_file("orchestrator_sqlite_not_found");
    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");

    let err = match orchestrator.get_task("missing-task").await {
        Ok(_) => panic!("missing task lookup should fail"),
        Err(err) => err,
    };

    assert_eq!(err.code, "TASK_NOT_FOUND");

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_reports_serde_error_for_corrupt_spec_json() {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::sqlite::SqlitePoolOptions;

    let db_path = unique_temp_file("orchestrator_sqlite_corrupt_spec_json");

    {
        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("db should open");

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
        .expect("tasks schema should exist");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_file_checkpoints (
                task_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                status TEXT NOT NULL,
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
        .expect("checkpoint schema should exist");

        sqlx::query(
            "INSERT INTO tasks (id, source_root, destinations_json, spec_json, state, total_bytes, complete_bytes)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("sqlite-corrupt-spec")
        .bind("/src")
        .bind("[\"/dst\"]")
        .bind("{")
        .bind("created")
        .bind(0_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("task row should be inserted");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");
    let err = match orchestrator.get_task("sqlite-corrupt-spec").await {
        Ok(_) => panic!("corrupt spec json should fail to load"),
        Err(err) => err,
    };

    assert_eq!(err.code, "SERDE_ERROR");

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_auto_creates_parent_directory_for_db_file() {
    let base = common::unique_temp_dir("orchestrator_sqlite_parent_dir");
    let db_path = base.join("a").join("b").join("tasks.db");

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize with nested parent path");

    let task = CopyTask::new(
        "sqlite-parent-dir-task",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );
    let _ = orchestrator.submit(task).await.expect("submit should work");

    assert!(db_path.exists());

    let _ = tokio::fs::remove_dir_all(base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_rejects_duplicate_id_in_same_instance() {
    let db_path = unique_temp_file("orchestrator_sqlite_duplicate_same_instance");
    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");

    let task1 = CopyTask::new(
        "sqlite-dup-same",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );
    let task2 = CopyTask::new(
        "sqlite-dup-same",
        PathBuf::from("/src2"),
        vec![PathBuf::from("/dst2")],
        Default::default(),
    );

    orchestrator
        .submit(task1)
        .await
        .expect("first submit should work");

    let err = match orchestrator.submit(task2).await {
        Ok(_) => panic!("duplicate task id should fail"),
        Err(err) => err,
    };

    assert_eq!(err.code, "TASK_ALREADY_EXISTS");

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_run_copies_files_to_destination() {
    let db_path = unique_temp_file("orchestrator_sqlite_copy_workload");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_copy_workload_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    write_file(&src.join("folder").join("x.txt"), "x-content").await;
    write_file(&src.join("folder").join("y.txt"), "y-content").await;

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");
    let task = CopyTask::new(
        "sqlite-copy-task",
        src.clone(),
        vec![dst.clone()],
        Default::default(),
    );

    let handle = orchestrator.submit(task).await.expect("submit should work");
    handle.run().await.expect("run should copy files");

    let x = fs::read_to_string(dst.join("folder").join("x.txt"))
        .await
        .expect("x should be copied");
    let y = fs::read_to_string(dst.join("folder").join("y.txt"))
        .await
        .expect("y should be copied");

    assert_eq!(x, "x-content");
    assert_eq!(y, "y-content");
    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_run_handles_empty_source_tree() {
    let db_path = unique_temp_file("orchestrator_sqlite_empty_source_tree");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_empty_source_tree_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    fs::create_dir_all(&src)
        .await
        .expect("empty source dir should be created");

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");
    let task = CopyTask::new(
        "sqlite-empty-source-task",
        src.clone(),
        vec![dst.clone()],
        Default::default(),
    );

    let handle = orchestrator.submit(task).await.expect("submit should work");
    handle
        .run()
        .await
        .expect("run should succeed even when source tree is empty");

    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert_eq!(
        handle.progress().await.expect("progress should load"),
        sure_copy_core::TaskProgress::default()
    );
    assert!(
        fs::metadata(&dst).await.is_err(),
        "empty source tree should not materialize destination directories"
    );

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_resume_skips_completed_file_checkpoints() {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::sqlite::SqlitePoolOptions;

    let db_path = unique_temp_file("orchestrator_sqlite_resume_file_checkpoint");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_resume_file_checkpoint_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    write_file(&src.join("done.txt"), "fresh-done").await;
    write_file(&src.join("todo.txt"), "fresh-todo").await;
    write_file(&dst.join("done.txt"), "already-done").await;

    {
        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("db should open");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                source_root TEXT NOT NULL,
                destinations_json TEXT NOT NULL,
                state TEXT NOT NULL,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                complete_bytes INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await
        .expect("tasks table should be created");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_file_checkpoints (
                task_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                status TEXT NOT NULL,
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
        .expect("checkpoint table should be created");

        sqlx::query(
            "INSERT INTO tasks (id, source_root, destinations_json, state, total_bytes, complete_bytes)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("resume-task")
        .bind(src.to_string_lossy().to_string())
        .bind(format!("[\"{}\"]", dst.to_string_lossy()))
        .bind("running")
        .bind(0_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("task row should be seeded");

        sqlx::query(
            "INSERT INTO task_file_checkpoints (task_id, source_path, destination_path, status, bytes_copied, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("resume-task")
        .bind(src.join("done.txt").to_string_lossy().to_string())
        .bind(dst.join("done.txt").to_string_lossy().to_string())
        .bind("completed")
        .bind(11_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("done checkpoint should be seeded");

        sqlx::query(
            "INSERT INTO task_file_checkpoints (task_id, source_path, destination_path, status, bytes_copied, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("resume-task")
        .bind(src.join("todo.txt").to_string_lossy().to_string())
        .bind(dst.join("todo.txt").to_string_lossy().to_string())
        .bind("pending")
        .bind(0_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("todo checkpoint should be seeded");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should recover");
    let handle = orchestrator
        .get_task("resume-task")
        .await
        .expect("task should load");

    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Paused
    );

    handle
        .resume()
        .await
        .expect("resume should run pending file");

    let done = fs::read_to_string(dst.join("done.txt"))
        .await
        .expect("done file should exist");
    let todo = fs::read_to_string(dst.join("todo.txt"))
        .await
        .expect("todo file should exist");

    assert_eq!(done, "already-done");
    assert_eq!(todo, "fresh-todo");

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_resume_recomputes_progress_from_expected_bytes() {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::sqlite::SqlitePoolOptions;

    let db_path = unique_temp_file("orchestrator_sqlite_resume_progress");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_resume_progress_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    write_file(&src.join("done.txt"), "hello").await;
    write_file(&src.join("todo.txt"), "world!!!").await;
    write_file(&dst.join("done.txt"), "hello").await;

    {
        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("db should open");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                source_root TEXT NOT NULL,
                destinations_json TEXT NOT NULL,
                state TEXT NOT NULL,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                complete_bytes INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await
        .expect("tasks table should be created");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_file_checkpoints (
                task_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                status TEXT NOT NULL,
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
        .expect("checkpoint table should be created");

        sqlx::query(
            "INSERT INTO tasks (id, source_root, destinations_json, state, total_bytes, complete_bytes)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("resume-progress-task")
        .bind(src.to_string_lossy().to_string())
        .bind(format!("[\"{}\"]", dst.to_string_lossy()))
        .bind("paused")
        .bind(0_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("task row should be seeded");

        sqlx::query(
            "INSERT INTO task_file_checkpoints (task_id, source_path, destination_path, status, bytes_copied, expected_bytes, actual_destination_path, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("resume-progress-task")
        .bind(src.join("done.txt").to_string_lossy().to_string())
        .bind(dst.join("done.txt").to_string_lossy().to_string())
        .bind("completed")
        .bind(5_i64)
        .bind(5_i64)
        .bind(dst.join("done.txt").to_string_lossy().to_string())
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("done checkpoint should be seeded");

        sqlx::query(
            "INSERT INTO task_file_checkpoints (task_id, source_path, destination_path, status, bytes_copied, expected_bytes, actual_destination_path, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("resume-progress-task")
        .bind(src.join("todo.txt").to_string_lossy().to_string())
        .bind(dst.join("todo.txt").to_string_lossy().to_string())
        .bind("pending")
        .bind(0_i64)
        .bind(8_i64)
        .bind(Option::<String>::None)
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("todo checkpoint should be seeded");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should recover");
    let handle = orchestrator
        .get_task("resume-progress-task")
        .await
        .expect("task should load");

    handle
        .resume()
        .await
        .expect("resume should finish pending work");

    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert_eq!(
        handle.progress().await.expect("progress should load"),
        sure_copy_core::TaskProgress {
            total_bytes: 13,
            complete_bytes: 13,
        }
    );

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_overwrite_policy_skip_keeps_existing_destination_file() {
    let db_path = unique_temp_file("orchestrator_sqlite_overwrite_skip");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_overwrite_skip_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    write_file(&src.join("data.txt"), "new-content").await;
    write_file(&dst.join("data.txt"), "old-content").await;

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");

    let mut options = TaskOptions::default();
    options.overwrite_policy = OverwritePolicy::Skip;

    let task = CopyTask::new(
        "sqlite-overwrite-skip-task",
        src.clone(),
        vec![dst.clone()],
        options,
    );

    let handle = orchestrator.submit(task).await.expect("submit should work");
    handle
        .run()
        .await
        .expect("run should complete with skip policy");

    let content = fs::read_to_string(dst.join("data.txt"))
        .await
        .expect("destination file should exist");
    assert_eq!(content, "old-content");
    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_overwrite_policy_rename_creates_new_name() {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::Row;

    let db_path = unique_temp_file("orchestrator_sqlite_overwrite_rename");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_overwrite_rename_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    write_file(&src.join("data.txt"), "fresh").await;
    write_file(&dst.join("data.txt"), "existing").await;

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");

    let mut options = TaskOptions::default();
    options.overwrite_policy = OverwritePolicy::Rename;

    let task = CopyTask::new(
        "sqlite-overwrite-rename-task",
        src.clone(),
        vec![dst.clone()],
        options,
    );

    let handle = orchestrator.submit(task).await.expect("submit should work");
    handle
        .run()
        .await
        .expect("run should complete with rename policy");

    let original = fs::read_to_string(dst.join("data.txt"))
        .await
        .expect("original destination should exist");
    let renamed = fs::read_to_string(dst.join("data (1).txt"))
        .await
        .expect("renamed destination should exist");

    let connect_options = SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await
        .expect("db should reopen");
    let actual_destination: Option<String> = sqlx::query(
        "SELECT actual_destination_path FROM task_file_checkpoints WHERE task_id = ? AND source_path = ? AND destination_path = ?",
    )
    .bind("sqlite-overwrite-rename-task")
    .bind(src.join("data.txt").to_string_lossy().to_string())
    .bind(dst.join("data.txt").to_string_lossy().to_string())
    .fetch_one(&pool)
    .await
    .expect("checkpoint row should exist")
    .get("actual_destination_path");

    assert_eq!(original, "existing");
    assert_eq!(renamed, "fresh");
    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );
    assert_eq!(
        actual_destination,
        Some(dst.join("data (1).txt").to_string_lossy().to_string())
    );

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_run_fails_on_invalid_checkpoint_status() {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::sqlite::SqlitePoolOptions;

    let db_path = unique_temp_file("orchestrator_sqlite_invalid_checkpoint_status");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_invalid_checkpoint_status_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    write_file(&src.join("a.txt"), "A").await;

    {
        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("db should open");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                source_root TEXT NOT NULL,
                destinations_json TEXT NOT NULL,
                state TEXT NOT NULL,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                complete_bytes INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await
        .expect("tasks table should exist");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_file_checkpoints (
                task_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                status TEXT NOT NULL,
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
        .expect("checkpoint table should exist");

        sqlx::query(
            "INSERT INTO tasks (id, source_root, destinations_json, state, total_bytes, complete_bytes)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("sqlite-invalid-checkpoint-status-task")
        .bind(src.to_string_lossy().to_string())
        .bind(format!("[\"{}\"]", dst.to_string_lossy()))
        .bind("paused")
        .bind(0_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("task row should be seeded");

        sqlx::query(
            "INSERT INTO task_file_checkpoints (task_id, source_path, destination_path, status, bytes_copied, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("sqlite-invalid-checkpoint-status-task")
        .bind(src.join("a.txt").to_string_lossy().to_string())
        .bind(dst.join("a.txt").to_string_lossy().to_string())
        .bind("corrupted-status")
        .bind(0_i64)
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect("invalid checkpoint should be seeded");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("orchestrator should initialize");
    let handle = orchestrator
        .get_task("sqlite-invalid-checkpoint-status-task")
        .await
        .expect("task should load");

    let err = match handle.run().await {
        Ok(_) => panic!("run should fail on invalid checkpoint status"),
        Err(err) => err,
    };

    assert_eq!(err.code, "UNKNOWN_FILE_CHECKPOINT_STATUS");
    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Failed
    );

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_overwrite_policy_newer_only_skips_when_destination_newer() {
    let db_path = unique_temp_file("orchestrator_sqlite_overwrite_newer_only_skip");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_overwrite_newer_only_skip_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    write_file(&src.join("data.txt"), "source-old").await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    write_file(&dst.join("data.txt"), "destination-new").await;

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");

    let mut options = TaskOptions::default();
    options.overwrite_policy = OverwritePolicy::NewerOnly;

    let task = CopyTask::new(
        "sqlite-overwrite-newer-only-skip-task",
        src.clone(),
        vec![dst.clone()],
        options,
    );

    let handle = orchestrator.submit(task).await.expect("submit should work");
    handle
        .run()
        .await
        .expect("run should complete with newer-only policy");

    let content = fs::read_to_string(dst.join("data.txt"))
        .await
        .expect("destination should exist");
    assert_eq!(content, "destination-new");
    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_overwrite_policy_newer_only_overwrites_when_source_newer() {
    let db_path = unique_temp_file("orchestrator_sqlite_overwrite_newer_only_copy");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_overwrite_newer_only_copy_fixture");
    let src = fixture.join("src");
    let dst = fixture.join("dst");

    write_file(&dst.join("data.txt"), "destination-old").await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    write_file(&src.join("data.txt"), "source-new").await;

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("sqlite orchestrator should initialize");

    let mut options = TaskOptions::default();
    options.overwrite_policy = OverwritePolicy::NewerOnly;

    let task = CopyTask::new(
        "sqlite-overwrite-newer-only-copy-task",
        src.clone(),
        vec![dst.clone()],
        options,
    );

    let handle = orchestrator.submit(task).await.expect("submit should work");
    handle
        .run()
        .await
        .expect("run should complete with newer-only policy");

    let content = fs::read_to_string(dst.join("data.txt"))
        .await
        .expect("destination should exist");
    assert_eq!(content, "source-new");
    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Completed
    );

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_orchestrator_marks_checkpoint_failed_when_copy_errors() {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::Row;

    let db_path = unique_temp_file("orchestrator_sqlite_failed_checkpoint_update");
    let fixture = common::unique_temp_dir("orchestrator_sqlite_failed_checkpoint_update_fixture");
    let dst = fixture.join("dst");
    fs::create_dir_all(&dst)
        .await
        .expect("dst dir should exist");

    let missing_source = fixture.join("src").join("missing.txt");
    let destination_file = dst.join("missing.txt");

    {
        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("db should open");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                source_root TEXT NOT NULL,
                destinations_json TEXT NOT NULL,
                state TEXT NOT NULL,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                complete_bytes INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await
        .expect("tasks table should exist");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_file_checkpoints (
                task_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                status TEXT NOT NULL,
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
        .expect("checkpoint table should exist");

        sqlx::query(
            "INSERT INTO tasks (id, source_root, destinations_json, state, total_bytes, complete_bytes)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("sqlite-failed-checkpoint-task")
        .bind(fixture.to_string_lossy().to_string())
        .bind(format!("[\"{}\"]", dst.to_string_lossy()))
        .bind("paused")
        .bind(0_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .expect("task row should be seeded");

        sqlx::query(
            "INSERT INTO task_file_checkpoints (task_id, source_path, destination_path, status, bytes_copied, expected_bytes, actual_destination_path, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("sqlite-failed-checkpoint-task")
        .bind(missing_source.to_string_lossy().to_string())
        .bind(destination_file.to_string_lossy().to_string())
        .bind("pending")
        .bind(0_i64)
        .bind(128_i64)
        .bind(Option::<String>::None)
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect("checkpoint row should be seeded");
    }

    let orchestrator = SqliteTaskOrchestrator::new(&db_path, OrchestratorConfig::default())
        .await
        .expect("orchestrator should initialize");
    let handle = orchestrator
        .get_task("sqlite-failed-checkpoint-task")
        .await
        .expect("task should load");

    let err = match handle.run().await {
        Ok(_) => panic!("run should fail when source file is missing"),
        Err(err) => err,
    };
    assert_eq!(err.code, "FILE_COPY_FAILED");
    assert_eq!(
        handle.state().await.expect("state should load"),
        TaskState::Failed
    );
    assert_eq!(
        handle.progress().await.expect("progress should load"),
        sure_copy_core::TaskProgress {
            total_bytes: 128,
            complete_bytes: 0,
        }
    );

    let connect_options = SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await
        .expect("db should reopen");

    let row = sqlx::query(
        "SELECT status, last_error FROM task_file_checkpoints WHERE task_id = ? AND source_path = ? AND destination_path = ?",
    )
    .bind("sqlite-failed-checkpoint-task")
    .bind(missing_source.to_string_lossy().to_string())
    .bind(destination_file.to_string_lossy().to_string())
    .fetch_one(&pool)
    .await
    .expect("failed checkpoint row should exist");

    let status: String = row.get("status");
    let last_error: Option<String> = row.get("last_error");

    assert_eq!(status, "failed");
    assert!(last_error.is_some());

    let _ = tokio::fs::remove_file(&db_path).await;
    let _ = tokio::fs::remove_dir_all(fixture).await;
}
