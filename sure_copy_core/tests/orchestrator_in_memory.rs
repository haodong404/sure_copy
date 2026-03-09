mod common;

use std::path::PathBuf;

use sure_copy_core::{CopyTask, InMemoryTaskOrchestrator, TaskOrchestrator, TaskState};

use common::init_test_logging;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_orchestrator_submit_and_get_task() {
    init_test_logging();
    let orchestrator = InMemoryTaskOrchestrator::default();
    let task = CopyTask::new(
        "task-a",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );

    let submitted = orchestrator.submit(task).await.expect("submit should work");
    let loaded = orchestrator
        .get_task("task-a")
        .await
        .expect("get_task should work");

    assert_eq!(submitted.id(), "task-a");
    assert_eq!(loaded.id(), "task-a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_orchestrator_rejects_duplicate_task_id() {
    init_test_logging();
    let orchestrator = InMemoryTaskOrchestrator::default();
    let task1 = CopyTask::new(
        "task-dup",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );
    let task2 = CopyTask::new(
        "task-dup",
        PathBuf::from("/src2"),
        vec![PathBuf::from("/dst2")],
        Default::default(),
    );

    orchestrator
        .submit(task1)
        .await
        .expect("first submit should work");
    let err = match orchestrator.submit(task2).await {
        Ok(_) => panic!("duplicate submit should fail"),
        Err(err) => err,
    };

    assert_eq!(err.code, "TASK_ALREADY_EXISTS");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_orchestrator_returns_not_found_for_unknown_task() {
    init_test_logging();
    let orchestrator = InMemoryTaskOrchestrator::default();
    let err = match orchestrator.get_task("task-missing").await {
        Ok(_) => panic!("unknown task should fail"),
        Err(err) => err,
    };

    assert_eq!(err.code, "TASK_NOT_FOUND");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_orchestrator_get_tasks_returns_all_handles() {
    init_test_logging();
    let orchestrator = InMemoryTaskOrchestrator::default();
    let task1 = CopyTask::new(
        "task-l1",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );
    let task2 = CopyTask::new(
        "task-l2",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );

    orchestrator
        .submit(task1)
        .await
        .expect("submit task1 should work");
    orchestrator
        .submit(task2)
        .await
        .expect("submit task2 should work");

    let handles = orchestrator
        .get_tasks()
        .await
        .expect("get_tasks should work");
    assert_eq!(handles.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_orchestrator_batch_pause_resume_cancel() {
    init_test_logging();
    let orchestrator = InMemoryTaskOrchestrator::default();
    let task1 = CopyTask::new(
        "task-b1",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );
    let task2 = CopyTask::new(
        "task-b2",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );

    let h1 = orchestrator
        .submit(task1)
        .await
        .expect("submit task1 should work");
    let h2 = orchestrator
        .submit(task2)
        .await
        .expect("submit task2 should work");

    h1.run().await.expect("task1 run should work");
    h2.run().await.expect("task2 run should work");

    orchestrator
        .pause_all()
        .await
        .expect("pause_all should work");
    assert_eq!(
        h1.state().await.expect("state should work"),
        TaskState::Paused
    );
    assert_eq!(
        h2.state().await.expect("state should work"),
        TaskState::Paused
    );

    orchestrator
        .resume_all()
        .await
        .expect("resume_all should work");
    assert_eq!(
        h1.state().await.expect("state should work"),
        TaskState::Running
    );
    assert_eq!(
        h2.state().await.expect("state should work"),
        TaskState::Running
    );

    orchestrator
        .cancel_all()
        .await
        .expect("cancel_all should work");
    assert_eq!(
        h1.state().await.expect("state should work"),
        TaskState::Cancelled
    );
    assert_eq!(
        h2.state().await.expect("state should work"),
        TaskState::Cancelled
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_orchestrator_get_tasks_is_empty_by_default() {
    init_test_logging();
    let orchestrator = InMemoryTaskOrchestrator::default();

    let handles = orchestrator
        .get_tasks()
        .await
        .expect("get_tasks should work");

    assert!(handles.is_empty());
}
