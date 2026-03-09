mod common;

use std::path::PathBuf;

use sure_copy_core::{CopyTask, InMemoryTask, Task, TaskProgress, TaskState, TaskUpdate};
use tokio::sync::broadcast::error::RecvError;

use common::init_test_logging;

fn build_task(id: &str) -> CopyTask {
    init_test_logging();
    CopyTask::new(
        id,
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    )
}

#[test]
fn in_memory_task_exposes_id_and_snapshot() {
    init_test_logging();
    let task = build_task("managed-001").with_state(TaskState::Created);
    let handle = InMemoryTask::new(task.clone(), 8);

    assert_eq!(handle.id(), "managed-001");
    assert_eq!(handle.snapshot().id, "managed-001");
    assert_eq!(handle.snapshot().state, TaskState::Created);
    assert_eq!(handle.snapshot().source_root, task.source_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_pause_resume_cancel_follow_state_machine() {
    let handle = InMemoryTask::new(build_task("managed-002"), 8);

    handle.run().await.expect("run should work");
    assert_eq!(
        handle.state().await.expect("state should work"),
        TaskState::Running
    );

    handle.pause().await.expect("pause should work");
    assert_eq!(
        handle.state().await.expect("state should work"),
        TaskState::Paused
    );

    handle.resume().await.expect("resume should work");
    assert_eq!(
        handle.state().await.expect("state should work"),
        TaskState::Running
    );

    handle.cancel().await.expect("cancel should work");
    assert_eq!(
        handle.state().await.expect("state should work"),
        TaskState::Cancelled
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_task_emits_state_and_progress_updates() {
    init_test_logging();
    let task = CopyTask::new(
        "task-stream",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );

    let handle = InMemoryTask::new(task, 8);
    let mut stream = handle.subscribe().expect("subscribe should work");

    handle.run().await.expect("run should work");
    handle
        .set_progress(TaskProgress {
            total_bytes: 100,
            complete_bytes: 30,
        })
        .expect("set_progress should work");

    let first = stream.recv().await.expect("first update should arrive");
    let second = stream.recv().await.expect("second update should arrive");
    let third = stream.recv().await.expect("third update should arrive");

    assert_eq!(first, TaskUpdate::State(TaskState::Planned));
    assert_eq!(second, TaskUpdate::State(TaskState::Running));
    assert_eq!(
        third,
        TaskUpdate::Progress(TaskProgress {
            total_bytes: 100,
            complete_bytes: 30,
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_task_with_zero_event_capacity_still_supports_subscribe() {
    init_test_logging();
    let task = CopyTask::new(
        "task-zero-cap",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );

    let handle = InMemoryTask::new(task, 0);
    let mut stream = handle.subscribe().expect("subscribe should work");

    handle.run().await.expect("run should work");

    // With minimal channel capacity, early updates may be lagged. We only require
    // that receiver remains usable and can observe the latest state transition.
    let latest = loop {
        match stream.recv().await {
            Ok(update) => break update,
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => panic!("stream unexpectedly closed"),
        }
    };

    assert_eq!(latest, TaskUpdate::State(TaskState::Running));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_task_rejects_invalid_state_transition() {
    init_test_logging();
    let task = CopyTask::new(
        "task-invalid-state",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );
    let handle = InMemoryTask::new(task, 8);

    let err = handle
        .pause()
        .await
        .expect_err("pause from Created should fail");
    assert_eq!(err.code, "INVALID_STATE_TRANSITION");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_task_run_is_idempotent_when_already_running() {
    init_test_logging();
    let task = CopyTask::new(
        "task-idempotent-run",
        PathBuf::from("/src"),
        vec![PathBuf::from("/dst")],
        Default::default(),
    );
    let handle = InMemoryTask::new(task, 8);

    handle.run().await.expect("first run should work");
    handle.run().await.expect("second run should work");

    assert_eq!(
        handle.state().await.expect("state should work"),
        TaskState::Running
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_task_report_uses_latest_progress_values() {
    let handle = InMemoryTask::new(build_task("task-report"), 8);

    handle
        .set_progress(TaskProgress {
            total_bytes: 200,
            complete_bytes: 123,
        })
        .expect("set_progress should work");

    let report = handle.report().await.expect("report should work");
    assert_eq!(report.total_bytes, 200);
    assert_eq!(report.complete_bytes, 123);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn report_is_recomputed_after_progress_changes() {
    let handle = InMemoryTask::new(build_task("managed-004"), 8);

    handle
        .set_progress(TaskProgress {
            total_bytes: 100,
            complete_bytes: 10,
        })
        .expect("set progress should work");
    let report1 = handle.report().await.expect("report should work");
    assert_eq!(report1.total_bytes, 100);
    assert_eq!(report1.complete_bytes, 10);

    handle
        .set_progress(TaskProgress {
            total_bytes: 200,
            complete_bytes: 80,
        })
        .expect("set progress should work");
    let report2 = handle.report().await.expect("report should work");

    assert_eq!(report2.total_bytes, 200);
    assert_eq!(report2.complete_bytes, 80);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_task_rejects_resume_after_cancelled() {
    let handle = InMemoryTask::new(build_task("managed-cancel-resume"), 8);

    handle.run().await.expect("run should work");
    handle.cancel().await.expect("cancel should work");

    let err = match handle.resume().await {
        Ok(_) => panic!("resume from cancelled should fail"),
        Err(err) => err,
    };

    assert_eq!(err.code, "INVALID_STATE_TRANSITION");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_task_rejects_pause_after_completed() {
    init_test_logging();
    let task = build_task("managed-completed").with_state(TaskState::Completed);
    let handle = InMemoryTask::new(task, 8);

    let err = match handle.pause().await {
        Ok(_) => panic!("pause from completed should fail"),
        Err(err) => err,
    };

    assert_eq!(err.code, "INVALID_STATE_TRANSITION");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_task_cancelled_before_run_cannot_start() {
    init_test_logging();
    let handle = InMemoryTask::new(build_task("managed-created-cancel"), 8);

    handle
        .cancel()
        .await
        .expect("cancel from created should work");

    let err = handle
        .run()
        .await
        .expect_err("cancelled task should not run");

    assert_eq!(err.code, "INVALID_STATE_TRANSITION");
    assert_eq!(
        handle.state().await.expect("state should work"),
        TaskState::Cancelled
    );
}
