use std::sync::Arc;

use async_trait::async_trait;
use sure_copy_core::{
    CopyError, CopyTask, OrchestratorConfig, SourcePipelineMode, Task, TaskOrchestrator, TaskState,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_orchestrator_default_methods_return_not_implemented() {
    struct DefaultOrchestrator;

    #[async_trait]
    impl TaskOrchestrator for DefaultOrchestrator {}

    let orchestrator = DefaultOrchestrator;
    let err = match orchestrator.get_task("task-1").await {
        Ok(_) => panic!("must be not implemented"),
        Err(err) => err,
    };

    assert_eq!(err.code, "NOT_IMPLEMENTED");
    assert!(err.message.contains("TaskOrchestrator::get_task"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_default_methods_return_not_implemented() {
    struct DefaultTask {
        task: CopyTask,
    }

    #[async_trait]
    impl Task for DefaultTask {
        fn snapshot(&self) -> CopyTask {
            self.task.clone()
        }

        fn id(&self) -> &str {
            &self.task.id
        }
    }

    let task = DefaultTask {
        task: CopyTask::new(
            "task-default",
            "/src".into(),
            vec!["/dst".into()],
            Default::default(),
        ),
    };

    let run_err = task.run().await.expect_err("run must be not implemented");
    assert_eq!(run_err.code, "NOT_IMPLEMENTED");
    assert!(run_err.message.contains("Task::run"));

    let state_err = task
        .state()
        .await
        .expect_err("state must be not implemented");
    assert_eq!(state_err.code, "NOT_IMPLEMENTED");
    assert!(state_err.message.contains("Task::state"));
}

#[test]
fn default_config_is_conservative() {
    let cfg = OrchestratorConfig::default();

    assert!(cfg.worker_threads >= 1);
    assert!(cfg.max_blocking_threads >= 1);
    assert!(cfg.max_parallel_tasks >= 1);
    assert_eq!(cfg.max_open_files, 1024);
    assert_eq!(cfg.bandwidth_limit_bytes_per_sec, None);
    assert!(cfg.task_queue_capacity >= 1);
    assert!(cfg.event_channel_capacity >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_creation_has_default_streaming_source_mode() {
    struct StubTask {
        task: CopyTask,
    }

    #[async_trait]
    impl Task for StubTask {
        fn snapshot(&self) -> CopyTask {
            self.task.clone()
        }

        fn id(&self) -> &str {
            &self.task.id
        }

        async fn state(&self) -> Result<TaskState, CopyError> {
            Ok(TaskState::Created)
        }
    }

    struct StubOrchestrator;

    #[async_trait]
    impl TaskOrchestrator for StubOrchestrator {
        async fn get_task(&self, task_id: &str) -> Result<Arc<dyn Task>, CopyError> {
            Ok(Arc::new(StubTask {
                task: CopyTask::new(
                    task_id,
                    "/src".into(),
                    vec!["/dst".into()],
                    Default::default(),
                ),
            }))
        }
    }

    let orchestrator = StubOrchestrator;
    let handle = orchestrator
        .get_task("task-001")
        .await
        .expect("get_task should work");

    assert_eq!(
        handle.snapshot().flow.source_pipeline_mode(),
        SourcePipelineMode::ConcurrentWithFanOut
    );
    assert_eq!(
        handle.snapshot().flow.post_write_pipeline_mode(),
        sure_copy_core::pipeline::PostWritePipelineMode::SerialAfterWrite
    );
}
