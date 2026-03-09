use std::sync::Arc;

use crate::domain::{CopyError, CopyTask};
use crate::orchestrator::{Task, TaskOrchestrator, TaskStream};

/// Stable facade API for consumers such as Tauri UI or CLI.
pub struct SureCopyCoreApi {
    orchestrator: Arc<dyn TaskOrchestrator>,
}

impl SureCopyCoreApi {
    /// Creates a new API facade from a task orchestrator implementation.
    pub fn new(orchestrator: Arc<dyn TaskOrchestrator>) -> Self {
        Self { orchestrator }
    }

    /// Submits a task and returns a managed task handle.
    pub async fn submit(&self, task: CopyTask) -> Result<Arc<dyn Task>, CopyError> {
        self.orchestrator.submit(task).await
    }

    /// Returns a managed task handle by id.
    pub async fn task(&self, task_id: &str) -> Result<Arc<dyn Task>, CopyError> {
        self.orchestrator.get_task(task_id).await
    }

    /// Subscribes to realtime updates for one task.
    ///
    /// The returned stream emits `TaskUpdate::State` and `TaskUpdate::Progress`.
    pub async fn subscribe(&self, task_id: &str) -> Result<TaskStream, CopyError> {
        let handle = self.task(task_id).await?;
        handle.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::domain::{CopyTask, TaskOptions, TaskProgress, TaskState};
    use crate::orchestrator::TaskUpdate;
    use crate::pipeline::{Pipeline, PipelineKind, PreCopyPipelineMode, StageExecution};
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    fn make_error(code: &'static str, message: &str) -> CopyError {
        CopyError {
            category: crate::domain::CopyErrorCategory::Unknown,
            code,
            message: message.to_string(),
        }
    }

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

        fn subscribe(&self) -> Result<TaskStream, CopyError> {
            let (tx, rx) = broadcast::channel(8);
            let _ = tx.send(TaskUpdate::State(TaskState::Running));
            let _ = tx.send(TaskUpdate::Progress(TaskProgress {
                total_bytes: 100,
                complete_bytes: 40,
            }));
            Ok(rx)
        }
    }

    struct StubOrchestrator;

    #[async_trait]
    impl TaskOrchestrator for StubOrchestrator {
        async fn submit(&self, task: CopyTask) -> Result<Arc<dyn Task>, CopyError> {
            Ok(Arc::new(StubTask { task }))
        }

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

    struct FailingSubmitOrchestrator;

    #[async_trait]
    impl TaskOrchestrator for FailingSubmitOrchestrator {
        async fn submit(&self, _task: CopyTask) -> Result<Arc<dyn Task>, CopyError> {
            Err(make_error("SUBMIT_FAILED", "submit failed for testing"))
        }
    }

    struct FailingGetTaskOrchestrator;

    #[async_trait]
    impl TaskOrchestrator for FailingGetTaskOrchestrator {
        async fn get_task(&self, _task_id: &str) -> Result<Arc<dyn Task>, CopyError> {
            Err(make_error(
                "TASK_LOOKUP_FAILED",
                "task lookup failed for testing",
            ))
        }
    }

    struct SubscribeFailTask {
        task: CopyTask,
    }

    #[async_trait]
    impl Task for SubscribeFailTask {
        fn snapshot(&self) -> CopyTask {
            self.task.clone()
        }

        fn id(&self) -> &str {
            &self.task.id
        }

        fn subscribe(&self) -> Result<TaskStream, CopyError> {
            Err(make_error(
                "SUBSCRIBE_FAILED",
                "subscribe failed for testing",
            ))
        }
    }

    struct SubscribeFailOrchestrator;

    #[async_trait]
    impl TaskOrchestrator for SubscribeFailOrchestrator {
        async fn get_task(&self, task_id: &str) -> Result<Arc<dyn Task>, CopyError> {
            Ok(Arc::new(SubscribeFailTask {
                task: CopyTask::new(
                    task_id,
                    "/src".into(),
                    vec!["/dst".into()],
                    Default::default(),
                ),
            }))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn facade_submit_returns_task_handle() {
        let api = SureCopyCoreApi::new(Arc::new(StubOrchestrator));
        let pre = Arc::new(Pipeline::new(
            PipelineKind::PreCopy,
            StageExecution::Parallel,
            vec![],
        ));
        let task = CopyTask::new(
            "task-001",
            PathBuf::from("/src"),
            vec![PathBuf::from("/dst")],
            TaskOptions::default(),
        )
        .with_pipelines(
            crate::pipeline::TaskPipelinePlan::new()
                .with_pre_copy_pipeline(pre)
                .with_pre_copy_mode(PreCopyPipelineMode::ConcurrentWithCopy),
        );

        let handle = api
            .submit(task)
            .await
            .expect("submit should return a task handle");
        assert_eq!(handle.snapshot().id, "task-001");
        assert_eq!(handle.id(), "task-001");
        assert_eq!(
            handle.snapshot().pipelines.pre_copy_mode,
            PreCopyPipelineMode::ConcurrentWithCopy
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn facade_can_get_task_by_id() {
        let api = SureCopyCoreApi::new(Arc::new(StubOrchestrator));
        let handle = api
            .task("task-001")
            .await
            .expect("task lookup should succeed");

        let state = handle.state().await.expect("state query should succeed");
        assert_eq!(handle.snapshot().id, "task-001");
        assert_eq!(state, TaskState::Created);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn facade_can_subscribe_task_updates() {
        let api = SureCopyCoreApi::new(Arc::new(StubOrchestrator));
        let mut update_rx = api
            .subscribe("task-001")
            .await
            .expect("task stream should be available");

        let state_update = update_rx.recv().await.expect("should receive state update");
        let progress_update = update_rx
            .recv()
            .await
            .expect("should receive progress update");

        assert_eq!(state_update, TaskUpdate::State(TaskState::Running));
        assert_eq!(
            progress_update,
            TaskUpdate::Progress(TaskProgress {
                total_bytes: 100,
                complete_bytes: 40,
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn facade_submit_propagates_orchestrator_error() {
        let api = SureCopyCoreApi::new(Arc::new(FailingSubmitOrchestrator));
        let task = CopyTask::new(
            "task-submit-fail",
            PathBuf::from("/src"),
            vec![PathBuf::from("/dst")],
            TaskOptions::default(),
        );

        let err = match api.submit(task).await {
            Ok(_) => panic!("submit should propagate orchestrator error"),
            Err(err) => err,
        };
        assert_eq!(err.code, "SUBMIT_FAILED");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn facade_task_propagates_orchestrator_lookup_error() {
        let api = SureCopyCoreApi::new(Arc::new(FailingGetTaskOrchestrator));

        let err = match api.task("task-lookup-fail").await {
            Ok(_) => panic!("task should propagate orchestrator lookup error"),
            Err(err) => err,
        };
        assert_eq!(err.code, "TASK_LOOKUP_FAILED");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn facade_subscribe_propagates_task_subscribe_error() {
        let api = SureCopyCoreApi::new(Arc::new(SubscribeFailOrchestrator));

        let err = api
            .subscribe("task-subscribe-fail")
            .await
            .expect_err("subscribe should propagate task subscribe error");
        assert_eq!(err.code, "SUBSCRIBE_FAILED");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn facade_subscribe_propagates_lookup_error_before_subscribe() {
        let api = SureCopyCoreApi::new(Arc::new(FailingGetTaskOrchestrator));

        let err = api
            .subscribe("task-lookup-fail")
            .await
            .expect_err("subscribe should fail on lookup before subscribe call");
        assert_eq!(err.code, "TASK_LOOKUP_FAILED");
    }
}
