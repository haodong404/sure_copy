use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::domain::{
    CopyError, CopyReport, CopyTask, TaskProgress, TaskState,
};

fn available_cpu_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1)
}

/// Realtime task update stream receiver type.
pub type TaskStream = broadcast::Receiver<TaskUpdate>;

/// Realtime task updates for UI observers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskUpdate {
    Progress(TaskProgress),
    State(TaskState),
}

/// Task handle returned by orchestrator submission and lookup APIs.
#[async_trait]
pub trait Task: Send + Sync {

    /// Returns task runtime snapshot including pipeline plan.
    ///
    /// This replaces the older `config()` naming.
    fn snapshot(&self) -> CopyTask;
    
    /// Returns task identifier.
    fn id(&self) -> &str;

    /// Starts or continues execution for this task.
    async fn run(&self) -> Result<(), CopyError> {
        Err(CopyError::not_implemented("Task::run"))
    }

    /// Pauses this task.
    async fn pause(&self) -> Result<(), CopyError> {
        Err(CopyError::not_implemented("Task::pause"))
    }

    /// Resumes this task.
    async fn resume(&self) -> Result<(), CopyError> {
        Err(CopyError::not_implemented("Task::resume"))
    }

    /// Cancels this task.
    async fn cancel(&self) -> Result<(), CopyError> {
        Err(CopyError::not_implemented("Task::cancel"))
    }

    /// Returns current task state.
    async fn state(&self) -> Result<TaskState, CopyError> {
        Err(CopyError::not_implemented("Task::state"))
    }

    /// Returns current task progress.
    async fn progress(&self) -> Result<TaskProgress, CopyError> {
        Err(CopyError::not_implemented("Task::progress"))
    }

    /// Subscribes to realtime task updates.
    fn subscribe(&self) -> Result<TaskStream, CopyError> {
        Err(CopyError::not_implemented("Task::subscribe"))
    }

    /// Returns task report.
    async fn report(&self) -> Result<CopyReport, CopyError> {
        Err(CopyError::not_implemented("Task::report"))
    }
}

/// Global orchestrator tuning knobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorConfig {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
    pub max_parallel_tasks: usize,
    pub max_open_files: usize,
    pub bandwidth_limit_bytes_per_sec: Option<u64>,
    pub task_queue_capacity: usize,
    pub event_channel_capacity: usize,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        let cpu = available_cpu_parallelism();

        Self {
            worker_threads: cpu,
            max_blocking_threads: cpu * 4,
            max_parallel_tasks: cpu,
            max_open_files: 1024,
            bandwidth_limit_bytes_per_sec: None,
            task_queue_capacity: 2048,
            event_channel_capacity: 4096,
        }
    }
}
#[async_trait]
pub trait TaskOrchestrator: Send + Sync {
    /// Creates and registers a managed task instance.
    async fn submit(&self, _task: CopyTask) -> Result<Arc<dyn Task>, CopyError> {
        Err(CopyError::not_implemented("TaskOrchestrator::submit"))
    }

    /// Returns a managed task by id.
    async fn get_task(&self, _task_id: &str) -> Result<Arc<dyn Task>, CopyError> {
        Err(CopyError::not_implemented("TaskOrchestrator::get_task"))
    }

    /// Returns all managed task handles.
    async fn get_tasks(&self) -> Result<Vec<Arc<dyn Task>>, CopyError> {
        Err(CopyError::not_implemented("TaskOrchestrator::get_tasks"))
    }

    /// Pauses all running tasks managed by this orchestrator.
    async fn pause_all(&self) -> Result<(), CopyError> {
        Err(CopyError::not_implemented("TaskOrchestrator::pause_all"))
    }

    /// Resumes all paused tasks managed by this orchestrator.
    async fn resume_all(&self) -> Result<(), CopyError> {
        Err(CopyError::not_implemented("TaskOrchestrator::resume_all"))
    }

    /// Cancels all tasks managed by this orchestrator.
    async fn cancel_all(&self) -> Result<(), CopyError> {
        Err(CopyError::not_implemented("TaskOrchestrator::cancel_all"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                complete_bytes: 25,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_trait_methods_return_not_implemented() {
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
    async fn submit_returns_task_handle() {
        let orchestrator = StubOrchestrator;
        let task = CopyTask::new(
            "task-001",
            "/src".into(),
            vec!["/dst".into()],
            Default::default(),
        );

        let handle = orchestrator
            .submit(task)
            .await
            .expect("submit should work");
        assert_eq!(handle.snapshot().id, "task-001");
        assert_eq!(handle.id(), "task-001");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_creation_has_default_parallel_pre_copy_mode() {
        let orchestrator = StubOrchestrator;
        let handle = orchestrator
            .get_task("task-001")
            .await
            .expect("get_task should work");

        assert_eq!(
            handle.snapshot().pipelines.pre_copy_mode,
            crate::pipeline::PreCopyPipelineMode::ConcurrentWithCopy
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_can_subscribe_realtime_updates() {
        let orchestrator = StubOrchestrator;
        let handle = orchestrator
            .get_task("task-001")
            .await
            .expect("get_task should work");

        let mut update_rx = handle.subscribe().expect("task stream should be available");

        let state_update = update_rx
            .recv()
            .await
            .expect("should receive one state update");
        let progress_update = update_rx
            .recv()
            .await
            .expect("should receive one progress update");

        assert_eq!(state_update, TaskUpdate::State(TaskState::Running));

        assert_eq!(
            progress_update,
            TaskUpdate::Progress(TaskProgress {
                total_bytes: 100,
                complete_bytes: 25,
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_batch_methods_return_not_implemented() {
        struct DefaultOrchestrator;
        #[async_trait]
        impl TaskOrchestrator for DefaultOrchestrator {}

        let orchestrator = DefaultOrchestrator;
        let err = orchestrator
            .pause_all()
            .await
            .expect_err("must be not implemented");
        assert_eq!(err.code, "NOT_IMPLEMENTED");
        assert!(err.message.contains("TaskOrchestrator::pause_all"));
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
}
