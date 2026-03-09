use async_trait::async_trait;
use log::{debug, info};
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::broadcast;

use crate::domain::{CopyError, CopyReport, CopyTask, DestinationReport, TaskProgress, TaskState};

use super::errors::{invalid_state_error, lock_poisoned_error};

/// Realtime task update stream receiver type.
pub type TaskStream = broadcast::Receiver<TaskUpdate>;

/// Realtime task updates for UI observers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskUpdate {
    Progress(TaskProgress),
    State(TaskState),
}

#[derive(Clone)]
struct TaskRuntime {
    snapshot: CopyTask,
    progress: TaskProgress,
    report: Option<CopyReport>,
}

/// In-memory task handle implementation.
pub struct InMemoryTask {
    id: String,
    runtime: RwLock<TaskRuntime>,
    event_tx: broadcast::Sender<TaskUpdate>,
}

impl InMemoryTask {
    /// Creates a managed in-memory task.
    pub fn new(task: CopyTask, event_channel_capacity: usize) -> Self {
        let (event_tx, _) = broadcast::channel(event_channel_capacity.max(1));
        let id = task.id.clone();
        debug!(
            "creating in-memory task '{}' with state {:?} and event channel capacity {}",
            id,
            task.state,
            event_channel_capacity.max(1)
        );

        Self {
            id,
            runtime: RwLock::new(TaskRuntime {
                snapshot: task,
                progress: TaskProgress::default(),
                report: None,
            }),
            event_tx,
        }
    }

    fn transition_state(&self, next: TaskState) -> Result<(), CopyError> {
        let mut runtime = self
            .runtime
            .write()
            .map_err(|_| lock_poisoned_error("InMemoryTask::transition_state"))?;
        let current = runtime.snapshot.state;

        if !current.can_transition_to(next) {
            return Err(invalid_state_error(&self.id, current, next));
        }

        info!(
            "task '{}' state transition {:?} -> {:?}",
            self.id, current, next
        );
        runtime.snapshot.state = next;
        runtime.report = None;
        let _ = self.event_tx.send(TaskUpdate::State(next));
        Ok(())
    }

    /// Updates current progress and emits a realtime progress event.
    pub fn set_progress(&self, progress: TaskProgress) -> Result<(), CopyError> {
        let mut runtime = self
            .runtime
            .write()
            .map_err(|_| lock_poisoned_error("InMemoryTask::set_progress"))?;
        debug!(
            "task '{}' progress updated to {}/{} bytes",
            self.id, progress.complete_bytes, progress.total_bytes
        );
        runtime.progress = progress;
        runtime.report = None;
        let _ = self.event_tx.send(TaskUpdate::Progress(progress));
        Ok(())
    }

    fn build_report(runtime: &TaskRuntime) -> CopyReport {
        CopyReport {
            snapshot: runtime.snapshot.clone(),
            total_files: runtime.snapshot.file_plans.len() as u64,
            succeeded_files: 0,
            failed_files: 0,
            total_bytes: runtime.progress.total_bytes,
            complete_bytes: runtime.progress.complete_bytes,
            duration_ms: 0,
            retry_count: 0,
            failures: Vec::new(),
            destinations: Vec::<DestinationReport>::new(),
        }
    }
}

/// Task handle returned by orchestrator submission and lookup APIs.
#[async_trait]
pub trait Task: Send + Sync {
    /// Returns task runtime snapshot including flow attachments.
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

#[async_trait]
impl Task for InMemoryTask {
    fn snapshot(&self) -> CopyTask {
        self.runtime
            .read()
            .map(|runtime| runtime.snapshot.clone())
            .unwrap_or_else(|_| {
                CopyTask::new(
                    self.id.clone(),
                    std::path::PathBuf::new(),
                    Vec::new(),
                    Default::default(),
                )
                .with_state(TaskState::Failed)
            })
    }

    fn id(&self) -> &str {
        &self.id
    }

    async fn run(&self) -> Result<(), CopyError> {
        let current = self.state().await?;

        if current == TaskState::Created {
            self.transition_state(TaskState::Planned)?;
        }

        self.transition_state(TaskState::Running)
    }

    async fn pause(&self) -> Result<(), CopyError> {
        self.transition_state(TaskState::Paused)
    }

    async fn resume(&self) -> Result<(), CopyError> {
        self.transition_state(TaskState::Running)
    }

    async fn cancel(&self) -> Result<(), CopyError> {
        self.transition_state(TaskState::Cancelled)
    }

    async fn state(&self) -> Result<TaskState, CopyError> {
        let runtime = self
            .runtime
            .read()
            .map_err(|_| lock_poisoned_error("InMemoryTask::state"))?;
        Ok(runtime.snapshot.state)
    }

    async fn progress(&self) -> Result<TaskProgress, CopyError> {
        let runtime = self
            .runtime
            .read()
            .map_err(|_| lock_poisoned_error("InMemoryTask::progress"))?;
        Ok(runtime.progress)
    }

    fn subscribe(&self) -> Result<TaskStream, CopyError> {
        Ok(self.event_tx.subscribe())
    }

    async fn report(&self) -> Result<CopyReport, CopyError> {
        let mut runtime = self
            .runtime
            .write()
            .map_err(|_| lock_poisoned_error("InMemoryTask::report"))?;

        let report = runtime
            .report
            .clone()
            .unwrap_or_else(|| Self::build_report(&runtime));
        runtime.report = Some(report.clone());
        Ok(report)
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
