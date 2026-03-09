use async_trait::async_trait;
use log::{debug, info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::domain::{CopyError, CopyTask, TaskState};

use super::config::OrchestratorConfig;
use super::errors::{duplicate_task_error, task_not_found_error};
use super::task::{InMemoryTask, Task, TaskOrchestrator};

/// In-memory orchestrator implementation for local runtime and tests.
pub struct InMemoryTaskOrchestrator {
    config: OrchestratorConfig,
    tasks: RwLock<HashMap<String, Arc<InMemoryTask>>>,
}

impl InMemoryTaskOrchestrator {
    /// Creates an in-memory orchestrator with explicit config.
    pub fn new(config: OrchestratorConfig) -> Self {
        debug!(
            "creating in-memory orchestrator with event channel capacity {}",
            config.event_channel_capacity
        );
        Self {
            config,
            tasks: RwLock::new(HashMap::new()),
        }
    }

    /// Returns runtime config for inspection.
    pub fn config(&self) -> &OrchestratorConfig {
        &self.config
    }
}

impl Default for InMemoryTaskOrchestrator {
    fn default() -> Self {
        Self::new(OrchestratorConfig::default())
    }
}

#[async_trait]
impl TaskOrchestrator for InMemoryTaskOrchestrator {
    async fn submit(&self, task: CopyTask) -> Result<Arc<dyn Task>, CopyError> {
        let mut tasks = self.tasks.write().await;

        if tasks.contains_key(&task.id) {
            return Err(duplicate_task_error(&task.id));
        }

        info!("submitting in-memory task '{}'", task.id);
        let handle = Arc::new(InMemoryTask::new(task, self.config.event_channel_capacity));
        let task_id = handle.id().to_string();
        tasks.insert(task_id, Arc::clone(&handle));

        let as_dyn: Arc<dyn Task> = handle;
        Ok(as_dyn)
    }

    async fn get_task(&self, task_id: &str) -> Result<Arc<dyn Task>, CopyError> {
        let tasks = self.tasks.read().await;
        let handle = tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| task_not_found_error(task_id))?;
        debug!(
            "loaded in-memory task '{}' from orchestrator cache",
            task_id
        );

        let as_dyn: Arc<dyn Task> = handle;
        Ok(as_dyn)
    }

    async fn get_tasks(&self) -> Result<Vec<Arc<dyn Task>>, CopyError> {
        let tasks = self.tasks.read().await;
        let mut handles = Vec::with_capacity(tasks.len());

        for task in tasks.values() {
            let cloned: Arc<InMemoryTask> = Arc::clone(task);
            let as_dyn: Arc<dyn Task> = cloned;
            handles.push(as_dyn);
        }

        debug!("loaded {} in-memory task handles", handles.len());
        Ok(handles)
    }

    async fn pause_all(&self) -> Result<(), CopyError> {
        info!("pausing all running in-memory tasks");
        let handles = {
            let tasks = self.tasks.read().await;
            tasks.values().cloned().collect::<Vec<_>>()
        };

        for handle in handles {
            let state = handle.state().await?;
            if state == TaskState::Running {
                handle.pause().await?;
            }
        }

        Ok(())
    }

    async fn resume_all(&self) -> Result<(), CopyError> {
        info!("resuming all paused in-memory tasks");
        let handles = {
            let tasks = self.tasks.read().await;
            tasks.values().cloned().collect::<Vec<_>>()
        };

        for handle in handles {
            let state = handle.state().await?;
            if state == TaskState::Paused {
                handle.resume().await?;
            }
        }

        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), CopyError> {
        info!("cancelling all active in-memory tasks");
        let handles = {
            let tasks = self.tasks.read().await;
            tasks.values().cloned().collect::<Vec<_>>()
        };

        for handle in handles {
            let state = handle.state().await?;
            if state != TaskState::Cancelled
                && state != TaskState::Completed
                && state != TaskState::Failed
            {
                handle.cancel().await?;
            }
        }

        Ok(())
    }
}
