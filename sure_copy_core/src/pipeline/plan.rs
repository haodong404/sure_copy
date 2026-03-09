use std::sync::Arc;

use super::types::{Pipeline, PreCopyPipelineMode};

/// Task-level pipeline customization injected at task creation time.
#[derive(Clone)]
pub struct TaskPipelinePlan {
    pub pre_copy_pipeline: Option<Arc<Pipeline>>,
    pub post_copy_pipeline: Option<Arc<Pipeline>>,
    pub pre_copy_mode: PreCopyPipelineMode,
}

impl Default for TaskPipelinePlan {
    fn default() -> Self {
        Self {
            pre_copy_pipeline: None,
            post_copy_pipeline: None,
            pre_copy_mode: PreCopyPipelineMode::SerialBeforeCopy,
        }
    }
}

impl TaskPipelinePlan {
    /// Creates a plan with defaults and no optional pipelines.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the pre-copy pipeline.
    pub fn with_pre_copy_pipeline(mut self, pipeline: Arc<Pipeline>) -> Self {
        self.pre_copy_pipeline = Some(pipeline);
        self
    }

    /// Sets the post-copy pipeline.
    pub fn with_post_copy_pipeline(mut self, pipeline: Arc<Pipeline>) -> Self {
        self.post_copy_pipeline = Some(pipeline);
        self
    }

    /// Sets pre-copy scheduling mode.
    pub fn with_pre_copy_mode(mut self, mode: PreCopyPipelineMode) -> Self {
        self.pre_copy_mode = mode;
        self
    }
}
