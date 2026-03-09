use std::sync::Arc;

use super::stage::{PostWriteStage, SourceObserverStage};
use super::types::{PostWritePipelineMode, SourcePipelineMode};

#[derive(Clone)]
pub struct SourceObserverPipeline {
    pub mode: SourcePipelineMode,
    pub stages: Vec<Arc<dyn SourceObserverStage>>,
}

impl Default for SourceObserverPipeline {
    fn default() -> Self {
        Self {
            mode: SourcePipelineMode::default(),
            stages: Vec::new(),
        }
    }
}

impl SourceObserverPipeline {
    pub fn new(mode: SourcePipelineMode) -> Self {
        Self {
            mode,
            stages: Vec::new(),
        }
    }

    pub fn with_stage(mut self, stage: Arc<dyn SourceObserverStage>) -> Self {
        self.stages.push(stage);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }
}

#[derive(Clone)]
pub struct PostWritePipeline {
    pub mode: PostWritePipelineMode,
    pub stages: Vec<Arc<dyn PostWriteStage>>,
}

impl Default for PostWritePipeline {
    fn default() -> Self {
        Self {
            mode: PostWritePipelineMode::default(),
            stages: Vec::new(),
        }
    }
}

impl PostWritePipeline {
    pub fn new(mode: PostWritePipelineMode) -> Self {
        Self {
            mode,
            stages: Vec::new(),
        }
    }

    pub fn with_stage(mut self, stage: Arc<dyn PostWriteStage>) -> Self {
        self.stages.push(stage);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }
}

/// Runtime-only task flow attachments plus their scheduling modes.
#[derive(Clone)]
pub struct TaskFlowPlan {
    pub source_observer: Option<SourceObserverPipeline>,
    pub post_write: Option<PostWritePipeline>,
    source_pipeline_mode: SourcePipelineMode,
    post_write_pipeline_mode: PostWritePipelineMode,
}

impl Default for TaskFlowPlan {
    fn default() -> Self {
        Self {
            source_observer: None,
            post_write: None,
            source_pipeline_mode: SourcePipelineMode::default(),
            post_write_pipeline_mode: PostWritePipelineMode::default(),
        }
    }
}

impl TaskFlowPlan {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_source_observer(mut self, pipeline: SourceObserverPipeline) -> Self {
        self.source_pipeline_mode = pipeline.mode;
        self.source_observer = Some(pipeline);
        self
    }

    pub fn with_post_write(mut self, pipeline: PostWritePipeline) -> Self {
        self.post_write_pipeline_mode = pipeline.mode;
        self.post_write = Some(pipeline);
        self
    }

    pub fn with_source_pipeline_mode(mut self, mode: SourcePipelineMode) -> Self {
        self.source_pipeline_mode = mode;
        if let Some(pipeline) = self.source_observer.as_mut() {
            pipeline.mode = mode;
        }
        self
    }

    pub fn with_post_write_pipeline_mode(mut self, mode: PostWritePipelineMode) -> Self {
        self.post_write_pipeline_mode = mode;
        if let Some(pipeline) = self.post_write.as_mut() {
            pipeline.mode = mode;
        }
        self
    }

    pub fn source_pipeline_mode(&self) -> SourcePipelineMode {
        self.source_observer
            .as_ref()
            .map(|pipeline| pipeline.mode)
            .unwrap_or(self.source_pipeline_mode)
    }

    pub fn post_write_pipeline_mode(&self) -> PostWritePipelineMode {
        self.post_write
            .as_ref()
            .map(|pipeline| pipeline.mode)
            .unwrap_or(self.post_write_pipeline_mode)
    }

    pub fn has_runtime_stages(&self) -> bool {
        self.source_observer
            .as_ref()
            .is_some_and(|pipeline| !pipeline.is_empty())
            || self
                .post_write
                .as_ref()
                .is_some_and(|pipeline| !pipeline.is_empty())
    }
}
