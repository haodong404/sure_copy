use std::sync::Arc;

use crate::domain::CopyError;

use super::stage::{PostWriteStage, SourceObserverStage};
use super::types::{
    PostWritePipelineMode, PostWritePipelineSpec, SourceObserverPipelineSpec, SourcePipelineMode,
    TaskFlowSpec,
};

/// Runtime source-side observer pipeline.
///
/// This struct describes *how* source chunks are processed while a file is being
/// copied: which observer stages are attached, and whether they run serially
/// before fan-out or concurrently with destination writes.
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

/// Runtime post-write pipeline executed per destination branch.
///
/// This struct holds the concrete post-write stages to run after one destination
/// file has been written successfully, together with their scheduling mode.
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

/// Runtime execution-flow plan for one task.
///
/// `TaskFlowPlan` is control-plane orchestration: it describes *how* file copy work
/// should flow through runtime pipeline stages. It is intentionally separate from
/// `FilePlan`, which describes *what* concrete file work needs to be performed.
///
/// The embedded `*_pipeline_mode` fields serve as fallback mode storage even when a
/// concrete runtime pipeline is absent, so task specs can preserve scheduling
/// semantics without requiring stage instances to exist yet.
#[derive(Clone)]
pub struct TaskFlowPlan {
    /// Optional source observer pipeline attached to the task runtime.
    pub source_observer: Option<SourceObserverPipeline>,
    /// Optional post-write pipeline attached to the task runtime.
    pub post_write: Option<PostWritePipeline>,
    /// Fallback source pipeline mode when no `SourceObserverPipeline` is attached.
    source_pipeline_mode: SourcePipelineMode,
    /// Fallback post-write pipeline mode when no `PostWritePipeline` is attached.
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

    pub fn try_to_spec(&self) -> Result<Option<TaskFlowSpec>, CopyError> {
        let source_observer = self
            .source_observer
            .as_ref()
            .map(|pipeline| {
                let stages = pipeline
                    .stages
                    .iter()
                    .map(|stage| {
                        stage.spec().ok_or(CopyError {
                            category: crate::domain::CopyErrorCategory::NotImplemented,
                            code: "STAGE_SPEC_MISSING",
                            message: format!(
                                "source observer stage '{}' cannot be durably persisted because it does not expose a stage spec",
                                stage.id()
                            ),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(SourceObserverPipelineSpec {
                    mode: pipeline.mode,
                    stages,
                })
            })
            .transpose()?;

        let post_write = self
            .post_write
            .as_ref()
            .map(|pipeline| {
                let stages = pipeline
                    .stages
                    .iter()
                    .map(|stage| {
                        stage.spec().ok_or(CopyError {
                            category: crate::domain::CopyErrorCategory::NotImplemented,
                            code: "STAGE_SPEC_MISSING",
                            message: format!(
                                "post-write stage '{}' cannot be durably persisted because it does not expose a stage spec",
                                stage.id()
                            ),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(PostWritePipelineSpec {
                    mode: pipeline.mode,
                    stages,
                })
            })
            .transpose()?;

        let spec = TaskFlowSpec {
            source_observer,
            post_write,
        };

        if spec.is_empty() {
            Ok(None)
        } else {
            Ok(Some(spec))
        }
    }
}
