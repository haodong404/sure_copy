use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::pipeline::{PostWritePipelineMode, SourcePipelineMode, TaskFlowPlan, TaskFlowSpec};

fn available_cpu_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1)
}

/// Unique identifier for one copy task.
pub type CopyTaskId = String;

/// Lifecycle states for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Created,
    Planned,
    Preparing,
    Running,
    Completed,
    PartialFailed,
    Failed,
    Cancelled,
    Paused,
}

impl TaskState {
    /// Checks whether a transition to `next` is allowed by the recommended state machine.
    pub fn can_transition_to(self, next: TaskState) -> bool {
        use TaskState::{
            Cancelled, Completed, Created, Failed, PartialFailed, Paused, Planned, Preparing,
            Running,
        };

        match (self, next) {
            (Created, Planned) => true,
            (Created, Cancelled) => true,
            (Planned, Preparing | Running) => true,
            (Planned, Cancelled) => true,
            (Preparing, Running | Cancelled | Paused) => true,
            (Running, Completed | PartialFailed | Failed | Cancelled | Paused) => true,
            (Paused, Preparing | Running | Cancelled) => true,
            // Self-transition can simplify idempotent orchestration calls.
            (a, b) if a == b => true,
            _ => false,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskState::Completed
                | TaskState::PartialFailed
                | TaskState::Failed
                | TaskState::Cancelled
        )
    }
}

/// Overwrite behavior for destination conflicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OverwritePolicy {
    Skip,
    Overwrite,
    Rename,
    NewerOnly,
}

/// Retry behavior for retryable failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
    pub exponential_factor: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 200,
            exponential_factor: 2,
        }
    }
}

/// Tunable options for one task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskOptions {
    pub overwrite_policy: OverwritePolicy,
    pub retry_policy: RetryPolicy,
    pub max_concurrency: usize,
    pub per_file_pipeline_parallelism: usize,
    pub buffer_size_bytes: usize,
    pub preserve_timestamps: bool,
    pub preserve_permissions: bool,
    pub include_patterns: Vec<String>,
    pub exclude_patterns: Vec<String>,
}

impl Default for TaskOptions {
    fn default() -> Self {
        let cpu = available_cpu_parallelism();

        Self {
            overwrite_policy: OverwritePolicy::Skip,
            retry_policy: RetryPolicy::default(),
            max_concurrency: cpu * 2,
            per_file_pipeline_parallelism: cpu,
            buffer_size_bytes: 1024 * 1024,
            preserve_timestamps: true,
            preserve_permissions: true,
            include_patterns: Vec::new(),
            exclude_patterns: Vec::new(),
        }
    }
}

/// Immutable submission spec that can be durably persisted and reloaded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub source_root: PathBuf,
    pub destinations: Vec<PathBuf>,
    pub options: TaskOptions,
    #[serde(default)]
    pub flow_spec: Option<TaskFlowSpec>,
    #[serde(default)]
    pub source_pipeline_mode: SourcePipelineMode,
    #[serde(default)]
    pub post_write_pipeline_mode: PostWritePipelineMode,
}

impl TaskSpec {
    pub fn new(source_root: PathBuf, destinations: Vec<PathBuf>, options: TaskOptions) -> Self {
        Self {
            source_root,
            destinations,
            options,
            flow_spec: None,
            source_pipeline_mode: SourcePipelineMode::default(),
            post_write_pipeline_mode: PostWritePipelineMode::default(),
        }
    }
}

/// File-level execution plan derived from a task's source tree scan.
///
/// `FilePlan` answers the question: "for this concrete source file, which
/// destination file paths should be produced?" It is data-plane work decomposition,
/// not pipeline orchestration. In other words, it describes *what* to copy for one
/// source file after include/exclude filtering and path expansion have already
/// happened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilePlan {
    pub source: PathBuf,
    pub destinations: Vec<PathBuf>,
    pub expected_size_bytes: Option<u64>,
    pub expected_checksum: Option<String>,
}

/// Task aggregate root in the domain layer.
#[derive(Clone, Serialize)]
pub struct CopyTask {
    pub id: CopyTaskId,
    pub source_root: PathBuf,
    pub destinations: Vec<PathBuf>,
    pub options: TaskOptions,
    /// Durable, data-only flow description used for persistence and restart.
    pub flow_spec: Option<TaskFlowSpec>,
    /// Runtime execution flow attachments rebuilt from `flow_spec` or injected
    /// directly by the caller. This can contain trait objects and is therefore
    /// not serialized.
    #[serde(skip_serializing)]
    pub flow: TaskFlowPlan,
    pub state: TaskState,
    /// File-level work items materialized from the task configuration.
    pub file_plans: Vec<FilePlan>,
}

impl CopyTask {
    /// Creates a task with safe defaults for runtime-managed fields.
    pub fn new(
        id: impl Into<CopyTaskId>,
        source_root: PathBuf,
        destinations: Vec<PathBuf>,
        options: TaskOptions,
    ) -> Self {
        Self::from_spec(id, TaskSpec::new(source_root, destinations, options))
    }

    /// Creates a task from an immutable submission spec.
    pub fn from_spec(id: impl Into<CopyTaskId>, spec: TaskSpec) -> Self {
        Self {
            id: id.into(),
            source_root: spec.source_root,
            destinations: spec.destinations,
            options: spec.options,
            flow_spec: spec.flow_spec,
            flow: TaskFlowPlan::default()
                .with_source_pipeline_mode(spec.source_pipeline_mode)
                .with_post_write_pipeline_mode(spec.post_write_pipeline_mode),
            state: TaskState::Created,
            file_plans: Vec::new(),
        }
    }

    /// Returns the immutable submission spec for persistence/recovery.
    pub fn spec(&self) -> TaskSpec {
        TaskSpec {
            source_root: self.source_root.clone(),
            destinations: self.destinations.clone(),
            options: self.options.clone(),
            flow_spec: self.flow_spec.clone(),
            source_pipeline_mode: self.flow.source_pipeline_mode(),
            post_write_pipeline_mode: self.flow.post_write_pipeline_mode(),
        }
    }

    /// Persists a durable flow specification for restart-safe pipeline rehydration.
    pub fn with_flow_spec(mut self, flow_spec: TaskFlowSpec) -> Self {
        self.flow_spec = Some(flow_spec);
        self
    }

    /// Overrides task flow plan.
    pub fn with_flow(mut self, flow: TaskFlowPlan) -> Self {
        self.flow = flow;
        self
    }

    /// Overrides task state.
    pub fn with_state(mut self, state: TaskState) -> Self {
        self.state = state;
        self
    }

    /// Overrides planned files.
    pub fn with_file_plans(mut self, file_plans: Vec<FilePlan>) -> Self {
        self.file_plans = file_plans;
        self
    }
}

/// Runtime progress in both bytes and file counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProgress {
    pub total_bytes: u64,
    pub complete_bytes: u64,
    pub active_transfers: Vec<ActiveTransferProgress>,
    pub stage_progresses: Vec<StageProgress>,
}

impl Default for TaskProgress {
    fn default() -> Self {
        Self {
            total_bytes: 0,
            complete_bytes: 0,
            active_transfers: Vec::new(),
            stage_progresses: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferPhase {
    Pending,
    Copying,
    PostWrite,
    Completed,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveTransferProgress {
    pub source_path: PathBuf,
    pub destination_path: PathBuf,
    pub actual_destination_path: Option<PathBuf>,
    pub bytes_copied: u64,
    pub expected_bytes: u64,
    pub phase: TransferPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageProgressStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageProgress {
    pub stage_id: String,
    pub source_path: PathBuf,
    pub destination_path: Option<PathBuf>,
    pub processed_bytes: u64,
    pub total_bytes: Option<u64>,
    pub status: StageProgressStatus,
}

/// Common error categories for reporting and filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CopyErrorCategory {
    Path,
    Permission,
    Io,
    Checksum,
    Cancelled,
    Interrupted,
    NotImplemented,
    Unknown,
}

/// Structured domain error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CopyError {
    pub category: CopyErrorCategory,
    pub code: &'static str,
    pub message: String,
}

impl CopyError {
    /// Creates a standard not-implemented error for skeleton APIs.
    pub fn not_implemented(api_name: &'static str) -> Self {
        Self {
            category: CopyErrorCategory::NotImplemented,
            code: "NOT_IMPLEMENTED",
            message: format!("{} is not implemented in the API skeleton", api_name),
        }
    }
}

/// Failure detail for one file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileFailure {
    pub path: PathBuf,
    pub error: CopyError,
    pub retries: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DestinationCopyStatus {
    Pending,
    Skipped,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DestinationPostWriteStatus {
    Pending,
    NotRun,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DestinationReport {
    pub source_path: PathBuf,
    pub destination_path: PathBuf,
    pub actual_destination_path: Option<PathBuf>,
    pub bytes_written: u64,
    pub copy_status: DestinationCopyStatus,
    pub post_write_status: DestinationPostWriteStatus,
    pub error: Option<CopyError>,
}

/// Task-level report output.
#[derive(Clone, Serialize)]
pub struct CopyReport {
    /// Task runtime snapshot captured for this report.
    pub snapshot: CopyTask,
    pub total_files: u64,
    pub succeeded_files: u64,
    pub failed_files: u64,
    pub total_bytes: u64,
    pub complete_bytes: u64,
    pub duration_ms: u128,
    pub retry_count: u64,
    pub failures: Vec<FileFailure>,
    pub destinations: Vec<DestinationReport>,
}

/// Template that can be reused and versioned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskTemplate {
    pub template_version: u32,
    pub source_root: PathBuf,
    pub destinations: Vec<PathBuf>,
    pub options: TaskOptions,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    #[test]
    fn state_machine_allows_expected_paths() {
        assert!(TaskState::Created.can_transition_to(TaskState::Planned));
        assert!(TaskState::Created.can_transition_to(TaskState::Cancelled));
        assert!(TaskState::Planned.can_transition_to(TaskState::Preparing));
        assert!(TaskState::Preparing.can_transition_to(TaskState::Running));
        assert!(TaskState::Planned.can_transition_to(TaskState::Cancelled));
        assert!(TaskState::Preparing.can_transition_to(TaskState::Paused));
        assert!(TaskState::Running.can_transition_to(TaskState::Paused));
        assert!(TaskState::Paused.can_transition_to(TaskState::Running));
        assert!(TaskState::Paused.can_transition_to(TaskState::Preparing));
        assert!(TaskState::Running.can_transition_to(TaskState::Completed));
        assert!(TaskState::Running.can_transition_to(TaskState::PartialFailed));
    }

    #[test]
    fn state_machine_rejects_invalid_path() {
        assert!(!TaskState::Created.can_transition_to(TaskState::Running));
        assert!(!TaskState::Completed.can_transition_to(TaskState::Running));
    }

    #[test]
    fn task_options_default_is_safe() {
        let opts = TaskOptions::default();
        assert_eq!(opts.overwrite_policy, OverwritePolicy::Skip);
        assert!(opts.max_concurrency >= 1);
        assert!(opts.per_file_pipeline_parallelism >= 1);
        assert!(opts.preserve_timestamps);
        assert!(opts.preserve_permissions);
    }

    #[test]
    fn can_build_not_implemented_error() {
        let err = CopyError::not_implemented("TaskOrchestrator::start");
        assert_eq!(err.category, CopyErrorCategory::NotImplemented);
        assert_eq!(err.code, "NOT_IMPLEMENTED");
        assert!(err.message.contains("TaskOrchestrator::start"));
    }

    #[test]
    fn state_machine_allows_self_transition_for_all_states() {
        let all_states = [
            TaskState::Created,
            TaskState::Planned,
            TaskState::Preparing,
            TaskState::Running,
            TaskState::Completed,
            TaskState::PartialFailed,
            TaskState::Failed,
            TaskState::Cancelled,
            TaskState::Paused,
        ];

        for state in all_states {
            assert!(state.can_transition_to(state));
        }
    }

    #[test]
    fn retry_policy_default_values_are_stable() {
        let retry = RetryPolicy::default();
        assert_eq!(retry.max_retries, 3);
        assert_eq!(retry.initial_backoff_ms, 200);
        assert_eq!(retry.exponential_factor, 2);
    }

    #[test]
    fn copy_task_new_initializes_runtime_managed_defaults() {
        let task = CopyTask::new(
            "task-123",
            PathBuf::from("/source"),
            vec![PathBuf::from("/dest-a"), PathBuf::from("/dest-b")],
            TaskOptions::default(),
        );

        assert_eq!(task.id, "task-123");
        assert_eq!(task.state, TaskState::Created);
        assert!(task.file_plans.is_empty());
        assert!(task.flow.source_observer.is_none());
        assert!(task.flow.post_write.is_none());
        assert_eq!(
            task.flow.source_pipeline_mode(),
            SourcePipelineMode::ConcurrentWithFanOut
        );
        assert_eq!(
            task.flow.post_write_pipeline_mode(),
            PostWritePipelineMode::SerialAfterWrite
        );
    }

    #[test]
    fn copy_task_builder_methods_override_fields() {
        let file_plans = vec![FilePlan {
            source: PathBuf::from("/src/file.txt"),
            destinations: vec![PathBuf::from("/dst/file.txt")],
            expected_size_bytes: Some(12),
            expected_checksum: Some("abc123".to_string()),
        }];

        let task = CopyTask::new(
            "task-456",
            PathBuf::from("/source"),
            vec![PathBuf::from("/dest")],
            TaskOptions::default(),
        )
        .with_state(TaskState::Planned)
        .with_file_plans(file_plans.clone());

        assert_eq!(task.state, TaskState::Planned);
        assert_eq!(task.file_plans, file_plans);
    }

    #[test]
    fn task_spec_roundtrip_preserves_flow_modes() {
        let task = CopyTask::new(
            "task-pipeline-spec",
            PathBuf::from("/source"),
            vec![PathBuf::from("/dest")],
            TaskOptions::default(),
        )
        .with_flow(
            TaskFlowPlan::new()
                .with_source_pipeline_mode(SourcePipelineMode::SerialBeforeFanOut)
                .with_post_write_pipeline_mode(PostWritePipelineMode::SerialAfterWrite),
        );

        let spec = task.spec();
        assert_eq!(
            spec.source_pipeline_mode,
            SourcePipelineMode::SerialBeforeFanOut
        );
        assert_eq!(
            spec.post_write_pipeline_mode,
            PostWritePipelineMode::SerialAfterWrite
        );

        let restored = CopyTask::from_spec("task-restored", spec);
        assert_eq!(
            restored.flow.source_pipeline_mode(),
            SourcePipelineMode::SerialBeforeFanOut
        );
        assert_eq!(
            restored.flow.post_write_pipeline_mode(),
            PostWritePipelineMode::SerialAfterWrite
        );
    }

    #[test]
    fn task_spec_roundtrip_preserves_flow_spec() {
        let task = CopyTask::new(
            "task-flow-spec",
            PathBuf::from("/source"),
            vec![PathBuf::from("/dest")],
            TaskOptions::default(),
        )
        .with_flow_spec(TaskFlowSpec {
            source_observer: None,
            post_write: Some(crate::pipeline::PostWritePipelineSpec {
                mode: PostWritePipelineMode::SerialAfterWrite,
                stages: vec![crate::pipeline::StageSpec::new("demo-stage")],
            }),
        });

        let spec = task.spec();
        assert_eq!(
            spec.flow_spec
                .as_ref()
                .and_then(|flow| flow.post_write.as_ref())
                .map(|pipeline| pipeline.stages.len()),
            Some(1)
        );

        let restored = CopyTask::from_spec("task-restored", spec);
        assert_eq!(
            restored
                .flow_spec
                .as_ref()
                .and_then(|flow| flow.post_write.as_ref())
                .map(|pipeline| pipeline.stages[0].kind.as_str()),
            Some("demo-stage")
        );
    }
}
