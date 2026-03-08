use std::path::PathBuf;

use serde::Serialize;

use crate::pipeline::TaskPipelinePlan;

fn available_cpu_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1)
}

/// Unique identifier for one copy task.
pub type CopyTaskId = String;

/// Lifecycle states for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskState {
    Created,
    Planned,
    Running,
    Completed,
    Failed,
    Cancelled,
    Paused,
}

impl TaskState {
    /// Checks whether a transition to `next` is allowed by the recommended state machine.
    pub fn can_transition_to(self, next: TaskState) -> bool {
        use TaskState::{Cancelled, Completed, Created, Failed, Paused, Planned, Running};

        match (self, next) {
            (Created, Planned) => true,
            (Planned, Running) => true,
            (Running, Completed | Failed | Cancelled | Paused) => true,
            (Paused, Running | Cancelled) => true,
            // Self-transition can simplify idempotent orchestration calls.
            (a, b) if a == b => true,
            _ => false,
        }
    }
}

/// Overwrite behavior for destination conflicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OverwritePolicy {
    Skip,
    Overwrite,
    Rename,
    NewerOnly,
}

/// Verification strategy after write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum VerificationPolicy {
    None,
    PostCopy,
    PreAndPostCopy,
}

/// Retry behavior for retryable failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskOptions {
    pub overwrite_policy: OverwritePolicy,
    pub verification_policy: VerificationPolicy,
    pub retry_policy: RetryPolicy,
    pub max_concurrency: usize,
    pub per_file_pipeline_parallelism: usize,
    pub checksum_parallelism: usize,
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
            verification_policy: VerificationPolicy::PostCopy,
            retry_policy: RetryPolicy::default(),
            max_concurrency: cpu * 2,
            per_file_pipeline_parallelism: cpu,
            checksum_parallelism: cpu,
            buffer_size_bytes: 1024 * 1024,
            preserve_timestamps: true,
            preserve_permissions: true,
            include_patterns: Vec::new(),
            exclude_patterns: Vec::new(),
        }
    }
}

/// Planned work for one source file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    #[serde(skip_serializing)]
    pub pipelines: TaskPipelinePlan,
    pub state: TaskState,
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
        Self {
            id: id.into(),
            source_root,
            destinations,
            options,
            pipelines: TaskPipelinePlan::default(),
            state: TaskState::Created,
            file_plans: Vec::new(),
        }
    }

    /// Overrides task pipeline plan.
    pub fn with_pipelines(mut self, pipelines: TaskPipelinePlan) -> Self {
        self.pipelines = pipelines;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct TaskProgress {
    pub total_bytes: u64,
    pub complete_bytes: u64,
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

    #[test]
    fn state_machine_allows_expected_paths() {
        assert!(TaskState::Created.can_transition_to(TaskState::Planned));
        assert!(TaskState::Planned.can_transition_to(TaskState::Running));
        assert!(TaskState::Running.can_transition_to(TaskState::Paused));
        assert!(TaskState::Paused.can_transition_to(TaskState::Running));
        assert!(TaskState::Running.can_transition_to(TaskState::Completed));
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
        assert_eq!(opts.verification_policy, VerificationPolicy::PostCopy);
        assert!(opts.max_concurrency >= 1);
        assert!(opts.per_file_pipeline_parallelism >= 1);
        assert!(opts.checksum_parallelism >= 1);
        assert!(opts.preserve_permissions);
    }

    #[test]
    fn can_build_not_implemented_error() {
        let err = CopyError::not_implemented("TaskOrchestrator::start");
        assert_eq!(err.category, CopyErrorCategory::NotImplemented);
        assert_eq!(err.code, "NOT_IMPLEMENTED");
        assert!(err.message.contains("TaskOrchestrator::start"));
    }
}
