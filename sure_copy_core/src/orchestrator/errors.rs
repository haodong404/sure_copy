use crate::domain::{CopyError, CopyErrorCategory, TaskState};

pub(super) fn lock_poisoned_error(context: &'static str) -> CopyError {
    CopyError {
        category: CopyErrorCategory::Interrupted,
        code: "LOCK_POISONED",
        message: format!("lock poisoned while handling {}", context),
    }
}

pub(super) fn invalid_state_error(task_id: &str, from: TaskState, to: TaskState) -> CopyError {
    CopyError {
        category: CopyErrorCategory::Unknown,
        code: "INVALID_STATE_TRANSITION",
        message: format!(
            "task '{}' cannot transition from {:?} to {:?}",
            task_id, from, to
        ),
    }
}

pub(super) fn task_not_found_error(task_id: &str) -> CopyError {
    CopyError {
        category: CopyErrorCategory::Path,
        code: "TASK_NOT_FOUND",
        message: format!("task '{}' was not found", task_id),
    }
}

pub(super) fn duplicate_task_error(task_id: &str) -> CopyError {
    CopyError {
        category: CopyErrorCategory::Path,
        code: "TASK_ALREADY_EXISTS",
        message: format!("task '{}' already exists", task_id),
    }
}

pub(super) fn unsupported_feature_error(feature: &str) -> CopyError {
    CopyError {
        category: CopyErrorCategory::NotImplemented,
        code: "UNSUPPORTED_FEATURE",
        message: format!(
            "feature '{}' is not supported by this orchestrator",
            feature
        ),
    }
}
