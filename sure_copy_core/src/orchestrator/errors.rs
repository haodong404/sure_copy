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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_poisoned_error_has_expected_shape() {
        let err = lock_poisoned_error("PersistentTask::state");
        assert_eq!(err.category, CopyErrorCategory::Interrupted);
        assert_eq!(err.code, "LOCK_POISONED");
        assert!(err.message.contains("PersistentTask::state"));
    }

    #[test]
    fn invalid_state_error_has_expected_shape() {
        let err = invalid_state_error("task-1", TaskState::Created, TaskState::Running);
        assert_eq!(err.category, CopyErrorCategory::Unknown);
        assert_eq!(err.code, "INVALID_STATE_TRANSITION");
        assert!(err.message.contains("task-1"));
        assert!(err.message.contains("Created"));
        assert!(err.message.contains("Running"));
    }

    #[test]
    fn task_lookup_errors_have_expected_codes() {
        let not_found = task_not_found_error("missing");
        assert_eq!(not_found.code, "TASK_NOT_FOUND");

        let duplicate = duplicate_task_error("duplicate");
        assert_eq!(duplicate.code, "TASK_ALREADY_EXISTS");
    }
}
