fn available_cpu_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1)
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
