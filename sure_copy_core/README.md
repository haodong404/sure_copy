# sure_copy_core

`sure_copy_core` is the Rust core of SureCopy. It owns task modeling, lifecycle orchestration, durable checkpointing, progress reporting, and the extension points around file processing.

## What This Crate Provides

- `CopyTask` and `TaskSpec` for describing copy work.
- `TaskOptions` for overwrite, verification, retry, and concurrency policies.
- `TaskOrchestrator` implementations for ephemeral and durable runtimes.
- `Task` handles for running, pausing, resuming, cancelling, and observing tasks.
- `TaskUpdate` streams for UI progress.
- Flow traits for source-side observers and per-destination post-write stages.

## Quick Start

### 1. In-memory orchestration

Use the in-memory orchestrator when you want a simple runtime for tests, prototypes, or non-durable flows.

```rust
use std::path::PathBuf;
use std::sync::Arc;

use sure_copy_core::{
    CopyTask, InMemoryTaskOrchestrator, SureCopyCoreApi, TaskOptions,
};

#[tokio::main]
async fn main() -> Result<(), sure_copy_core::CopyError> {
    let orchestrator = Arc::new(InMemoryTaskOrchestrator::default());
    let api = SureCopyCoreApi::new(orchestrator);

    let task = CopyTask::new(
        "task-001",
        PathBuf::from("/source"),
        vec![PathBuf::from("/dest-a"), PathBuf::from("/dest-b")],
        TaskOptions::default(),
    );

    let handle = api.submit(task).await?;
    handle.run().await?;

    Ok(())
}
```

### 2. Durable SQLite orchestration

Use the SQLite orchestrator when task state must survive process restarts.

```rust
use std::path::PathBuf;
use std::sync::Arc;

use sure_copy_core::{
    CopyTask, OrchestratorConfig, SqliteTaskOrchestrator, SureCopyCoreApi, TaskOptions,
};

#[tokio::main]
async fn main() -> Result<(), sure_copy_core::CopyError> {
    let orchestrator = Arc::new(
        SqliteTaskOrchestrator::new(
            PathBuf::from("./sure_copy.db"),
            OrchestratorConfig::default(),
        )
        .await?,
    );
    let api = SureCopyCoreApi::new(orchestrator);

    let task = CopyTask::new(
        "task-002",
        PathBuf::from("/source"),
        vec![PathBuf::from("/dest")],
        TaskOptions::default(),
    );

    let handle = api.submit(task).await?;
    handle.run().await?;

    Ok(())
}
```

### 3. Subscribe to task updates

The interface layer exposes a broadcast stream of task state and progress events.

```rust
use std::path::PathBuf;
use std::sync::Arc;

use sure_copy_core::{
    CopyTask, InMemoryTaskOrchestrator, SureCopyCoreApi, TaskOptions, TaskUpdate,
};

#[tokio::main]
async fn main() -> Result<(), sure_copy_core::CopyError> {
    let orchestrator = Arc::new(InMemoryTaskOrchestrator::default());
    let api = SureCopyCoreApi::new(orchestrator);

    let task = CopyTask::new(
        "task-003",
        PathBuf::from("/source"),
        vec![PathBuf::from("/dest")],
        TaskOptions::default(),
    );

    let handle = api.submit(task).await?;
    let task_id = handle.id().to_string();
    let mut updates = api.subscribe(&task_id).await?;

    handle.run().await?;

    while let Ok(update) = updates.recv().await {
        match update {
            TaskUpdate::State(state) => println!("state: {state:?}"),
            TaskUpdate::Progress(progress) => {
                println!("progress: {}/{}", progress.complete_bytes, progress.total_bytes);
            }
        }
    }

    Ok(())
}
```

## Main Concepts

### TaskSpec vs CopyTask

- `TaskSpec` is the immutable submission payload: source root, destinations, and options.
- `CopyTask` is the runtime snapshot: it wraps the spec data and adds state, file plans, and pipeline attachments.

The split matters for durable orchestration. SQLite-backed tasks persist the submission spec and rebuild the runtime snapshot from storage on restart.

### Task lifecycle

The runtime uses this state machine:

`Created -> Planned -> Running -> Completed`

Exceptional branches:

- `Created -> Cancelled`
- `Planned -> Cancelled`
- `Running -> PartialFailed`
- `Running -> Failed`
- `Running -> Cancelled`
- `Running -> Paused`
- `Paused -> Running`
- `Paused -> Cancelled`

### Verification and retries

- `OverwritePolicy` controls what happens when the destination already exists.
- `VerificationPolicy` controls whether copy results are checksum-verified.
- `RetryPolicy` controls retry count and exponential backoff during copy failures.

## Design Structure

### `domain`

This layer defines the stable business model:

- `TaskSpec`
- `CopyTask`
- `TaskOptions`
- `FilePlan`
- `CopyReport`
- `CopyError`

Keep this layer free of concrete I/O concerns.

### `interface`

This layer provides the facade that higher-level apps should depend on first:

- `SureCopyCoreApi::submit`
- `SureCopyCoreApi::task`
- `SureCopyCoreApi::subscribe`

If you are integrating with Tauri or a CLI, start here.

### `orchestrator`

This layer owns execution and durability.

- `InMemoryTaskOrchestrator`: lightweight runtime for tests and ephemeral use.
- `SqliteTaskOrchestrator`: durable runtime with checkpoint persistence and startup recovery.
- `Task`: the task handle interface returned to callers.

`PersistentTask` is the core of the durable flow. It groups work per source file, fans one reader out to multiple destination writers, runs source-side observers, executes per-destination post-write verification, refreshes progress, and persists state transitions.

### `pipeline`

This layer defines the flow contracts:

- `SourceObserverStage`
- `PostWriteStage`
- `SourceObserverPipeline`
- `PostWritePipeline`
- `TaskFlowPlan`

The durable runtime support is intentionally conservative:

- `SourcePipelineMode` controls whether source observers run before fan-out or during fan-out.
- `PostWritePipelineMode::SerialAfterWrite` runs after each destination write completes.
- Durable SQLite tasks reject custom runtime stages because trait-object stages are not yet persisted safely.

### `infrastructure`

This layer contains swappable runtime services:

- `FileSystem`
- `ChecksumProvider`
- `LocalFileSystem`
- `Sha256ChecksumProvider`

The durable runtime depends on these abstractions rather than hard-coding every I/O operation inline.

## Current Behavior Boundaries

The crate is usable today, but it is important to understand the current boundaries:

- SQLite-backed tasks persist task specs, task state, and file checkpoints.
- Resume works at file/destination checkpoint granularity, not byte-range partial copy granularity.
- Rename overwrite mode records the actual destination path in checkpoints.
- Progress is computed from checkpoint `expected_bytes`, so totals stay stable during resume.
- Custom runtime stage execution is not yet supported by the durable SQLite orchestrator.
- The in-memory orchestrator is intentionally simpler and does not aim to mirror every durability concern.

## Suggested Integration Pattern

For application code, a good default flow is:

1. Build a `CopyTask` with `TaskOptions`.
2. Submit it through `SureCopyCoreApi`.
3. Store the returned task id in your UI state.
4. Subscribe to `TaskUpdate` for progress rendering.
5. Call `run`, `pause`, `resume`, and `cancel` through the returned `Task` handle.
6. Query `snapshot`, `state`, `progress`, and `report` when you need a synchronous view.

## Tests

The crate already includes integration tests under `sure_copy_core/tests/` covering:

- in-memory orchestration
- SQLite durability and schema migration
- overwrite policies
- checkpoint-based resume
- progress reconstruction
- filesystem and checksum adapters

Run them with:

```bash
cargo test -p sure_copy_core
```

Run the benchmark suite with:

```bash
cargo bench -p sure_copy_core
```

## Logging

`sure_copy_core` emits runtime logs through the `log` facade. A host application or test binary can enable them with `env_logger`.

Example:

```rust
env_logger::Builder::from_env(
    env_logger::Env::default().default_filter_or("sure_copy_core=info"),
)
.init();
```

For local debugging, a useful default is:

```bash
RUST_LOG=sure_copy_core=debug cargo test -p sure_copy_core -- --nocapture
```

## Benchmarks

The crate includes Criterion benchmarks under `sure_copy_core/benches/` for four high-value paths:

- `checksum`: SHA-256 throughput on representative file sizes
- `fs_walk`: recursive directory traversal throughput
- `sqlite_recovery`: SQLite startup recovery latency as task and checkpoint counts grow
- `persistent_copy`: end-to-end durable copy throughput for single-destination, multi-destination, and post-write verification workloads

These benchmarks are designed for regression tracking more than absolute cross-machine comparison. Use the same machine, filesystem type, and dataset profile when comparing runs.
