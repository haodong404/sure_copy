# SureCopy

Copy data in a better way.

## Structure

### sure_copy_core

The core module is responsible for **reliable copying** itself: task orchestration, file transfer, verification, error recovery, report generation, and extensibility.

### Design Goals

1. **Correctness first**: guarantee verifiable copy results with source/destination checks.
2. **Recoverability**: support resume-after-interruption, retries, and traceable failures.
3. **Extensibility**: allow pluggable processing stages (compression, encryption, transcoding) without modifying the core flow. The current runtime keeps pipeline support conservative: serial stage chains are the supported execution model today, while concurrent topologies remain future work.
4. **Observability**: provide a unified event stream and structured reports for GUI display and auditing.
5. **Controllable performance**: make concurrency, buffering, and I/O policies configurable with sensible limits.

### Layered Architecture

1. **Domain Layer**
   - Defines core entities: `CopyTask`, `FilePlan`, `TaskOptions`, `CopyReport`, and `CopyError`.
   - Encapsulates business semantics only, without concrete I/O implementation details.

2. **Orchestrator Layer**
   - Manages task lifecycle (create, run, pause, cancel, resume, complete).
   - Handles task decomposition (directory scan -> file plan -> execution queue).
   - Coordinates multi-task concurrency and resource quotas (threads, bandwidth, file handles).

3. **Pipeline Layer**
   - Defines stage contracts and serial streaming composition.
   - Keeps topology modeling separate from the durable SQLite task runtime.
   - Enforces a consistent stage contract for easy plug-in integration.

4. **Infrastructure Layer**
   - Provides filesystem adapters and checksum implementations used by the runtime.
   - Isolates platform differences (Windows/macOS/Linux path and permission behavior).

5. **Interface Layer**
   - Exposes stable APIs (for Tauri/CLI): submit task, query state, subscribe events, fetch reports.

### Core Capability Design

1. **Data Copy**
   - Supports `1 -> N` multi-destination copy.
   - Supports recursive traversal and include/exclude filtering.
   - Supports overwrite policies (skip, overwrite, rename, timestamp-based decision).
   - Supports file metadata preservation (timestamps, permissions, extended attributes) with platform-aware handling.

2. **Data Checksum** (implemented by source observers and post-write verification)
   - Supports optional source-hash generation during fan-out and recommended post-write verification.
   - Supports multiple algorithms (e.g., `SHA-256`, `BLAKE3`) selected by policy.
   - Routes verification failures into retry/quarantine workflows with detailed diagnostics.

3. **Report Generation**
   - Produces task-level and file-level metrics: totals, success/failure counts, bytes, duration, throughput, retry counts.
   - Uses a JSON-serializable report format for GUI rendering and export.
   - Standardizes error categories: path, permission, I/O, checksum, cancellation/interruption.

4. **Task Template**
   - Template fields include source path, destination list, filters, overwrite policy, concurrency settings, verification policy, and retry policy.
   - Supports template versioning for backward-compatible evolution.
   - Supports execution via template + runtime overrides.

5. **Multi Tasks**
   - Scheduler supports parallel task execution with priority and fairness.
   - Introduces global resource budgeting to avoid contention and system thrashing.
   - Supports task-level pause/resume without impacting other tasks.

6. **Linked Stream**
   - Models processing steps as `Stage` units (compression, encryption, transformation, masking).
   - Current runtime support is a serial stream handoff between stages.
   - The core engine stays stage-agnostic and can grow stricter topology support later.

### Task Lifecycle (Recommended State Machine)

`Created -> Planned -> Running -> Completed`  
Exception branches: `Failed`, `Cancelled`, `Paused`

- `Created`: Task parameters are validated.
- `Planned`: Directory scan is complete and execution plan is generated.
- `Running`: Source fan-out, destination writes, and post-write verification are in progress.
- `Completed/PartialFailed`: Report is finalized and queryable.
- `Failed/Cancelled/Paused`: Recoverable context is preserved.

### Error Handling and Recovery Strategy

1. **Layered error model**: user-friendly error + debug context (source error chain).
2. **Retry strategy**: exponential backoff, max retry count, retryable errors only.
3. **Resume support**: persist file-level progress (completed, failed, pending-retry).
4. **Idempotency guarantees**: repeated execution avoids duplicated side effects.

### Observability Design

1. **Event stream**: `TaskStarted`, `FileStarted`, `FileProgress`, `FileCompleted`, `FileFailed`, `TaskCompleted`.
2. **Progress model**: dual-dimension progress by bytes and file count.
3. **Structured logging**: standardized key fields (`task_id`, `file_path`, `elapsed_ms`, `error_code`).

### Non-Functional Requirements (NFR)

1. Large-file friendly: streaming processing without loading whole files into memory.
2. Resource control: configurable concurrency and buffer sizes with safe defaults.
3. Cross-platform consistency: predictable path/permission/symlink behavior.
4. Backward compatibility: versioned templates and report schemas.

### GUI

1. Make this app just like a native app, with a simple and clean interface.

## Core API Quick Guide (Current Skeleton)

This section documents the current `sure_copy_core` API shape to keep code and docs aligned.

### 1) Create a task with custom flow modes

`CopyTask` now includes flow customization directly via `flow: TaskFlowPlan`.

- Use `CopyTask::new(...)` for safe defaults.
- Override flow modes with `.with_flow(...)`.
- Default source mode is `SourcePipelineMode::ConcurrentWithFanOut`.

Conceptual example:

- Build task: id + source + destinations + options
- Attach source/post-write flow preferences in `TaskFlowPlan`
- Submit task through `SureCopyCoreApi::submit(...)`

### 2) Runtime task access model

`TaskOrchestrator` returns a `Task` handle.

- `Task::id()` returns the stable task id.
- `Task::snapshot()` returns a runtime snapshot (`CopyTask`) including current flow plan.
- Lifecycle methods: `run`, `pause`, `resume`, `cancel`.

### 3) Realtime updates

Subscribe with `Task::subscribe()` (or `SureCopyCoreApi::subscribe(task_id)`) to receive `TaskUpdate` events.

`TaskUpdate` variants:

- `TaskUpdate::State(TaskState)`
- `TaskUpdate::Progress(TaskProgress)`

### 4) Streaming fan-out contract

The durable runtime treats each source file as one flow session.

- One source reader fans chunks out to multiple destination writers.
- `SourceObserverStage` can inspect source chunks without mutating payload.
- `PostWriteStage` runs against a single destination after that destination finishes writing.
- `TaskFlowPlan` selects `SourcePipelineMode` and `PostWritePipelineMode`.

This keeps the copy path streaming-friendly without loading whole file content into memory.

### 5) Minimal async flow

Typical integration flow:

1. Build a `CopyTask` using `CopyTask::new(...)`.
2. Attach custom `TaskFlowPlan` if needed.
3. Call `SureCopyCoreApi::submit(task).await`.
4. Read identity via `task_handle.id()`.
5. Read runtime snapshot via `task_handle.snapshot()`.
6. Subscribe to updates via `api.subscribe(task_id).await`.
7. Consume `TaskUpdate::State` and `TaskUpdate::Progress` in UI loop.

Skeleton snippet:

```rust
let task = CopyTask::new("task-001", src, vec![dst], TaskOptions::default())
   .with_flow(TaskFlowPlan::new());

let handle = api.submit(task).await?;
let task_id = handle.id().to_string();
let snapshot = handle.snapshot();

let mut updates = api.subscribe(&task_id).await?;
while let Ok(update) = updates.recv().await {
   match update {
      TaskUpdate::State(state) => {
         // render state
      }
      TaskUpdate::Progress(progress) => {
         // render progress
      }
   }
}
```

### 6) Flow naming note

The older `PreCopy/PostCopy` pipeline naming has been replaced by the more accurate flow model:

- `SourceObserverStage` / `SourcePipelineMode`
- `PostWriteStage` / `PostWritePipelineMode`

This better matches the actual runtime topology: one source reader, fan-out writes, and per-destination post-write hooks.
