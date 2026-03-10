# SureCopy

Copy data in a better way.

## Overview

SureCopy is a workspace for building a reliable, resume-friendly copy application with a native desktop frontend and a reusable Rust core.

At a high level, the project aims to provide:

- reliable `1 -> N` file copy
- restart-safe task orchestration
- structured progress and reporting
- extensible processing hooks around the copy pipeline

## Workspace Structure

### `sure_copy_core`

The Rust core crate that implements copy orchestration, durability, progress reporting, and extension points.

Detailed crate documentation lives in [`sure_copy_core/README.md`](./sure_copy_core/README.md).

### `src/` + `src-tauri/`

The desktop application layers:

- `src/`: frontend UI
- `src-tauri/`: native shell / Tauri integration

## Documentation Map

- Workspace overview: `README.md` (this file)
- Core runtime and API details: [`sure_copy_core/README.md`](./sure_copy_core/README.md)
- Core crate source layout: `sure_copy_core/src/`

If you want the detailed task model, flow model, retry behavior, durability boundaries, or pipeline extension contracts, go straight to [`sure_copy_core/README.md`](./sure_copy_core/README.md).

## Development

### Core crate

Run core tests:

```bash
cargo test -p sure_copy_core
```

Run core benchmarks:

```bash
cargo bench -p sure_copy_core
```

### Desktop app

Install frontend dependencies:

```bash
bun install
```

Start the desktop development app:

```bash
bun run tauri dev
```

## Current Focus

The core of the project is already where most of the reliability and architecture work happens. The root workspace README intentionally stays lightweight and delegates implementation details to the crate-level docs so that project navigation stays clear and maintenance stays sane.
