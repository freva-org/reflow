# Overview

`reflow` is a decorator-based Python workflow engine for HPC-oriented task
graphs. It lets you define tasks as Python functions, infer dependencies from
type annotations, submit work through an executor such as Slurm, and persist
run/task state in a manifest store.

## What it is good at

- defining DAGs with normal Python functions
- generating a CLI from function signatures
- wiring task dependencies through `Result(...)`
- handling singleton, fan-out, gather, and chain patterns
- storing run and task manifests in SQLite with no heavy dependencies
- exposing run control through a `Run` object and a CLI

## Main building blocks

- `Flow` – reusable collection of task definitions
- `Workflow` – orchestration object that validates, submits, dispatches, and tracks runs
- `Run` – user-facing handle for status, cancel, and retry
- `Param`, `Result`, `RunDir` – annotation helpers for CLI and data wiring
- `SqliteStore` – manifest persistence layer
- `SlurmExecutor`, `LocalExecutor` – backend submission adapters
- `ManifestCodec` – builtin typed serializer for manifest payloads
