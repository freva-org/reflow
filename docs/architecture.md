# Architecture

Reflow has four main layers.

## 1. Definition layer

- `Flow` — reusable task groups
- `Workflow` — runtime orchestration (extends `Flow`)
- `@wf.job()` — task decorators

This is where you describe the DAG.

## 2. Parameter and typing layer

- `Param` — CLI parameter metadata
- `Result` — data dependency declarations
- `RunDir` — working directory injection
- Type inspection helpers in `params.py`

This layer turns Python function signatures into CLI flags, typed data
edges, and wire mode inference.

## 3. Persistence layer

- `manifest.py` — typed JSON serialisation (Path, datetime, UUID, …)
- `stores/sqlite.py` — SQLite-backed manifest store
- `stores/records.py` — typed dataclass records (`RunRecord`, `TaskSpecRecord`, `TaskInstanceRecord`)
- `cache.py` — Merkle-DAG identity hashing and verification
- `results.py` — file-based worker result protocol

## 4. Execution layer

- `executors/__init__.py` — `Executor` ABC, `JobResources`, key normalisation
- `executors/slurm.py` — Slurm backend
- `executors/pbs.py` — PBS Pro / OpenPBS / Torque backend
- `executors/lsf.py` — LSF backend
- `executors/sge.py` — SGE / UGE backend
- `executors/flux.py` — Flux backend
- `executors/local.py` — local subprocess backend
- `executors/helpers.py` — `default_executor()`, `resolve_executor()`

Each executor translates `JobResources` into scheduler-native CLI
commands and handles array jobs, dependency chaining, cancellation,
and state queries.

## Workflow subpackage

The `Workflow` class is split across a subpackage for maintainability:

```text
workflow/
├── __init__.py      — re-exports (Workflow + public helpers)
├── _core.py         — Workflow class: submit, validate, cancel, retry, status, describe
├── _dispatch.py     — DispatchMixin: dispatch loop, cache, result wiring, fan-out
├── _worker.py       — WorkerMixin: task execution with signal handling
└── _helpers.py      — make_run_id, build_kwargs, resolve_index
```

The MRO is `Workflow → DispatchMixin → WorkerMixin → Flow → object`.
External code imports `from reflow.workflow import Workflow` as before.

## Execution flow

1. `wf.submit(...)` validates the graph and checks required parameters.
2. Reflow opens the shared manifest database.
3. It inserts a run row, task specs, and initial task instances.
4. A dispatch job is submitted through the configured executor.
5. The dispatcher ingests worker result files, resolves upstream outputs,
   tries the Merkle cache, and submits runnable tasks.
6. Workers execute Python callables and write result files to
   `<run_dir>/results/`.
7. The dispatcher chains a follow-up dispatch with a scheduler
   dependency on all submitted jobs.
8. Steps 5–7 repeat until the graph is complete.

## Key normalization

Users write scheduler-agnostic keys (`partition`, `queue`, `account`).
Each executor declares a `_KEY_ALIASES` mapping that rewrites these
to the backend's native vocabulary before rendering CLI flags.  This
means a workflow is portable across schedulers without config changes.

## Why the shared store matters

A separate database in every run directory makes inspection and
cache reuse harder.  Using one shared manifest store gives a cleaner
split:

- **run directory** — files, logs, outputs
- **manifest store** — workflow history, task states, cache records
