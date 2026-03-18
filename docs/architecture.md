# Architecture

Reflow has four main layers.

## 1. Definition layer

- `Flow`
- `Workflow`
- task decorators such as `job()` and `array_job()`

This is where you describe the DAG.

## 2. Parameter and typing layer

- `Param`
- `Result`
- `RunDir`
- type inspection helpers in `params.py`

This is where Reflow turns Python signatures into CLI flags and typed data edges.

## 3. Persistence layer

- `manifest.py`
- `stores/sqlite.py`
- `stores/records.py`

This layer persists run metadata, task specs, task instances, cache identities, and outputs.

## 4. Execution layer

- `executors/slurm.py`
- `executors/local.py`
- worker entrypoints in `Workflow`

This layer translates runnable tasks into backend jobs and writes their final state back to the store.

## Execution flow

1. `wf.submit(...)` validates the graph
2. Reflow opens the shared manifest database
3. it inserts a run row and task specs
4. it creates initial task instances
5. a dispatch job examines which tasks are runnable
6. workers execute Python callables
7. outputs and state changes are written back into the manifest store

## Why the shared store matters

A run directory is good for logs and files, but a separate database in every run directory makes inspection and cache reuse harder.

Using one shared manifest store gives you a cleaner split:

- **run directory** = files, logs, outputs
- **manifest store** = workflow history, task states, cache records
