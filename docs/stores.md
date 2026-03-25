# Manifest store

Reflow persists run metadata, task states, cache identities, and
outputs in a **shared SQLite manifest database**.

## Why a shared store

Instead of creating one database inside every run directory, the
default store lives in the user's cache directory:

```text
~/.cache/reflow/manifest.db
```

This gives you:

- run inspection across multiple run directories
- cross-run cache reuse
- one place to look for workflow history
- an easier migration path to a future service-backed database

## Path resolution

The store path is resolved in order:

1. `REFLOW_STORE_PATH` environment variable
2. `defaults.store_path` in the config file
3. XDG cache directory (`~/.cache/reflow/manifest.db`)

## Overriding the store path

For project-specific manifests or shared team databases:

```toml
[defaults]
store_path = "/scratch/shared/reflow/manifest.db"
```

From the CLI:

```bash
python flow.py submit \
  --run-dir /scratch/run-001 \
  --store-path /scratch/shared/reflow.db
```

From Python:

```python
from reflow.stores.sqlite import SqliteStore

store = SqliteStore("/scratch/shared/reflow.db")
run = wf.submit(run_dir="/scratch/run-001", store=store, start="2026-01-01")
```

## What the store contains

The `SqliteStore` manages:

- **Run records** — run ID, workflow name, user, parameters, status
- **Task specs** — per-run task configuration and dependencies
- **Task instances** — state machine (`PENDING` → `RUNNING` → `SUCCESS` / `FAILED`), input payloads, output values, identity and output hashes
- **Cache lookups** — find previous successful instances by identity hash

## Typed internal records

The store uses dataclass records internally:

- `RunRecord`
- `TaskSpecRecord`
- `TaskInstanceRecord`

This keeps the persistence code strict without breaking the public
store API.

## Run directories still matter

The run directory is still used for:

- task logs (stdout / stderr)
- task-produced files
- worker result files (ingested by the dispatcher)

Only the manifest metadata moved out of the run directory.
