# Shared manifest store

Reflow now uses a **shared SQLite manifest database by default**.

Instead of creating one database inside every run directory, the default store path lives in the user's cache directory.

Typical default on Linux:

```text
~/.cache/reflow/manifest.db
```

This is important because it allows:

- run inspection across multiple run directories
- cross-run cache reuse
- one place to look for workflow history
- an easier migration path to a future service-backed database

## How the default path is resolved

Resolution order:

1. `defaults.store_path` in the config file
2. `REFLOW_STORE_PATH`
3. XDG cache directory, usually `~/.cache/reflow/manifest.db`

## Example config

```toml
[defaults]
run_dir = "/scratch/my-runs"
store_path = "/scratch/shared/reflow/manifest.db"
```

## When to override the store path

Override it when you want:

- project-specific manifests
- temporary isolated testing
- a shared filesystem location for a team

CLI:

```bash
python flow.py submit \
  --run-dir /scratch/run-001 \
  --store-path /scratch/shared/reflow.db
```

Python:

```python
from reflow.stores.sqlite import SqliteStore

store = SqliteStore("/scratch/shared/reflow.db")
run = wf.submit(run_dir="/scratch/run-001", store=store, start="2026-01-01")
```

## Run directories still matter

The run directory is still used for:

- task logs
- task-produced files
- workflow-local scratch data

Only the manifest database moved out of the run directory.
