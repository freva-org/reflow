# CLI reference

## Main commands

### `submit`

Create a new run and submit the dispatcher.

```bash
python flow.py submit --run-dir /scratch/r1 --start 2026-01-01 --bucket demo
```

### `status`

Show status for a run.

```bash
python flow.py status <run-id>
```

Because the manifest database is shared, `status` can usually find the run without `--run-dir`.

### `runs`

List known runs for the current workflow.

```bash
python flow.py runs
```

### `cancel`

Cancel active task instances.

```bash
python flow.py cancel <run-id>
python flow.py cancel <run-id> --task convert
```

### `retry`

Retry failed or cancelled instances.

```bash
python flow.py retry <run-id>
python flow.py retry <run-id> --task convert
```

## Hidden internal commands

Reflow also has internal `dispatch` and `worker` commands. These are used by the executor and workers and are intentionally hidden from normal help output.
