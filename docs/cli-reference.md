# CLI reference

Call `wf.cli()` at the end of your script to get a full command-line
interface. Task parameters annotated with `Param` become CLI flags.

## Commands

### `submit`

Create a new run and start dispatching:

```console
$ python pipeline.py submit --run-dir /scratch/r1 --source data.csv
```

Skip the cache and re-run everything:

```console
$ python pipeline.py submit --run-dir /scratch/r1 --source data.csv --force
```

Skip the cache for specific tasks only:

```console
$ python pipeline.py submit --run-dir /scratch/r1 --source data.csv --force-tasks extract transform
```

### `status`

Show status for a run:

```console
$ python pipeline.py status RUN_ID
$ python pipeline.py status RUN_ID --task process
```

### `cancel`

Cancel active jobs:

```console
$ python pipeline.py cancel RUN_ID
$ python pipeline.py cancel RUN_ID --task process
```

### `retry`

Retry failed or cancelled tasks:

```console
$ python pipeline.py retry RUN_ID
$ python pipeline.py retry RUN_ID --task process
```

### `runs`

List all runs:

```console
$ python pipeline.py runs
```

### `dag`

Print the task dependency graph:

```console
$ python pipeline.py dag
```

### `describe`

Print the full workflow manifest as JSON:

```console
$ python pipeline.py describe
```

## Options

### `--run-dir`

Shared working directory. Required for `submit`.

### `--force`

Skip the Merkle cache entirely and re-run all tasks.
Only applies to `submit`.

### `--force-tasks`

Skip the cache for specific tasks only. Accepts one or more task
names. Only applies to `submit`.

### `--store-path`

Path to the SQLite manifest database.  Overrides the default
(`~/.cache/reflow/manifest.db`).

### `--task`

Filter `status`, `cancel`, or `retry` to a single task.

### `--version`

Print the reflow version.
