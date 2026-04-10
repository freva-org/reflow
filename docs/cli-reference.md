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

Failed array elements are shown automatically beneath each task:

```console
$ python pipeline.py status RUN_ID
  produce               SUCCESS=1
  compute               FAILED=2, SUCCESS=298
    [47]    FAILED       job=12345
    [183]   FAILED       job=12346
```

Show error tracebacks for failed instances:

```console
$ python pipeline.py status RUN_ID --errors
```

Long tracebacks are truncated to the last 15 lines. JSON output
includes the full `failed_instances` list:

```console
$ python pipeline.py status RUN_ID --json
```

### `cancel`

Cancel active jobs in one or more runs:

```console
$ python pipeline.py cancel RUN_ID
$ python pipeline.py cancel RUN_ID --task process
$ python pipeline.py cancel RUN1 RUN2 RUN3 --yes
```

When cancelling multiple runs, a confirmation prompt is shown unless
`--yes` (or `-y`) is passed.

### `retry`

Retry failed or cancelled tasks:

```console
$ python pipeline.py retry RUN_ID
$ python pipeline.py retry RUN_ID --task process
```

### `runs`

List runs, most recent first.  By default the last 20 are shown:

```console
$ python pipeline.py runs
$ python pipeline.py runs --last 5
$ python pipeline.py runs --all
$ python pipeline.py runs --status FAILED
$ python pipeline.py runs --since 2025-03-01
$ python pipeline.py runs --since 2025-03-01 --until 2025-04-01
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

### `--errors`

Show error tracebacks for failed instances.  Only applies to
`status`.

### `--last N`

Show the last *N* runs.  Default is 20.  Only applies to `runs`.

### `--all`

Show all runs (overrides `--last`).  Only applies to `runs`.

### `--since DATE`

Only show runs created at or after *DATE* (ISO-8601).  Only
applies to `runs`.

### `--until DATE`

Only show runs created before *DATE* (ISO-8601).  Only applies
to `runs`.

### `--status STATE`

Filter by run status (`RUNNING`, `SUCCESS`, `FAILED`, `CANCELLED`).
Only applies to `runs`.

### `--yes`, `-y`

Skip the confirmation prompt when cancelling multiple runs.

### `--version`

Print the reflow version.
