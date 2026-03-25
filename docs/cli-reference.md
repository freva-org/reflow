# CLI reference

The CLI is auto-generated from the workflow definition using the
standard library `argparse` module.  Call `wf.cli()` at the end of
your script to activate it.

## Public commands

### `submit`

Create a new run and start dispatching.

```bash
python flow.py submit \
  --run-dir /scratch/r1 \
  --start 2026-01-01 \
  --bucket demo
```

Task parameters annotated with `Param` become CLI flags.
`Literal[...]` annotations become argparse choices.

Optional flags:

- `--store-path PATH` — use a specific manifest database instead of the default

### `status`

Show status for a run.

```bash
python flow.py status <run-id>
```

Because the manifest database is shared, `status` can find the run
without `--run-dir`.

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

### `runs`

List known runs for the current workflow.

```bash
python flow.py runs
```

### `dag`

Print the task dependency graph.

```bash
python flow.py dag
```

### `describe`

Return a JSON workflow manifest with task names, types, resource
configs, dependencies, and CLI parameters.

```bash
python flow.py describe
```

## Internal commands

Reflow also uses `dispatch` and `worker` subcommands internally.
These are invoked by the scheduler (not by users) and are hidden from
normal help output.

- **`dispatch`** — ingests worker results, submits runnable tasks,
  chains a follow-up dispatch with scheduler dependencies.
- **`worker`** — executes a single task instance, writes a result
  file to `<run_dir>/results/`.

## Parameter mapping summary

| Annotation | CLI flag |
|-----------|---------|
| `start: str` | `--start` |
| `start: Annotated[str, Param(help="...")]` | `--start` with help text |
| `chunk: Annotated[int, Param(namespace="local")]` | `--<task>-chunk` |
| `model: Literal["era5", "icon"]` | `--model {era5,icon}` |
| `run_dir: RunDir` | not on CLI (injected) |
| `item: Annotated[str, Result(step="...")]` | not on CLI (wired) |
