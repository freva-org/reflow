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
  gather_sources
  prepare_shared  <- gather_sources
  download_source [array]  <- gather_sources
  convert_source [array]  <- download_source, prepare_shared
  gather_temp_levels  <- convert_source
  finalize [array]  <- gather_temp_levels
```

Array tasks are marked `[array]`; the `<-` arrow lists each task's
dependencies.

Use `--format` to choose a different rendering. The `text`, `mermaid`,
and `dot` formats have no extra dependencies:

```console
$ python pipeline.py dag --format mermaid
flowchart TD
    gather_sources[gather_sources]
    download_source[[download_source]]
    convert_source[[convert_source]]
    gather_sources --> download_source
    download_source --> convert_source
    prepare_shared --> convert_source
```

`mermaid` output renders directly in GitHub and in mkdocs-material, so it
is handy for embedding a workflow diagram in documentation. `dot` emits
Graphviz source you can pipe to the `dot` binary:

```console
$ python pipeline.py dag --format dot | dot -Tpng -o dag.png
```

In both `mermaid` and `dot`, array tasks are drawn with a distinct shape
(a subroutine box `[[...]]` in Mermaid, a doubled border in Graphviz) so
they stand out from singleton tasks.

The `phart` format draws a pretty Unicode diagram directly in the
terminal, with the multi-parent fan-in rendered without duplicating
shared nodes:

```console
$ python pipeline.py dag --format phart
                       [gather_sources]
                              ↓
    <<download_source>>───────+───────────→[prepare_shared]
            ↓                                     ↓
            +────────→<<convert_source>>──────────+
                              ↓
                    →[gather_temp_levels]
                              ↓
                         <<finalize>>
```

Array tasks appear as `<<name>>` and singletons as `[name]`. This format
needs an optional dependency:

```console
$ pip install 'reflow-hpc[pretty]'
```

If the extra is not installed, `--format phart` prints the plain `text`
diagram to stdout and a one-line install hint to stderr, then exits
successfully — so redirecting the output (for example
`dag --format phart > graph.txt`) still produces a clean diagram. Pass
`--ascii` to force 7-bit ASCII instead of Unicode box characters, which
is useful for terminals or logs that do not handle Unicode well.

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

### `--format`

Output format for `dag`: `text` (default), `mermaid`, `dot`, or
`phart`. Only applies to `dag`. See the [`dag`](#dag) command above for
details and examples.

### `--ascii`

For `dag --format phart`, force 7-bit ASCII output instead of Unicode
box-drawing characters. Only applies to `dag`.

### `--version`

Print the reflow version.
