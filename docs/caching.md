# Caching and retries

Reflow uses content-addressed hashing to skip tasks whose inputs
haven't changed.

## How it works

Each task instance gets an **identity hash** built from:

- the task name and version string
- its direct inputs: the `Result` values this instance receives
- the `Param` values the task function takes (CLI parameters and
  task-local overrides); parameters the function does not take, and the
  `RunDir`, are left out, so changing an unrelated parameter or starting
  a new run directory keeps the cache
- the output hashes of all upstream tasks

If a previous run produced a successful result with the same identity,
reflow reuses it instead of submitting a new job.

## Cache invalidation

Bump the `version` field when your task logic changes:

```python
@wf.job(version="2")  # was "1" before the code change
def process() -> str:
    return "new logic"
```

Everything downstream is also recomputed because the upstream output
hash changes.

## Forcing recomputation

From the CLI:

```console
$ python pipeline.py submit --run-dir /tmp/r --source data.csv --force
$ python pipeline.py submit --run-dir /tmp/r --source data.csv --force-tasks process
```

From Python:

```python
# Force everything
run = wf.submit(run_dir="/tmp/r", force=True, source="data.csv")

# Force only specific tasks
run = wf.submit(run_dir="/tmp/r", force_tasks=["process"], source="data.csv")
```

The same works with `run_local`:

```python
run = wf.run_local(run_dir="/tmp/r", force=True, source="data.csv")
```

## Disabling the cache per task

```python
@wf.job(cache=False)
def always_run() -> str:
    return "fresh every time"
```

## Output verification

For tasks that return file paths, reflow can verify that cached files
still exist on disk before reusing them:

```python
run = wf.submit(run_dir="/tmp/r", verify=True, source="data.csv")
```

This checks `Path.exists()` for `Path` and `list[Path]` return types.
You can also supply a custom verification function:

```python
@wf.job(verify=lambda output: Path(output).stat().st_size > 0)
def create_file() -> str:
    return "/tmp/output.csv"
```

## Retries

Failed or cancelled tasks can be retried without rerunning
the whole pipeline:

```python
run.retry()                 # retry all failed tasks
run.retry(task="process")   # retry only "process"
```

Or from the CLI:

```console
$ python pipeline.py retry RUN_ID
$ python pipeline.py retry RUN_ID --task process
```

On retry, upstream cache hits are verified by default so stale
intermediate files are detected.

## Workers that die without a result

A worker reports its outcome by writing a result file, which the next
dispatch pass ingests.  A worker that is killed hard never writes one:
out of memory, a time limit followed by `SIGKILL`, a node failure, or
`scancel`.  Without help its instance would stay `SUBMITTED` or
`RUNNING` forever: nothing downstream starts and the run never
finishes.

reflow catches this on every backend, without asking the scheduler
anything.  After each pass, the follow-up dispatch job is submitted
with an "after any" dependency on the jobs of that pass:

| Backend | Dependency |
|---------|------------|
| Slurm, PBS, Flux | `afterany` |
| LSF | `ended()` |
| SGE | `-hold_jid` |

So when the follow-up pass starts, the scheduler has guaranteed that
all those jobs have ended, successfully or not.  reflow stores their ids
and hands the dispatch job a reference to them (`--after-ref`).  Any of
their instances that still has no result never will, and is marked
`FAILED`:

```text
Job 27152666_42 ended without writing a result (scheduler state:
OUT_OF_MEMORY).  The worker was probably killed (out of memory, time
limit, node failure or cancellation); see the job's log in <run_dir>/logs.
```

The scheduler state in the message is added where the backend can tell
it (Slurm `sacct`, Flux `flux jobs`); otherwise it says `unknown`.  It
is only text: the decision never depends on it.  If the scheduler does
report the job as queued or running again (a requeued job), the
instance is left alone.

This works per instance: if 3 of 500 array elements were killed, only
those 3 fail.  Downstream tasks do not start, the run finishes as
`FAILED` instead of staying `RUNNING`, and `retry` resubmits exactly
the failed elements.

Before failing anything, the dispatcher waits a few seconds and looks
for results again, because result files written on compute nodes can
take a moment to become visible elsewhere.  Set `REFLOW_RESULT_GRACE`
(seconds, default 10, `0` to disable) to change the wait.

### `retry` and killed workers

`retry` runs outside that dependency chain, so it asks the live queue
instead (`Executor.job_status`, see [Schedulers](schedulers.md)).
Instances whose job the queue no longer holds are failed and retried
with the rest.  Jobs that are still queued or running, or whose status
cannot be determined, are left alone.

## The manifest store

Reflow keeps run metadata, task states, and cached outputs in a shared
SQLite database at `~/.cache/reflow/manifest.db`. This gives you
cross-run cache reuse and a single place to inspect workflow history.

The path can be overridden:

```console
export REFLOW_STORE_PATH=/shared/team/manifest.db
```

Or in the config file:

```toml
# ~/.config/reflow/config.toml
[defaults]
store_path = "/shared/team/manifest.db"
```
