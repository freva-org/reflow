# Caching and retries

Reflow uses content-addressed hashing to skip tasks whose inputs
haven't changed.

## How it works

Each task instance gets an **identity hash** built from:

- the task name and version string
- the input parameters
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

Skip the cache entirely:

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
