# Changelog

All notable changes to this project will be documented in this file.
## v2610.0.0
- **One active run per target**: `Workflow(..., exclusive=["target"])`
  refuses a new run while another active run (of any workflow using the
  same store) holds the same target.  Claims are released when a run
  finishes, fails or is cancelled.  Claims of runs that have nothing
  queued or running any more are released automatically; this check
  uses `job_status` and so works on every backend.  See
  *One run per target*.
- **Fix: Flux array jobs.** `--cc` creates one job per element, but
  reflow kept only the last id, so `cancel` stopped one element and the
  follow-up dispatch waited for one element only.  reflow now keeps
  every element's id, names elements `<name>.<index>`, writes one
  `--dependency=afterany:ID` per job (the comma form was invalid), and
  cancels in batches.  `array_parallelism` is ignored with a warning,
  since Flux has no such limit.
- New `Executor.job_status(job_id)`: whether a job is still queued or
  running, from the live queue of every backend (`squeue`, `qstat`,
  `bjobs`, `qstat -j`, `flux jobs`).  No accounting database needed.
- **Fix: killed workers no longer leave a run hanging.** A worker killed
  without writing a result (out of memory, time limit, node failure)
  left its instance `SUBMITTED`/`RUNNING` forever.  The follow-up
  dispatch job, which the scheduler only starts after the awaited jobs
  ended, now marks such instances `FAILED` with the scheduler's reason
  where available.  Works on every backend.  `retry` also picks them up
  (via `job_status`).  `REFLOW_RESULT_GRACE` sets how long to wait for
  late result files (default 10 s).
- **Fix: parameters are part of the cache identity.** A task's cache
  identity now includes the `Param` values its function takes (CLI
  parameters and task-local overrides). Before, a task was reused from
  the cache even when called with different parameter values, e.g. a
  changed `--target` returned the previous result. Parameters a task
  does not take, and `RunDir`, are still ignored, so unrelated flags or
  a new run directory keep the cache.
- Local runner and scheduler dispatch now compute the identity through
  one shared helper, so both paths always agree.

## v2604.4.0
- **Improved `runs` command**: shows the last 20 runs by default instead
  of all. New flags `--last N`, `--all`, `--since`, `--until`, and
  `--status` for filtering.
- **Improved `status` command**: failed array elements are now shown
  automatically beneath each task summary. New `--errors` flag prints
  error tracebacks (truncated to 15 lines).
- **Bulk cancel**: `cancel` now accepts multiple run IDs.  A
  confirmation prompt is shown unless `--yes` is passed.
- **Python API**: `run_status()` returns a `failed_instances` key.
  New `cancel_runs()` method on `Workflow`.  `Run.status()` accepts
  an `errors=` parameter.  `store.list_runs()` accepts `limit`,
  `since`, `until`, and `status` keyword arguments.
## v2604.3.0
- Added `--force` and `--force-task` for forcing tasks / all DAG to submit.
## v2604.2.0
- Added suggestions for failure due to typos in the dependency flows.
- Added `broadcast` option to avoid splitting results for array jobs.
- Added local job execution mode without workload manager.
## v2604.1.0
- `@job` decorator can spawn array jobs.
## v2604.0.1
- Initial release
