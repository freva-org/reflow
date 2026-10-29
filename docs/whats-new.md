# Changelog

All notable changes to this project will be documented in this file.
## v2610.1.0
- **Fix: Flux array jobs.** `--cc` creates one job per element, but
  reflow kept only the last id, so `cancel` stopped one element and the
  follow-up dispatch waited for one element only.  reflow now keeps
  every element's id, names elements `<name>.<index>`, writes one
  `--dependency=afterany:ID` per job (the comma form was invalid), and
  cancels in batches.  `array_parallelism` is ignored with a warning,
  since Flux has no such limit.

## v2610.0.0
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
