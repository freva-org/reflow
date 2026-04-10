# Changelog

All notable changes to this project will be documented in this file.
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
- **Test refactor**: test suite reorganised from 6 files into 13
  module-aligned files for easier navigation and extension.
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
