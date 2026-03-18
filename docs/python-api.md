# Python API

## Main imports

```python
from reflow import (
    Workflow,
    Flow,
    Run,
    Param,
    Result,
    RunDir,
    SlurmExecutor,
    LocalExecutor,
    ManifestCodec,
)
```

## `Workflow`

Important methods:

- `include()`
- `validate()`
- `cli()`
- `submit()`
- `submit_run()`
- `dispatch()`
- `worker()`
- `cancel_run()`
- `retry_failed()`
- `run_status()`
- `describe_typed()`
- `describe()`

## `Run`

The `Run` object is the user-facing control handle returned by `wf.submit()`.

Important methods:

- `status()`
- `cancel()`
- `retry()`

## `Flow`

Important methods:

- `job()`
- `array_job()`
- workflow composition via `include()`

## `ManifestCodec`

The builtin manifest codec is responsible for JSON-safe conversion and typed
round-tripping for stored payloads.
