# CLI guide

The CLI is generated from the workflow definition and implemented with the
standard library `argparse` module.

## Common commands

```bash
python demo_flow.py dag
python demo_flow.py describe
python demo_flow.py submit --help
python demo_flow.py dispatch --help
python demo_flow.py worker --help
```

## Public commands

### `dag`

Print the workflow graph.

### `describe`

Return a workflow description. Internally this is derived from the typed workflow
description model and then converted to JSON-safe output.

### `submit`

Create a run, persist its manifest rows, and start dispatching runnable tasks.

## Internal commands

### `dispatch`

Used by the execution path to continue scheduling and submission. This is an
internal command, not the main user entrypoint.

### `worker`

Used to execute a task instance and report its result or failure back to the
store.

## Parameter mapping

`Param(...)` annotations become CLI flags. `Literal[...]` annotations become
argument choices automatically.
