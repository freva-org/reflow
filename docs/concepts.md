# Core concepts

## `Flow`

A `Flow` is a reusable group of task definitions. You can include one flow in
multiple workflows, optionally with a prefix.

## `Workflow`

A `Workflow` is the runtime/orchestration layer. It knows how to:

- validate the graph
- materialize run and task records
- resolve inputs from upstream outputs
- dispatch work through an executor
- read and update manifest state

## `Param`

`Param` marks a function parameter as a CLI/runtime input.

```python
start: Annotated[str, Param(help="Start date")]
```

## `Result`

`Result` expresses dependency and data flow.

```python
item: Annotated[str, Result(step="prepare")]
```

The upstream task becomes a dependency automatically, and the downstream input is
filled from the upstream output.

## `RunDir`

`RunDir` is an injected parameter used for per-run working directories. It does
not appear on the CLI as a normal task argument.

## Task kinds

### `job`

A singleton task.

### `array_job`

A task that fans out over array items.

## Data flow patterns

- **fan-out** – one upstream collection becomes many task instances
- **gather** – many upstream array outputs are collected into one downstream task
- **chain** – array item `i` feeds downstream array item `i`
- **flatten + gather** – nested list-like outputs are collapsed before gathering

## Caching

The project uses content-based task identity hashing. A task instance identity is
based on:

- task name
- version string
- direct inputs
- upstream output hashes

If a matching successful result already exists, the output can be reused instead
of resubmitting work.

## Typed manifest serialization

The manifest layer uses a builtin typed serializer so values such as `Path`,
`datetime`, `UUID`, enums, tuples, and dataclasses can round-trip cleanly through
JSON storage.
