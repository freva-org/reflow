# Development

## Setup

```bash
git clone https://github.com/freva-org/reflow
cd reflow
pip install -e ".[dev]"
```

## Running tests

```bash
pytest -v tests/
tox -e test
```

## Linting and type checking

```bash
ruff check src/
mypy src/reflow
tox -e lint
tox -e types
```

## Building docs

```bash
pip install -e ".[docs]"
mkdocs serve
```

## What to keep stable

At this stage, the most important surfaces to keep stable are:

- public decorator API (`@wf.job()`, `Param`, `Result`, `RunDir`)
- `Run` control surface (`.status()`, `.cancel()`, `.retry()`)
- `Executor` protocol (`.submit()`, `.cancel()`, `.job_state()`, `.dependency_options()`)
- manifest storage behaviour and cache semantics
- CLI subcommand names and flag conventions
