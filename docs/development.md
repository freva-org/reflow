# Development

## Test suite

```bash
pytest
```

## Linting and type checking

```bash
ruff check .
mypy reflow
```

## Local docs with MkDocs

Create a `mkdocs.yml` at repository root from `docs/mkdocs.yml.example`, then run:

```bash
mkdocs serve
```

## What to keep stable

At this stage, the most important things to keep stable are:

- public decorator API
- `Run` control surface
- manifest storage behavior
- cache semantics
- executor protocol

## Good next refactors

- split large orchestration helpers in `workflow.py`
- make backend metadata models more explicit
- add more focused tests for retry and invalidation paths
- prepare for a database-backed multi-user service layer
