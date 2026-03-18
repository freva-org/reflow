# Reflow

`reflow` is a decorator-based Python workflow engine for HPC-oriented DAGs.
It lets you define tasks as normal Python functions, infer dependencies from
annotations, submit work through an executor such as Slurm, and persist run/task
state in a manifest store.

## What you will find here

- [Overview](overview.md)
- [Getting started](getting-started.md)
- [Core concepts](concepts.md)
- [CLI guide](cli.md)
- [Python API](python-api.md)
- [Executors and stores](executors-and-stores.md)
- [Architecture](architecture.md)
- [Development](development.md)
- [Future service API notes](future-service-api.md)

## Design goals

- minimal dependency footprint
- typed workflow definitions and manifest payloads
- HPC-first execution model
- a clean path toward additional backends
- a future web app and database-backed control plane
