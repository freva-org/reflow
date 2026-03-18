# Executors and stores

## Executors

Executors translate runnable task instances into backend jobs.

### `SlurmExecutor`

Uses `sbatch`, `scancel`, and `squeue`/`sacct`-style interactions to submit,
cancel, and inspect jobs.

### `LocalExecutor`

Useful for testing and local runs. It launches work directly without involving a
batch scheduler.

## Store layer

The current store implementation is `SqliteStore`.

Responsibilities:

- initialize the manifest database
- insert and update run rows
- insert task specs and task instances
- persist task outputs and output hashes
- support cache lookup
- provide status summaries

## Typed internal records

The SQLite store now uses internal dataclass records instead of relying only on
loose row dictionaries.

Key records:

- `RunRecord`
- `TaskSpecRecord`
- `TaskInstanceRecord`

This makes the persistence code stricter without breaking the existing public
store API.
