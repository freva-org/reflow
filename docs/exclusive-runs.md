# One active run per target

Some workflows must never run twice at the same time for the same
target, for example two conversions writing into one output store.
Declare the parameters that name such targets with `exclusive`:

```python
wf = Workflow("era5-healpix", exclusive=["target"])


@wf.job()
def init(target: Annotated[str, Param(help="Output location")]) -> list[str]:
    ...
```

When a run starts, reflow *claims* the value of `target` in the
manifest store.  A second run with the same value is refused while the
first one is active:

```console
$ python convert.py submit --run-dir /scratch/r2 --target /scratch/out/era5
Cannot start a new run of 'era5-healpix':
  target=/scratch/out/era5 is held by run era5-healpix-20261029-3f9c
  (user k204230, created 2026-10-29T01:12): a dispatch pass is queued or running.
Wait for that run to finish, or cancel it with `cancel <run-id>` if it
should not continue.
```

The command exits with status 2.  In Python, `submit`, `run_local` and
`retry_failed` raise `reflow.RunConflictError`.

## What counts as the same target

- Values are normalised: local paths are resolved (`./out` and the
  absolute path of the same directory are one target), URLs such as
  `s3://bucket/era5/` lose trailing slashes.
- Keys are **global**: a run of another workflow claiming the same
  value conflicts too, even if its parameter has a different name.
- A list value claims every element.
- For derived targets, pass a function that maps the run parameters to
  the values to claim:

  ```python
  wf = Workflow("yearly", exclusive=lambda p: [f"s3://bucket/{p['year']}"])
  ```

## When a claim is released

- When the run finishes, fails or is cancelled.
- When `run_local` returns or raises.
- `retry` claims the targets again before resubmitting; if another run
  holds them by then, the retry is refused.

## Runs that died

A run whose dispatch job was cancelled or killed together with its
tasks never finishes on its own, so its claim would block the target
forever.  Before reporting a conflict, reflow therefore checks whether
the holding run still has anything in the scheduler.

A running run always does: after each pass, its next dispatch job sits
in the queue, waiting for the task jobs of that pass.  So reflow asks
the live queue (`job_status`, see [Schedulers](schedulers.md)) about
the run's recorded dispatch jobs, newest first, and about the jobs of
its instances that have no result yet:

- If any of them is queued or running, the claim stands.
- If all of them have ended, the run is marked `FAILED`, its claims are
  released, and the new run proceeds.  The holder's owner can still
  `retry` it later.
- If the scheduler cannot tell (the query failed, or a `run_local` run
  on another host), the claim stands and the conflict is reported.
  Cancel the old run to release it.

This works the same on Slurm, PBS, LSF, SGE and Flux, and needs no
accounting database.

The check only reads the holding run's state; it never touches its
tasks, which may belong to another user.

## Limits

Claims only protect runs that use the **same manifest store**.  The
default store is per user (`~/.cache/reflow/manifest.db`), so two
people, each with their own store, do not see each other's claims.
Point `REFLOW_STORE_PATH` at a shared location for team-wide protection.
Runs started with reflow versions without claims hold none.
