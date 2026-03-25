# Scheduler backends

Reflow supports five workload managers out of the box.  You write
scheduler-agnostic configuration and reflow translates to the correct
flags at submission time.

## Supported backends

| Backend | `mode` value | Submit | Cancel | Status | Array flag |
|---------|-------------|--------|--------|--------|-----------|
| Slurm | `sbatch` (default) | `sbatch` | `scancel` | `sacct` | `--array` |
| PBS Pro / Torque | `qsub-pbs` | `qsub` | `qdel` | `qstat` | `-J` (Pro) / `-t` (Torque) |
| LSF | `bsub` | `bsub` | `bkill` | `bjobs` | `-J "name[…]"` |
| SGE / UGE | `qsub-sge` | `qsub` | `qdel` | `qstat` | `-t` |
| Flux | `flux` | `flux submit` | `flux cancel` | `flux jobs` | `--cc` |

Use `dry-run` as the mode to log commands without submitting (useful
for testing on a laptop).

## Choosing a backend

### Via config file

```toml
# ~/.config/reflow/config.toml
[executor]
mode = "qsub-pbs"
```

### Via environment variable

```bash
export REFLOW_MODE=bsub
python pipeline.py submit --run-dir /scratch/run1 --start 2025-01-01
```

### Via Python

```python
# Shorthand string
run = wf.submit(run_dir="/scratch/run1", executor="pbs", start="2025-01-01")

# Explicit instance
from reflow import PBSExecutor
exc = PBSExecutor(qsub="/opt/pbs/bin/qsub", array_flag="-t")
run = wf.submit(run_dir="/scratch/run1", executor=exc, start="2025-01-01")
```

Supported shorthand strings: `"local"`, `"pbs"`, `"lsf"`, `"sge"`,
`"flux"`.

## Scheduler-agnostic configuration

Users never need to know which scheduler is active.  Reflow
normalises keys automatically:

- `partition` and `queue` are treated as synonyms.
- `account` works on every backend.

```toml
[executor.submit_options]
partition = "compute"    # → --partition on Slurm, -q on PBS/LSF/SGE, --queue on Flux
account  = "my_project"  # → --account on Slurm, -A on PBS/SGE, -G on LSF
signal   = "B:INT@60"    # pre-termination signal
```

A PBS user can write `queue = "batch"` and switch to Slurm later
without changing their config — reflow maps `queue` to `--partition`
on Slurm.

### How normalisation works

Each executor declares a `_KEY_ALIASES` mapping.  Before rendering
CLI flags, `_normalize_options()` rewrites alias keys to the
backend's native vocabulary:

| Backend | `partition` → | `queue` → |
|---------|--------------|----------|
| Slurm | `partition` (native) | → `partition` |
| PBS | → `queue` | `queue` (native) |
| LSF | → `queue` | `queue` (native) |
| SGE | → `queue` | `queue` (native) |
| Flux | → `queue` | `queue` (native) |

When both the alias and the native key are present, the native key
wins.

## Custom command paths

Override scheduler binary paths when they are not on `$PATH`:

```toml
[executor]
mode = "qsub-pbs"

# PBS-specific
qsub  = "/opt/pbs/bin/qsub"
qdel  = "/opt/pbs/bin/qdel"
qstat = "/opt/pbs/bin/qstat"
```

Or via environment variables:

```bash
export REFLOW_QSUB=/opt/pbs/bin/qsub
export REFLOW_QDEL=/opt/pbs/bin/qdel
```

## Environment variables

All config values can be overridden with `REFLOW_*` environment
variables:

| Variable | Effect |
|----------|--------|
| `REFLOW_MODE` | Scheduler backend |
| `REFLOW_PYTHON` | Python interpreter for worker jobs |
| `REFLOW_PARTITION` | Default partition / queue |
| `REFLOW_ACCOUNT` | Default account / project |
| `REFLOW_SBATCH`, `REFLOW_SCANCEL`, `REFLOW_SACCT` | Slurm command paths |
| `REFLOW_QSUB`, `REFLOW_QDEL`, `REFLOW_QSTAT` | PBS / SGE command paths |
| `REFLOW_BSUB`, `REFLOW_BKILL`, `REFLOW_BJOBS` | LSF command paths |
| `REFLOW_FLUX` | Flux command path |

## Dependency chaining

Each executor knows how to express job dependencies in its scheduler's
native syntax.  The dispatch loop calls `executor.dependency_options()`
to build the dependency specification for follow-up dispatch jobs:

| Backend | Dependency syntax |
|---------|------------------|
| Slurm | `--dependency=afterany:ID1:ID2` |
| PBS | `-W depend=afterany:ID1:ID2` |
| LSF | `-w "ended(ID1) && ended(ID2)"` |
| SGE | `-hold_jid ID1,ID2` |
| Flux | `--dependency=afterany:ID1,ID2` |

## Array index resolution

Inside a running array job, each scheduler sets a different
environment variable for the array task index.  Reflow checks all
known variables in priority order:

1. `SLURM_ARRAY_TASK_ID`
2. `PBS_ARRAY_INDEX` / `PBS_ARRAYID`
3. `LSB_JOBINDEX`
4. `SGE_TASK_ID`
5. `FLUX_JOB_CC`

The first valid integer wins.  This means workflows are portable
across schedulers without code changes.

## PBS Pro vs Torque

The PBS executor auto-detects the variant from `qstat --version`
output:

- **PBS Pro / OpenPBS** → uses `-J` for array jobs
- **Torque** → uses `-t` for array jobs

You can override this with `array_flag="-t"` when constructing
the executor manually.

## The `LocalExecutor`

For testing without a scheduler, use the local executor:

```python
run = wf.submit(run_dir="/tmp/test", executor="local", start="2025-01-01")
```

It runs tasks as local subprocesses.  Array jobs execute sequentially.

## Dispatch job resources

The internal dispatch/coordinator job has its own resource defaults:

```toml
[dispatch]
cpus = 1
time = "00:10:00"
mem  = "1G"
partition = "my_partition"
account   = "my_account"
```
