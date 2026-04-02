"""reflow -- decorator-based HPC workflow engine.

Define tasks with Python decorators, wire data flow with ``Result``
annotations, and submit to HPC schedulers with an auto-generated CLI.
"""

__version__ = "2604.3.0"

from ._types import RunState, TaskState
from .config import (
    Config,
    config_path,
    ensure_config_exists,
    load_config,
    write_example_config,
)
from .executors import Executor, JobResources
from .executors.flux import FluxExecutor
from .executors.local import LocalExecutor
from .executors.lsf import LSFExecutor
from .executors.pbs import PBSExecutor
from .executors.sge import SGEExecutor
from .executors.slurm import SlurmExecutor
from .flow import Flow, JobConfig, TaskSpec
from .manifest import ManifestCodec, WorkflowDescription
from .params import Param, Result, RunDir
from .run import Run
from .signals import TaskInterrupted
from .workflow import Workflow

__all__ = [
    "Flow",
    "Workflow",
    "Run",
    "Param",
    "Result",
    "RunDir",
    "Config",
    "config_path",
    "load_config",
    "ensure_config_exists",
    "write_example_config",
    "TaskInterrupted",
    "JobConfig",
    "TaskSpec",
    "RunState",
    "TaskState",
    "Executor",
    "JobResources",
    "SlurmExecutor",
    "PBSExecutor",
    "LSFExecutor",
    "SGEExecutor",
    "FluxExecutor",
    "LocalExecutor",
    "ManifestCodec",
    "WorkflowDescription",
]
