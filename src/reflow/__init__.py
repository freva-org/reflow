"""reflow -- decorator-based HPC workflow engine.

Define tasks with Python decorators, wire data flow with ``Result``
annotations, and submit to Slurm with an auto-generated CLI.
"""

__version__ = "1.0.0a1"

from .flow import Flow, JobConfig, TaskSpec
from .workflow import Workflow
from .run import Run
from .params import Param, Result, RunDir
from ._types import RunState, TaskState
from .signals import TaskInterrupted
from .executors import Executor, JobResources
from .executors.slurm import SlurmExecutor
from .executors.local import LocalExecutor
from .config import Config, load_config

__all__ = [
    "Flow",
    "Workflow",
    "Run",
    "Param",
    "Result",
    "RunDir",
    "Config",
    "load_config",
    "TaskInterrupted",
    "JobConfig",
    "TaskSpec",
    "RunState",
    "TaskState",
    "Executor",
    "JobResources",
    "SlurmExecutor",
    "LocalExecutor",
]
