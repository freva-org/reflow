"""reflow -- decorator-based HPC workflow engine.

Define tasks with Python decorators, wire data flow with ``Result``
annotations, and submit to Slurm with an auto-generated CLI.
"""

__version__ = "1.0.0a1"

from ._types import RunState, TaskState
from .config import Config, load_config
from .executors import Executor, JobResources
from .executors.local import LocalExecutor
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
    "ManifestCodec",
    "WorkflowDescription",
]
