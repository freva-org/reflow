"""slurm_flow — decorator-based Slurm DAG engine.

Define tasks with Python decorators, submit them as Slurm jobs, and
track progress through a shared SQLAlchemy-backed manifest database.

Quick start
-----------
>>> from typing import Annotated
>>> from slurm_flow import Graph, Param
>>> graph = Graph("my_pipeline")
>>>
>>> @graph.job(cpus=2, time="00:10:00")
... def prepare(
...     start: Annotated[str, Param(help="Start date")],
...     end: Annotated[str, Param(help="End date")],
...     run_dir: str,
... ):
...     return {"files": ["a.nc", "b.nc"]}
>>>
>>> @graph.array_job(expand="prepare.files", cpus=4)
... def convert(item: str, bucket: str):
...     return {"item": item}
>>>
>>> if __name__ == "__main__":
...     graph.cli()
"""

from ._types import RunState, TaskState
from .executors import Executor, JobResources
from .executors.local import LocalExecutor
from .executors.slurm import SlurmExecutor
from .graph import Graph, JobConfig, TaskSpec
from .params import Param

__version__ = "2603.0.0"

__all__ = [
    "__version__",
    "Graph",
    "Param",
    "JobConfig",
    "TaskSpec",
    "RunState",
    "TaskState",
    "Executor",
    "JobResources",
    "SlurmExecutor",
    "LocalExecutor",
]
