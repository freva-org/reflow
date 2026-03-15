"""slurm_flow — decorator-based Slurm DAG engine.

Define tasks with Python decorators, submit them as Slurm jobs, and
track progress through a shared SQLAlchemy-backed manifest database.

Quick start
-----------
>>> from slurm_flow import Graph
>>> graph = Graph("my_pipeline")
>>>
>>> @graph.job(cpus=2, time="00:10:00")
... def prepare(start: str, end: str, run_dir: str):
...     return {"files": ["a.nc", "b.nc"]}
>>>
>>> @graph.array_job(expand="prepare.files", cpus=4)
... def convert(item: str, bucket: str):
...     return {"item": item}
>>>
>>> if __name__ == "__main__":
...     graph.cli()
"""

from .graph import Graph, JobConfig, TaskSpec
from ._types import RunState, TaskState
from .executors import Executor, JobResources
from .executors.slurm import SlurmExecutor
from .executors.local import LocalExecutor

__all__ = [
    "Graph",
    "JobConfig",
    "TaskSpec",
    "RunState",
    "TaskState",
    "Executor",
    "JobResources",
    "SlurmExecutor",
    "LocalExecutor",
]
