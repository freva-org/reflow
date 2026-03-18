"""Interactive run handle for reflow.

A [`Run`][Run] wraps a ``run_id`` together with its workflow and
store so that every operation is a single method call.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .executors import Executor
    from .stores import Store
    from .workflow import Workflow


class Run:
    """Bound handle to a submitted run.

    Parameters
    ----------
    workflow : Workflow
        The workflow that owns this run.
    run_id : str
        Unique run identifier.
    run_dir : Path
        Shared working directory.
    store : Store
        Manifest store.

    Examples
    --------
    >>> run = workflow.submit(run_dir="/scratch/run1", start="2025-01-01")
    >>> run.status()
    >>> run.cancel()
    >>> run.retry("convert")

    """

    def __init__(
        self,
        workflow: Workflow,
        run_id: str,
        run_dir: Path,
        store: Store,
    ) -> None:
        self.workflow: Workflow = workflow
        self.run_id: str = run_id
        self.run_dir: Path = Path(run_dir)
        self.store: Store = store

    def __repr__(self) -> str:
        return (
            f"Run(id={self.run_id!r}, workflow={self.workflow.name!r}, "
            f"dir={str(self.run_dir)!r})"
        )

    def status(
        self,
        task: str | None = None,
        as_dict: bool = False,
    ) -> dict[str, Any] | None:
        """Show or return the current run status.

        Parameters
        ----------
        task : str or None
            Filter by task name.
        as_dict : bool
            If ``True``, return the raw dict instead of printing.

        """
        info = self.workflow.run_status(self.run_id, self.store)

        if as_dict:
            return info

        run = info["run"]
        print(f"Run      {self.run_id}")
        print(f"Workflow {run.get('graph_name', '?')}")
        print(f"Status   {run.get('status', '?')}")
        print(f"Created  {run.get('created_at', '?')}")
        print()

        summary = info["summary"]
        try:
            topo = self.workflow._topological_order()
            ordered = [t for t in topo if t in summary]
            ordered += [t for t in summary if t not in topo]
        except ValueError:
            ordered = sorted(summary)

        for tname in ordered:
            if task and tname != task:
                continue
            states = summary[tname]
            parts = [f"{s}={n}" for s, n in sorted(states.items())]
            print(f"  {tname:20s}  {', '.join(parts)}")

        if task:
            print()
            for inst in info["instances"]:
                if inst.get("task_name") != task:
                    continue
                idx = inst.get("array_index")
                idx_str = f"[{idx}]" if idx is not None else "   "
                state = inst.get("state", "?")
                job_id = inst.get("job_id", "-")
                print(f"    {idx_str:6s}  {state:12s}  job={job_id}")

        return None

    def cancel(
        self,
        task: str | None = None,
        executor: Executor | None = None,
    ) -> int:
        """Cancel active task instances.

        Parameters
        ----------
        task : str or None
            Cancel only this task.
        executor : Executor or None
            Workload-manager executor.

        Returns
        -------
        int
            Number of instances cancelled.

        """
        n = self.workflow.cancel_run(
            self.run_id,
            self.store,
            task_name=task,
            executor=executor,
        )
        print(f"Cancelled {n} task instance(s).")
        return n

    def retry(
        self,
        task: str | None = None,
        executor: Executor | None = None,
        verify: bool = True,
    ) -> int:
        """Retry failed or cancelled instances.

        By default, cached upstream outputs are verified (file
        existence for ``Path`` types, custom callables) before
        resubmitting.  Pass ``verify=False`` to skip verification
        and trust the Merkle identity.

        Parameters
        ----------
        task : str or None
            Retry only this task.
        executor : Executor or None
            Workload-manager executor.
        verify : bool
            If ``True`` (default), verify cached upstream outputs.

        Returns
        -------
        int
            Number of instances marked for retry.

        """
        n = self.workflow.retry_failed(
            self.run_id,
            self.store,
            self.run_dir,
            task_name=task,
            executor=executor,
            verify=verify,
        )
        print(f"Marked {n} task instance(s) for retry.")
        return n
