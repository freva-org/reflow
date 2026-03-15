"""Typer-based CLI for slurm_flow.

The ``submit`` command is built dynamically from task function signatures
so that each parameter becomes a ``--flag``.  The remaining commands
(``status``, ``cancel``, ``retry``, ``dispatch``, ``worker``) are
statically defined.
"""

from __future__ import annotations

import inspect
import json
import sys
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    get_args,
    get_origin,
)

import click
import typer

from ._types import TaskState

# ── annotation helpers ────────────────────────────────────────────────


def _unwrap_optional(annotation: Any) -> Tuple[Any, bool]:
    """Remove ``Optional[...]`` and return ``(inner, is_optional)``.

    Parameters
    ----------
    annotation : Any
        Type annotation.

    Returns
    -------
    Tuple[Any, bool]
        Inner type and whether the annotation was optional.
    """
    origin = get_origin(annotation)
    if origin is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0], True
    return annotation, False


def _list_element_type(annotation: Any) -> Optional[Any]:
    """Return the element type if *annotation* is ``List[T]``.

    Parameters
    ----------
    annotation : Any
        Type annotation.

    Returns
    -------
    Any or None
        Element type or ``None``.
    """
    origin = get_origin(annotation)
    if origin in (list, List):
        args = get_args(annotation)
        return args[0] if len(args) == 1 else None
    return None


def _click_type(annotation: Any) -> click.ParamType:
    """Map a Python type to a Click parameter type.

    Parameters
    ----------
    annotation : Any
        Type annotation.

    Returns
    -------
    click.ParamType
    """
    inner, _ = _unwrap_optional(annotation)
    if inner in (inspect.Parameter.empty, Any, str):
        return click.STRING
    if inner is int:
        return click.INT
    if inner is float:
        return click.FLOAT
    if inner is bool:
        return click.BOOL
    return click.STRING


# ── dynamic submit command builder ────────────────────────────────────

# Internal parameters that should not appear on the submit CLI.
_SKIP_PARAMS = frozenset({"item", "run_id"})


def _collect_parameters(
    signatures: Sequence[inspect.Signature],
) -> Dict[str, Dict[str, Any]]:
    """Merge parameters across all task signatures.

    Parameters
    ----------
    signatures : Sequence[inspect.Signature]
        Introspected task signatures.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        ``{param_name: {"type": ..., "required": ..., "default": ...}}``.
    """
    collected: Dict[str, Dict[str, Any]] = {}

    for sig in signatures:
        for param in sig.parameters.values():
            if param.name in _SKIP_PARAMS:
                continue

            inner, is_optional = _unwrap_optional(param.annotation)
            list_elem = _list_element_type(inner)
            has_default = param.default is not inspect.Parameter.empty

            if param.name in collected:
                # widen: if any signature requires this param, it is required
                if not has_default and param.name != "run_dir":
                    collected[param.name]["required"] = True
                continue

            collected[param.name] = {
                "type": _click_type(inner if list_elem is None else list_elem),
                "is_list": list_elem is not None,
                "required": not has_default and param.name != "run_dir",
                "default": param.default if has_default else None,
            }

    return collected


def _make_submit_command(graph: Any) -> click.Command:
    """Build the ``submit`` Click command programmatically.

    Parameters
    ----------
    graph : Graph
        The DAG graph instance.

    Returns
    -------
    click.Command
    """
    from .graph import Graph  # deferred to avoid circular import

    sigs = [spec.signature for spec in graph.tasks.values()]
    params_info = _collect_parameters(sigs)

    # -- build Click parameters ----------------------------------------
    click_params: List[click.Parameter] = []

    # fixed params first
    click_params.append(
        click.Option(
            ["--run-dir"],
            required=True,
            type=click.Path(),
            help="Shared working directory for the run.",
        )
    )
    click_params.append(
        click.Option(
            ["--db-type"],
            default="sqlite",
            show_default=True,
            help="Database backend (sqlite, postgresql, mariadb).",
        )
    )
    click_params.append(
        click.Option(
            ["--db-url"],
            default=None,
            help="Explicit SQLAlchemy database URL.",
        )
    )

    # dynamic params from task signatures
    for pname in sorted(params_info):
        if pname in {"run_dir", "db_type", "db_url"}:
            continue
        info = params_info[pname]
        option_name = f"--{pname.replace('_', '-')}"
        kw: Dict[str, Any] = {
            "type": info["type"],
            "required": info["required"],
            "default": info["default"],
        }
        if info["is_list"]:
            kw["multiple"] = True
        click_params.append(click.Option([option_name], **kw))

    # -- callback ------------------------------------------------------
    def submit_callback(**kwargs: Any) -> None:
        run_dir = Path(kwargs.pop("run_dir"))
        db_type = kwargs.pop("db_type", "sqlite")
        db_url = kwargs.pop("db_url", None)

        # convert tuples from Click's multiple=True back to lists
        parameters: Dict[str, Any] = {}
        for key, value in kwargs.items():
            parameters[key] = list(value) if isinstance(value, tuple) else value

        run_id = graph.submit_run(
            run_dir=run_dir,
            parameters=parameters,
            db_type=db_type,
            db_url=db_url,
        )
        typer.echo(f"run_id   = {run_id}")
        typer.echo(f"run_dir  = {run_dir.expanduser().resolve()}")

    return click.Command(
        name="submit",
        callback=submit_callback,
        params=click_params,
        help="Create a new run and submit the dispatcher.",
    )


# ── app builder ───────────────────────────────────────────────────────


def build_app(graph: Any) -> typer.Typer:
    """Build the full Typer CLI for a graph.

    Parameters
    ----------
    graph : Graph
        The DAG graph.

    Returns
    -------
    typer.Typer
        Ready-to-invoke Typer application.
    """
    app = typer.Typer(
        name=graph.name,
        help=f"slurm_flow DAG: {graph.name}",
        add_completion=False,
    )

    # -- submit (dynamic) ----------------------------------------------
    # We register the Click command directly on the underlying Click group.
    submit_cmd = _make_submit_command(graph)

    # -- status --------------------------------------------------------

    @app.command()
    def status(
        run_id: str = typer.Argument(..., help="Run identifier."),
        run_dir: str = typer.Option(..., help="Shared working directory."),
        db_type: str = typer.Option("sqlite", help="Database backend."),
        db_url: Optional[str] = typer.Option(None, help="SQLAlchemy URL."),
        task: Optional[str] = typer.Option(None, help="Filter by task name."),
        output_json: bool = typer.Option(
            False, "--json", help="Output as JSON."
        ),
    ) -> None:
        """Show the status of a run."""
        info = graph.run_status(
            run_id=run_id,
            run_dir=Path(run_dir),
            db_url=db_url,
            db_type=db_type,
        )

        if output_json:
            typer.echo(json.dumps(info, indent=2, default=str))
            return

        run = info["run"]
        typer.echo(f"Run      {run_id}")
        typer.echo(f"Graph    {run.get('graph_name', '?')}")
        typer.echo(f"Status   {run.get('status', '?')}")
        typer.echo(f"Created  {run.get('created_at', '?')}")
        typer.echo("")

        summary = info["summary"]
        topo = graph._topological_order()
        ordered = [t for t in topo if t in summary]
        ordered += [t for t in summary if t not in topo]

        for tname in ordered:
            if task and tname != task:
                continue
            states = summary[tname]
            parts = [f"{s}={n}" for s, n in sorted(states.items())]
            typer.echo(f"  {tname:20s}  {', '.join(parts)}")

        # show individual instances for a filtered task
        if task:
            typer.echo("")
            instances = [
                i for i in info["instances"] if i.get("task_name") == task
            ]
            for inst in instances:
                idx = inst.get("array_index")
                idx_str = f"[{idx}]" if idx is not None else "   "
                state = inst.get("state", "?")
                job = inst.get("slurm_job_id", "-")
                typer.echo(f"    {idx_str:6s}  {state:12s}  job={job}")

    # -- cancel --------------------------------------------------------

    @app.command()
    def cancel(
        run_id: str = typer.Argument(..., help="Run identifier."),
        run_dir: str = typer.Option(..., help="Shared working directory."),
        db_type: str = typer.Option("sqlite", help="Database backend."),
        db_url: Optional[str] = typer.Option(None, help="SQLAlchemy URL."),
        task: Optional[str] = typer.Option(
            None, help="Cancel only this task."
        ),
    ) -> None:
        """Cancel active tasks in a run."""
        n = graph.cancel_run(
            run_id=run_id,
            run_dir=Path(run_dir),
            db_url=db_url,
            db_type=db_type,
            task_name=task,
        )
        typer.echo(f"Cancelled {n} task instance(s).")

    # -- retry ---------------------------------------------------------

    @app.command()
    def retry(
        run_id: str = typer.Argument(..., help="Run identifier."),
        run_dir: str = typer.Option(..., help="Shared working directory."),
        db_type: str = typer.Option("sqlite", help="Database backend."),
        db_url: Optional[str] = typer.Option(None, help="SQLAlchemy URL."),
        task: Optional[str] = typer.Option(
            None, help="Retry only this task."
        ),
    ) -> None:
        """Retry failed or cancelled tasks."""
        n = graph.retry_failed(
            run_id=run_id,
            run_dir=Path(run_dir),
            db_url=db_url,
            db_type=db_type,
            task_name=task,
        )
        typer.echo(f"Marked {n} task instance(s) for retry.")

    # -- dispatch (internal) -------------------------------------------

    @app.command(hidden=True)
    def dispatch(
        run_id: str = typer.Option(..., help="Run identifier."),
        run_dir: str = typer.Option(..., help="Shared working directory."),
        db_type: str = typer.Option("sqlite", help="Database backend."),
        db_url: Optional[str] = typer.Option(None, help="SQLAlchemy URL."),
    ) -> None:
        """[internal] Inspect the manifest and submit runnable tasks."""
        graph.dispatch(
            run_id=run_id,
            run_dir=Path(run_dir),
            db_url=db_url,
            db_type=db_type,
        )

    # -- worker (internal) ---------------------------------------------

    @app.command(hidden=True)
    def worker(
        run_id: str = typer.Option(..., help="Run identifier."),
        run_dir: str = typer.Option(..., help="Shared working directory."),
        task: str = typer.Option(..., help="Task to execute."),
        db_type: str = typer.Option("sqlite", help="Database backend."),
        db_url: Optional[str] = typer.Option(None, help="SQLAlchemy URL."),
        index: Optional[int] = typer.Option(None, help="Array index."),
    ) -> None:
        """[internal] Execute one task instance."""
        graph.worker(
            run_id=run_id,
            run_dir=Path(run_dir),
            task_name=task,
            index=index,
            db_url=db_url,
            db_type=db_type,
        )

    # -- runs ----------------------------------------------------------

    @app.command()
    def runs(
        run_dir: str = typer.Option(..., help="Shared working directory."),
        db_type: str = typer.Option("sqlite", help="Database backend."),
        db_url: Optional[str] = typer.Option(None, help="SQLAlchemy URL."),
    ) -> None:
        """List all runs in the manifest database."""
        from . import db as _db

        resolved = _db.build_db_url(
            Path(run_dir), db_type=db_type, db_url=db_url
        )
        engine = _db.connect_db(resolved)
        try:
            rows = _db.list_runs(engine, graph_name=graph.name)
        finally:
            engine.dispose()

        if not rows:
            typer.echo("No runs found.")
            return

        for row in rows:
            typer.echo(
                f"  {row['run_id']}  {row['status']:12s}  {row['created_at']}"
            )

    # -- dag -----------------------------------------------------------

    @app.command()
    def dag() -> None:
        """Print the task DAG in topological order."""
        order = graph._topological_order()
        for tname in order:
            spec = graph.tasks[tname]
            deps = graph._effective_dependencies(spec)
            dep_str = f"  <- {', '.join(deps)}" if deps else ""
            tag = " [array]" if spec.config.array else ""
            typer.echo(f"  {tname}{tag}{dep_str}")

    # -- attach the dynamic submit command to the Click group ----------
    # Typer wraps Click internally; we hook in after the group is created.
    _click_group = typer.main.get_group(app)
    _click_group.add_command(submit_cmd, "submit")

    return app
