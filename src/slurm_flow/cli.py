"""Argparse-based CLI for slurm_flow.

The ``submit`` subcommand is built dynamically from task function
signatures.  ``Annotated[T, Param(...)]`` metadata controls help text,
short flags, and global/local scoping.

All other subcommands (``status``, ``cancel``, ``retry``, ``runs``,
``dag``, ``dispatch``, ``worker``) are statically defined.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
)

from .params import (
    ResolvedParam,
    collect_params_from_task,
    merge_resolved_params,
)

# ── submit subcommand builder ─────────────────────────────────────────

# Fixed params that should not be duplicated from task signatures.
_FIXED_SUBMIT_PARAMS = frozenset({"run_dir", "db_type", "db_url"})


def _add_submit_parser(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
    graph: Any,
) -> None:
    """Add the ``submit`` subcommand with dynamic options.

    Parameters
    ----------
    subparsers : argparse._SubParsersAction
        Parent subparser group.
    graph : Graph
        DAG graph instance.
    """
    parser = subparsers.add_parser(
        "submit",
        help="Create a new run and submit the dispatcher.",
    )

    # fixed infrastructure flags
    parser.add_argument(
        "--run-dir",
        required=True,
        type=str,
        help="Shared working directory for the run.",
    )
    parser.add_argument(
        "--db-type",
        default="sqlite",
        type=str,
        help="Database backend (sqlite, postgresql, mariadb).  [default: sqlite]",
    )
    parser.add_argument(
        "--db-url",
        default=None,
        type=str,
        help="Connection URL for database.",
    )

    # collect and merge from all task signatures
    all_resolved: List[ResolvedParam] = []
    for spec in graph.tasks.values():
        all_resolved.extend(
            collect_params_from_task(spec.name, spec.func, spec.signature)
        )
    merged = merge_resolved_params(all_resolved)

    # Build local-param mapping so run_command can reconstruct it
    local_map: Dict[str, tuple] = {}
    for rp in merged:
        if rp.namespace == "local":
            local_map[rp.dest_name()] = (rp.task_name, rp.name)

    for rp in sorted(merged, key=lambda r: r.cli_flag()):
        if rp.name in _FIXED_SUBMIT_PARAMS:
            continue
        rp.add_to_parser(parser)

    parser.set_defaults(_command="submit", _local_map=local_map)


# ── static subcommands ────────────────────────────────────────────────


def _add_db_flags(parser: argparse.ArgumentParser) -> None:
    """Add the common ``--run-dir / --db-type / --db-url`` flags.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Target parser.
    """
    parser.add_argument(
        "--run-dir",
        required=True,
        type=str,
        help="Shared working directory.",
    )
    parser.add_argument(
        "--db-type",
        default="sqlite",
        type=str,
        help="Database backend.  [default: sqlite]",
    )
    parser.add_argument(
        "--db-url",
        default=None,
        type=str,
        help="Connection URL for database.",
    )


def _add_status_parser(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Add the ``status`` subcommand."""
    parser = subparsers.add_parser("status", help="Show the status of a run.")
    parser.add_argument("run_id", type=str, help="Run identifier.")
    _add_db_flags(parser)
    parser.add_argument(
        "--task",
        default=None,
        type=str,
        help="Filter by task name.",
    )
    parser.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        help="Output as JSON.",
    )
    parser.set_defaults(_command="status")


def _add_cancel_parser(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Add the ``cancel`` subcommand."""
    parser = subparsers.add_parser("cancel", help="Cancel active tasks in a run.")
    parser.add_argument("run_id", type=str, help="Run identifier.")
    _add_db_flags(parser)
    parser.add_argument(
        "--task",
        default=None,
        type=str,
        help="Cancel only this task.",
    )
    parser.set_defaults(_command="cancel")


def _add_retry_parser(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Add the ``retry`` subcommand."""
    parser = subparsers.add_parser(
        "retry",
        help="Retry failed or cancelled tasks.",
    )
    parser.add_argument("run_id", type=str, help="Run identifier.")
    _add_db_flags(parser)
    parser.add_argument(
        "--task",
        default=None,
        type=str,
        help="Retry only this task.",
    )
    parser.set_defaults(_command="retry")


def _add_runs_parser(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Add the ``runs`` subcommand."""
    parser = subparsers.add_parser(
        "runs",
        help="List all runs in the manifest database.",
    )
    _add_db_flags(parser)
    parser.set_defaults(_command="runs")


def _add_dag_parser(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Add the ``dag`` subcommand."""
    parser = subparsers.add_parser(
        "dag",
        help="Print the task DAG in topological order.",
    )
    parser.set_defaults(_command="dag")


def _add_dispatch_parser(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Add the ``dispatch`` subcommand (internal)."""
    parser = subparsers.add_parser("dispatch", help=argparse.SUPPRESS)
    parser.add_argument("--run-id", required=True, type=str)
    _add_db_flags(parser)
    parser.set_defaults(_command="dispatch")


def _add_worker_parser(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Add the ``worker`` subcommand (internal)."""
    parser = subparsers.add_parser("worker", help=argparse.SUPPRESS)
    parser.add_argument("--run-id", required=True, type=str)
    _add_db_flags(parser)
    parser.add_argument("--task", required=True, type=str)
    parser.add_argument("--index", default=None, type=int)
    parser.set_defaults(_command="worker")


# ── public API ────────────────────────────────────────────────────────


def build_parser(graph: Any) -> argparse.ArgumentParser:
    """Build the full CLI parser for a graph.

    Parameters
    ----------
    graph : Graph
        The DAG graph.

    Returns
    -------
    argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        prog=graph.name,
        description=f"slurm_flow DAG: {graph.name}",
    )
    subparsers = parser.add_subparsers(dest="_command", required=True)

    _add_submit_parser(subparsers, graph)
    _add_status_parser(subparsers)
    _add_cancel_parser(subparsers)
    _add_retry_parser(subparsers)
    _add_runs_parser(subparsers)
    _add_dag_parser(subparsers)
    _add_dispatch_parser(subparsers)
    _add_worker_parser(subparsers)

    return parser


def run_command(graph: Any, args: argparse.Namespace) -> int:
    """Execute the parsed command.

    Parameters
    ----------
    graph : Graph
        The DAG graph.
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    int
        Exit code.
    """
    command = args._command

    if command == "submit":
        return _cmd_submit(graph, args)
    if command == "status":
        return _cmd_status(graph, args)
    if command == "cancel":
        return _cmd_cancel(graph, args)
    if command == "retry":
        return _cmd_retry(graph, args)
    if command == "runs":
        return _cmd_runs(graph, args)
    if command == "dag":
        return _cmd_dag(graph, args)
    if command == "dispatch":
        return _cmd_dispatch(graph, args)
    if command == "worker":
        return _cmd_worker(graph, args)

    return 1


# ── command implementations ───────────────────────────────────────────

# Keys on the namespace that are not user parameters.
_INTERNAL_KEYS = frozenset(
    {
        "_command",
        "_local_map",
        "run_dir",
        "db_type",
        "db_url",
        "run_id",
        "task",
        "index",
        "output_json",
    }
)


def _cmd_submit(graph: Any, args: argparse.Namespace) -> int:
    """Handle the ``submit`` command."""
    run_dir = Path(args.run_dir)
    db_type = args.db_type
    db_url = args.db_url
    local_map: Dict[str, tuple] = getattr(args, "_local_map", {})

    # Separate global parameters from local (task-prefixed) ones.
    parameters: Dict[str, Any] = {}
    task_local: Dict[str, Dict[str, Any]] = {}

    for key, value in vars(args).items():
        if key in _INTERNAL_KEYS:
            continue
        if key in local_map:
            tname, pname = local_map[key]
            task_local.setdefault(tname, {})[pname] = value
        else:
            parameters[key] = value

    if task_local:
        parameters["__task_params__"] = task_local

    run_id = graph.submit_run(
        run_dir=run_dir,
        parameters=parameters,
        db_type=db_type,
        db_url=db_url,
    )
    print(f"run_id   = {run_id}")
    print(f"run_dir  = {run_dir.expanduser().resolve()}")
    return 0


def _cmd_status(graph: Any, args: argparse.Namespace) -> int:
    """Handle the ``status`` command."""
    info = graph.run_status(
        run_id=args.run_id,
        run_dir=Path(args.run_dir),
        db_url=args.db_url,
        db_type=args.db_type,
    )

    if getattr(args, "output_json", False):
        print(json.dumps(info, indent=2, default=str))
        return 0

    run = info["run"]
    print(f"Run      {args.run_id}")
    print(f"Graph    {run.get('graph_name', '?')}")
    print(f"Status   {run.get('status', '?')}")
    print(f"Created  {run.get('created_at', '?')}")
    print()

    summary = info["summary"]
    topo = graph._topological_order()
    ordered = [t for t in topo if t in summary]
    ordered += [t for t in summary if t not in topo]

    task_filter = getattr(args, "task", None)
    for tname in ordered:
        if task_filter and tname != task_filter:
            continue
        states = summary[tname]
        parts = [f"{s}={n}" for s, n in sorted(states.items())]
        print(f"  {tname:20s}  {', '.join(parts)}")

    if task_filter:
        print()
        instances = [
            i for i in info["instances"] if i.get("task_name") == task_filter
        ]
        for inst in instances:
            idx = inst.get("array_index")
            idx_str = f"[{idx}]" if idx is not None else "   "
            state = inst.get("state", "?")
            job = inst.get("slurm_job_id", "-")
            print(f"    {idx_str:6s}  {state:12s}  job={job}")

    return 0


def _cmd_cancel(graph: Any, args: argparse.Namespace) -> int:
    """Handle the ``cancel`` command."""
    n = graph.cancel_run(
        run_id=args.run_id,
        run_dir=Path(args.run_dir),
        db_url=args.db_url,
        db_type=args.db_type,
        task_name=getattr(args, "task", None),
    )
    print(f"Cancelled {n} task instance(s).")
    return 0


def _cmd_retry(graph: Any, args: argparse.Namespace) -> int:
    """Handle the ``retry`` command."""
    n = graph.retry_failed(
        run_id=args.run_id,
        run_dir=Path(args.run_dir),
        db_url=args.db_url,
        db_type=args.db_type,
        task_name=getattr(args, "task", None),
    )
    print(f"Marked {n} task instance(s) for retry.")
    return 0


def _cmd_runs(graph: Any, args: argparse.Namespace) -> int:
    """Handle the ``runs`` command."""
    from . import db as _db

    resolved = _db.build_db_url(
        Path(args.run_dir),
        db_type=args.db_type,
        db_url=args.db_url,
    )
    engine = _db.connect_db(resolved)
    try:
        rows = _db.list_runs(engine, graph_name=graph.name)
    finally:
        engine.dispose()

    if not rows:
        print("No runs found.")
        return 0

    for row in rows:
        print(f"  {row['run_id']}  {row['status']:12s}  {row['created_at']}")
    return 0


def _cmd_dag(graph: Any, args: argparse.Namespace) -> int:
    """Handle the ``dag`` command."""
    order = graph._topological_order()
    for tname in order:
        spec = graph.tasks[tname]
        deps = graph._effective_dependencies(spec)
        dep_str = f"  <- {', '.join(deps)}" if deps else ""
        tag = " [array]" if spec.config.array else ""
        print(f"  {tname}{tag}{dep_str}")
    return 0


def _cmd_dispatch(graph: Any, args: argparse.Namespace) -> int:
    """Handle the ``dispatch`` command (internal)."""
    graph.dispatch(
        run_id=args.run_id,
        run_dir=Path(args.run_dir),
        db_url=args.db_url,
        db_type=args.db_type,
    )
    return 0


def _cmd_worker(graph: Any, args: argparse.Namespace) -> int:
    """Handle the ``worker`` command (internal)."""
    graph.worker(
        run_id=args.run_id,
        run_dir=Path(args.run_dir),
        task_name=args.task,
        index=args.index,
        db_url=args.db_url,
        db_type=args.db_type,
    )
    return 0
