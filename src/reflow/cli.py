"""Argparse-based CLI for reflow.

The ``submit`` subcommand is built dynamically from task function
signatures.  Hidden ``dispatch``/``worker`` commands are routed to
separate parsers so they never appear in ``--help``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .params import (
    ResolvedParam,
    collect_cli_params,
    merge_resolved_params,
)
from .stores.sqlite import SqliteStore

# --- submit builder --------------------------------------------------------

_FIXED = frozenset({"run_dir", "store_path"})


def _add_submit_parser(subparsers: Any, workflow: Any) -> None:
    parser = subparsers.add_parser(
        "submit", help="Create a new run and submit the dispatcher.",
    )
    parser.add_argument(
        "--run-dir", required=True, type=str,
        help="Shared working directory.",
    )
    parser.add_argument(
        "--store-path", default=None, type=str,
        help="Explicit path to SQLite manifest.  [default: <run-dir>/manifest.db]",
    )

    all_resolved: list[ResolvedParam] = []
    for spec in workflow.tasks.values():
        all_resolved.extend(
            collect_cli_params(spec.name, spec.func, spec.signature)
        )
    merged = merge_resolved_params(all_resolved)

    local_map: dict[str, tuple[str, str]] = {}
    for rp in merged:
        if rp.namespace == "local":
            local_map[rp.dest_name()] = (rp.task_name, rp.name)

    for rp in sorted(merged, key=lambda r: r.cli_flag()):
        if rp.name in _FIXED:
            continue
        rp.add_to_parser(parser)

    parser.set_defaults(_command="submit", _local_map=local_map)


# --- static subcommands ----------------------------------------------------


def _add_store_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-dir", required=True, type=str, help="Working directory.")
    parser.add_argument("--store-path", default=None, type=str, help="SQLite manifest path.")


def _add_status_parser(sp: Any) -> None:
    p = sp.add_parser("status", help="Show the status of a run.")
    p.add_argument("run_id", type=str, help="Run identifier.")
    _add_store_flags(p)
    p.add_argument("--task", default=None, type=str, help="Filter by task.")
    p.add_argument("--json", dest="output_json", action="store_true", help="JSON output.")
    p.set_defaults(_command="status")


def _add_cancel_parser(sp: Any) -> None:
    p = sp.add_parser("cancel", help="Cancel active tasks in a run.")
    p.add_argument("run_id", type=str, help="Run identifier.")
    _add_store_flags(p)
    p.add_argument("--task", default=None, type=str)
    p.set_defaults(_command="cancel")


def _add_retry_parser(sp: Any) -> None:
    p = sp.add_parser("retry", help="Retry failed or cancelled tasks.")
    p.add_argument("run_id", type=str, help="Run identifier.")
    _add_store_flags(p)
    p.add_argument("--task", default=None, type=str)
    p.add_argument("--no-verify", dest="verify", action="store_false", default=True,
                   help="Skip output verification on cached upstream tasks.")
    p.set_defaults(_command="retry")


def _add_runs_parser(sp: Any) -> None:
    p = sp.add_parser("runs", help="List all runs.")
    _add_store_flags(p)
    p.set_defaults(_command="runs")


def _add_dag_parser(sp: Any) -> None:
    p = sp.add_parser("dag", help="Print the task DAG.")
    p.set_defaults(_command="dag")


def _add_describe_parser(sp: Any) -> None:
    p = sp.add_parser("describe", help="Print the workflow manifest as JSON.")
    p.set_defaults(_command="describe")


# --- hidden commands -------------------------------------------------------


def _build_dispatch_parser(prog: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog=f"{prog} dispatch")
    p.add_argument("--run-id", required=True, type=str)
    _add_store_flags(p)
    p.add_argument("--verify", action="store_true", default=False,
                   help="Verify cached outputs before accepting cache hits.")
    p.set_defaults(_command="dispatch")
    return p


def _build_worker_parser(prog: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog=f"{prog} worker")
    p.add_argument("--run-id", required=True, type=str)
    _add_store_flags(p)
    p.add_argument("--task", required=True, type=str)
    p.add_argument("--index", default=None, type=int)
    p.set_defaults(_command="worker")
    return p


_HIDDEN = frozenset({"dispatch", "worker"})


# --- public API ------------------------------------------------------------


def build_parser(workflow: Any) -> argparse.ArgumentParser:
    """Build the public CLI parser."""
    from . import __version__
    parser = argparse.ArgumentParser(
        prog=workflow.name, description=f"reflow workflow: {workflow.name}",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s (reflow {__version__})",
    )
    sp = parser.add_subparsers(dest="_command", required=True)
    _add_submit_parser(sp, workflow)
    _add_status_parser(sp)
    _add_cancel_parser(sp)
    _add_retry_parser(sp)
    _add_runs_parser(sp)
    _add_dag_parser(sp)
    _add_describe_parser(sp)
    return parser


def parse_args(workflow: Any, argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments, routing hidden commands internally."""
    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] in _HIDDEN:
        cmd, rest = argv[0], argv[1:]
        if cmd == "dispatch":
            return _build_dispatch_parser(workflow.name).parse_args(rest)
        if cmd == "worker":
            return _build_worker_parser(workflow.name).parse_args(rest)
    return build_parser(workflow).parse_args(argv)


def _make_store(args: argparse.Namespace) -> SqliteStore:
    """Create a SqliteStore from parsed args."""
    if getattr(args, "store_path", None):
        return SqliteStore(args.store_path)
    return SqliteStore.for_run_dir(Path(args.run_dir))


def run_command(workflow: Any, args: argparse.Namespace) -> int:
    """Execute the parsed command."""
    handlers = {
        "submit": _cmd_submit, "status": _cmd_status,
        "cancel": _cmd_cancel, "retry": _cmd_retry,
        "runs": _cmd_runs, "dag": _cmd_dag,
        "describe": _cmd_describe,
        "dispatch": _cmd_dispatch, "worker": _cmd_worker,
    }
    handler = handlers.get(args._command)
    return handler(workflow, args) if handler else 1


# --- command implementations -----------------------------------------------

_INTERNAL_KEYS = frozenset({
    "_command", "_local_map", "run_dir", "store_path",
    "run_id", "task", "index", "output_json",
})


def _cmd_submit(wf: Any, args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir)
    local_map: dict[str, tuple[str, str]] = getattr(args, "_local_map", {})
    store = _make_store(args) if getattr(args, "store_path", None) else None

    parameters: dict[str, Any] = {}
    task_local: dict[str, dict[str, Any]] = {}
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

    run_id = wf.submit_run(run_dir=run_dir, parameters=parameters, store=store)
    print(f"run_id   = {run_id}")
    print(f"run_dir  = {run_dir.expanduser().resolve()}")
    return 0


def _cmd_status(wf: Any, args: argparse.Namespace) -> int:
    store = _make_store(args)
    store.init()
    info = wf.run_status(args.run_id, store)
    if getattr(args, "output_json", False):
        print(json.dumps(info, indent=2, default=str))
        return 0

    run = info["run"]
    print(f"Run      {args.run_id}")
    print(f"Workflow {run.get('graph_name', '?')}")
    print(f"Status   {run.get('status', '?')}")
    print(f"Created  {run.get('created_at', '?')}")
    print()

    summary = info["summary"]
    try:
        ordered = wf._topological_order()
    except ValueError:
        ordered = sorted(summary)
    ordered = [t for t in ordered if t in summary] + [t for t in summary if t not in ordered]
    task_filter = getattr(args, "task", None)
    for tname in ordered:
        if task_filter and tname != task_filter:
            continue
        states = summary[tname]
        parts = [f"{s}={n}" for s, n in sorted(states.items())]
        print(f"  {tname:20s}  {', '.join(parts)}")

    if task_filter:
        print()
        for inst in info["instances"]:
            if inst.get("task_name") != task_filter:
                continue
            idx = inst.get("array_index")
            idx_str = f"[{idx}]" if idx is not None else "   "
            state = inst.get("state", "?")
            jid = inst.get("job_id", "-")
            print(f"    {idx_str:6s}  {state:12s}  job={jid}")
    return 0


def _cmd_cancel(wf: Any, args: argparse.Namespace) -> int:
    store = _make_store(args)
    store.init()
    n = wf.cancel_run(args.run_id, store, task_name=getattr(args, "task", None))
    print(f"Cancelled {n} task instance(s).")
    return 0


def _cmd_retry(wf: Any, args: argparse.Namespace) -> int:
    store = _make_store(args)
    store.init()
    n = wf.retry_failed(
        args.run_id, store, Path(args.run_dir),
        task_name=getattr(args, "task", None),
        verify=getattr(args, "verify", True),
    )
    print(f"Marked {n} task instance(s) for retry.")
    return 0


def _cmd_runs(wf: Any, args: argparse.Namespace) -> int:
    store = _make_store(args)
    store.init()
    rows = store.list_runs(graph_name=wf.name)
    if not rows:
        print("No runs found.")
        return 0
    for row in rows:
        print(f"  {row['run_id']}  {row['status']:12s}  {row['created_at']}")
    return 0


def _cmd_dag(wf: Any, args: argparse.Namespace) -> int:
    order = wf._topological_order()
    for tname in order:
        spec = wf.tasks[tname]
        deps = wf._effective_dependencies(spec)
        dep_str = f"  <- {', '.join(deps)}" if deps else ""
        tag = " [array]" if spec.config.array else ""
        print(f"  {tname}{tag}{dep_str}")
    return 0


def _cmd_describe(wf: Any, args: argparse.Namespace) -> int:
    manifest = wf.describe()
    print(json.dumps(manifest, indent=2, default=str))
    return 0


def _cmd_dispatch(wf: Any, args: argparse.Namespace) -> int:
    store = _make_store(args)
    store.init()
    wf.dispatch(args.run_id, store, Path(args.run_dir),
                verify=getattr(args, "verify", False))
    return 0


def _cmd_worker(wf: Any, args: argparse.Namespace) -> int:
    store = _make_store(args)
    store.init()
    wf.worker(
        args.run_id, store, Path(args.run_dir),
        task_name=args.task, index=args.index,
    )
    return 0


# --- standalone entry point ------------------------------------------------


def main() -> None:
    """Entry point for the ``reflow`` command (future server CLI)."""
    from . import __version__
    if len(sys.argv) > 1 and sys.argv[1] in ("--version", "-V"):
        print(f"reflow {__version__}")
        raise SystemExit(0)
    print("reflow: standalone CLI not yet implemented.")
    print("Use `python your_workflow.py --help` instead.")
    raise SystemExit(1)
