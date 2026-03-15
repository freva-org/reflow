"""Database helpers for slurm_flow.

This module defines the manifest schema and all queries used by the
orchestration engine.  SQLAlchemy Core is used so that the same code
can target SQLite, PostgreSQL, or MariaDB.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from sqlalchemy import (
    JSON,
    Column,
    Engine,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    case,
    create_engine,
    event,
    func,
    insert,
    select,
    update,
)

from ._types import RunState, TaskState

logger = logging.getLogger(__name__)

UTC = timezone.utc
_METADATA = MetaData()

# ── schema ─────────────────────────────────────────────────────────────

RUNS = Table(
    "runs",
    _METADATA,
    Column("run_id", String(255), primary_key=True),
    Column("graph_name", String(255), nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("status", String(64), nullable=False),
    Column("parameters_json", JSON, nullable=False),
)

TASK_SPECS = Table(
    "task_specs",
    _METADATA,
    Column("run_id", String(255), primary_key=True),
    Column("task_name", String(255), primary_key=True),
    Column("is_array", Integer, nullable=False),
    Column("expand_expr", String(255), nullable=True),
    Column("config_json", JSON, nullable=False),
)

TASK_DEPENDENCIES = Table(
    "task_dependencies",
    _METADATA,
    Column("run_id", String(255), primary_key=True),
    Column("task_name", String(255), primary_key=True),
    Column("depends_on", String(255), primary_key=True),
)

TASK_INSTANCES = Table(
    "task_instances",
    _METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(255), nullable=False),
    Column("task_name", String(255), nullable=False),
    Column("array_index", Integer, nullable=True),
    Column("state", String(64), nullable=False),
    Column("slurm_job_id", String(255), nullable=True),
    Column("input_json", JSON, nullable=False),
    Column("output_json", JSON, nullable=True),
    Column("error_text", Text, nullable=True),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

# ── helpers ────────────────────────────────────────────────────────────


def utcnow() -> str:
    """Return the current UTC time as an ISO-8601 string.

    Returns
    -------
    str
        Timestamp in ISO-8601 format.
    """
    return datetime.now(tz=UTC).isoformat()


_DB_TYPE_ALIASES: Dict[str, str] = {
    "sqlite": "sqlite",
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "postgresql+psycopg": "postgresql",
    "mariadb": "mariadb",
    "mysql": "mariadb",
    "mysql+pymysql": "mariadb",
    "mariadb+mariadbconnector": "mariadb",
}


def normalize_db_type(db_type: str) -> str:
    """Normalize a user-supplied backend name.

    Parameters
    ----------
    db_type : str
        User-supplied backend identifier.

    Returns
    -------
    str
        One of ``"sqlite"``, ``"postgresql"``, ``"mariadb"``.

    Raises
    ------
    ValueError
        If *db_type* is not recognized.
    """
    key = db_type.strip().lower()
    if key not in _DB_TYPE_ALIASES:
        supported = ", ".join(sorted(_DB_TYPE_ALIASES))
        raise ValueError(
            f"Unsupported db_type {db_type!r}. Supported: {supported}"
        )
    return _DB_TYPE_ALIASES[key]


def infer_db_type(db_url: str) -> str:
    """Infer the normalized backend family from a SQLAlchemy URL.

    Parameters
    ----------
    db_url : str
        Full SQLAlchemy connection URL.

    Returns
    -------
    str
        One of ``"sqlite"``, ``"postgresql"``, ``"mariadb"``.
    """
    low = db_url.strip().lower()
    if low.startswith("sqlite"):
        return "sqlite"
    if low.startswith("postgres"):
        return "postgresql"
    if low.startswith("mariadb") or low.startswith("mysql"):
        return "mariadb"
    return "sqlite"


def build_db_url(
    run_dir: Path,
    db_type: str = "sqlite",
    db_url: Optional[str] = None,
) -> str:
    """Resolve the SQLAlchemy URL for a run.

    Parameters
    ----------
    run_dir : Path
        Shared run directory.
    db_type : str
        Logical backend type.
    db_url : str or None
        Explicit SQLAlchemy URL.  When given, returned as-is.

    Returns
    -------
    str
        Resolved SQLAlchemy URL.

    Raises
    ------
    ValueError
        If a non-SQLite backend is requested without *db_url*.
    """
    if db_url:
        return db_url

    normalized = normalize_db_type(db_type)
    if normalized == "sqlite":
        path = (run_dir / "manifest.db").expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite+pysqlite:///{path}"

    raise ValueError(
        f"{normalized} requires an explicit --db-url.  Example: "
        f"postgresql+psycopg://user:pass@host:5432/db"
    )


def connect_db(db_url: str) -> Engine:
    """Create a SQLAlchemy engine for the manifest database.

    Parameters
    ----------
    db_url : str
        SQLAlchemy database URL.

    Returns
    -------
    Engine
        Configured SQLAlchemy engine.
    """
    connect_args: Dict[str, Any] = {}
    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    engine = create_engine(
        db_url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
    )

    if db_url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragmas(dbapi_conn: Any, rec: Any) -> None:
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("PRAGMA journal_mode=WAL;")
                cursor.execute("PRAGMA busy_timeout=5000;")
            finally:
                cursor.close()

    return engine


def init_db(engine: Engine) -> None:
    """Create the manifest tables if they do not yet exist.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    """
    _METADATA.create_all(engine)


# ── CRUD: runs ─────────────────────────────────────────────────────────


def insert_run(
    engine: Engine,
    run_id: str,
    graph_name: str,
    parameters: Dict[str, Any],
) -> None:
    """Insert a new run record.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Unique run identifier.
    graph_name : str
        Name of the DAG graph.
    parameters : Dict[str, Any]
        Serializable run parameters.
    """
    with engine.begin() as conn:
        conn.execute(
            insert(RUNS).values(
                run_id=run_id,
                graph_name=graph_name,
                created_at=utcnow(),
                status=RunState.RUNNING.value,
                parameters_json=parameters,
            )
        )


def get_run(engine: Engine, run_id: str) -> Optional[Mapping[str, Any]]:
    """Load a single run record.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.

    Returns
    -------
    Mapping[str, Any] or None
        Row mapping or ``None`` if not found.
    """
    with engine.connect() as conn:
        row = conn.execute(
            select(RUNS).where(RUNS.c.run_id == run_id)
        ).first()
    return row._mapping if row is not None else None


def get_run_parameters(engine: Engine, run_id: str) -> Dict[str, Any]:
    """Load the top-level run parameters.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.

    Returns
    -------
    Dict[str, Any]
        Deserialized run parameters.

    Raises
    ------
    KeyError
        If *run_id* does not exist.
    """
    with engine.connect() as conn:
        row = conn.execute(
            select(RUNS.c.parameters_json).where(RUNS.c.run_id == run_id)
        ).first()
    if row is None:
        raise KeyError(f"Unknown run_id: {run_id}")
    payload = row[0]
    if isinstance(payload, str):
        return json.loads(payload)
    return dict(payload)


def update_run_status(engine: Engine, run_id: str, status: RunState) -> None:
    """Update the top-level run status.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.
    status : RunState
        New status value.
    """
    with engine.begin() as conn:
        conn.execute(
            update(RUNS)
            .where(RUNS.c.run_id == run_id)
            .values(status=status.value)
        )


def list_runs(engine: Engine, graph_name: Optional[str] = None) -> List[Mapping[str, Any]]:
    """List all runs, optionally filtered by graph name.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    graph_name : str or None
        Optional graph name filter.

    Returns
    -------
    List[Mapping[str, Any]]
        List of run row mappings.
    """
    stmt = select(RUNS).order_by(RUNS.c.created_at.desc())
    if graph_name is not None:
        stmt = stmt.where(RUNS.c.graph_name == graph_name)
    with engine.connect() as conn:
        rows = conn.execute(stmt).all()
    return [r._mapping for r in rows]


# ── CRUD: task specs ───────────────────────────────────────────────────


def insert_task_spec(
    engine: Engine,
    run_id: str,
    task_name: str,
    is_array: bool,
    expand_expr: Optional[str],
    config_json: Dict[str, Any],
    dependencies: Sequence[str],
) -> None:
    """Persist one task specification and its dependencies.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.
    task_name : str
        Task name.
    is_array : bool
        Whether this is an array task.
    expand_expr : str or None
        Expansion expression (e.g. ``"prepare.files"``).
    config_json : Dict[str, Any]
        Serialized job configuration.
    dependencies : Sequence[str]
        Names of upstream tasks.
    """
    with engine.begin() as conn:
        conn.execute(
            TASK_SPECS.delete().where(
                and_(
                    TASK_SPECS.c.run_id == run_id,
                    TASK_SPECS.c.task_name == task_name,
                )
            )
        )
        conn.execute(
            insert(TASK_SPECS).values(
                run_id=run_id,
                task_name=task_name,
                is_array=int(is_array),
                expand_expr=expand_expr,
                config_json=config_json,
            )
        )
        conn.execute(
            TASK_DEPENDENCIES.delete().where(
                and_(
                    TASK_DEPENDENCIES.c.run_id == run_id,
                    TASK_DEPENDENCIES.c.task_name == task_name,
                )
            )
        )
        if dependencies:
            conn.execute(
                insert(TASK_DEPENDENCIES),
                [
                    {
                        "run_id": run_id,
                        "task_name": task_name,
                        "depends_on": dep,
                    }
                    for dep in dependencies
                ],
            )


def list_task_dependencies(
    engine: Engine,
    run_id: str,
    task_name: str,
) -> List[str]:
    """Return the upstream dependency names for a task.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.
    task_name : str
        Task name.

    Returns
    -------
    List[str]
        Ordered list of upstream task names.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            select(TASK_DEPENDENCIES.c.depends_on)
            .where(
                and_(
                    TASK_DEPENDENCIES.c.run_id == run_id,
                    TASK_DEPENDENCIES.c.task_name == task_name,
                )
            )
            .order_by(TASK_DEPENDENCIES.c.depends_on)
        ).all()
    return [str(r[0]) for r in rows]


# ── CRUD: task instances ───────────────────────────────────────────────


def insert_task_instance(
    engine: Engine,
    run_id: str,
    task_name: str,
    array_index: Optional[int],
    state: TaskState,
    input_payload: Dict[str, Any],
) -> int:
    """Insert one task instance row.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.
    task_name : str
        Task name.
    array_index : int or None
        Array index (``None`` for singletons).
    state : TaskState
        Initial state.
    input_payload : Dict[str, Any]
        JSON-serializable task input.

    Returns
    -------
    int
        Database id of the new instance.
    """
    now = utcnow()
    with engine.begin() as conn:
        result = conn.execute(
            insert(TASK_INSTANCES).values(
                run_id=run_id,
                task_name=task_name,
                array_index=array_index,
                state=state.value,
                slurm_job_id=None,
                input_json=input_payload,
                output_json=None,
                error_text=None,
                created_at=now,
                updated_at=now,
            )
        )
        pk = result.inserted_primary_key
        if not pk:
            raise RuntimeError("Could not determine inserted instance id.")
        return int(pk[0])


def get_task_instance(
    engine: Engine,
    run_id: str,
    task_name: str,
    array_index: Optional[int],
) -> Optional[Mapping[str, Any]]:
    """Load one task instance.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.
    task_name : str
        Task name.
    array_index : int or None
        Array index (``None`` for singletons).

    Returns
    -------
    Mapping[str, Any] or None
        Row mapping or ``None``.
    """
    conds = [
        TASK_INSTANCES.c.run_id == run_id,
        TASK_INSTANCES.c.task_name == task_name,
    ]
    if array_index is None:
        conds.append(TASK_INSTANCES.c.array_index.is_(None))
    else:
        conds.append(TASK_INSTANCES.c.array_index == array_index)

    with engine.connect() as conn:
        row = conn.execute(
            select(TASK_INSTANCES).where(and_(*conds))
        ).first()
    return row._mapping if row is not None else None


def list_task_instances(
    engine: Engine,
    run_id: str,
    task_name: Optional[str] = None,
    states: Optional[Sequence[TaskState]] = None,
) -> List[Mapping[str, Any]]:
    """List task instances with optional filters.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.
    task_name : str or None
        Optional task name filter.
    states : Sequence[TaskState] or None
        Optional state filter.

    Returns
    -------
    List[Mapping[str, Any]]
        Matching instance rows.
    """
    conds = [TASK_INSTANCES.c.run_id == run_id]
    if task_name is not None:
        conds.append(TASK_INSTANCES.c.task_name == task_name)
    if states:
        conds.append(
            TASK_INSTANCES.c.state.in_([s.value for s in states])
        )

    with engine.connect() as conn:
        rows = conn.execute(
            select(TASK_INSTANCES)
            .where(and_(*conds))
            .order_by(TASK_INSTANCES.c.task_name, TASK_INSTANCES.c.array_index)
        ).all()
    return [r._mapping for r in rows]


def get_successful_singleton_output(
    engine: Engine,
    run_id: str,
    task_name: str,
) -> Optional[Dict[str, Any]]:
    """Load the output JSON of a successful singleton task.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.
    task_name : str
        Task name.

    Returns
    -------
    Dict[str, Any] or None
        Deserialized output or ``None``.
    """
    with engine.connect() as conn:
        row = conn.execute(
            select(TASK_INSTANCES.c.output_json).where(
                and_(
                    TASK_INSTANCES.c.run_id == run_id,
                    TASK_INSTANCES.c.task_name == task_name,
                    TASK_INSTANCES.c.array_index.is_(None),
                    TASK_INSTANCES.c.state == TaskState.SUCCESS.value,
                )
            )
        ).first()
    if row is None or row[0] is None:
        return None
    payload = row[0]
    return json.loads(payload) if isinstance(payload, str) else dict(payload)


def dependency_is_satisfied(
    engine: Engine,
    run_id: str,
    task_name: str,
) -> bool:
    """Check whether all instances of an upstream task succeeded.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.
    task_name : str
        Name of the upstream task.

    Returns
    -------
    bool
        ``True`` when at least one instance exists and all are ``SUCCESS``.
    """
    with engine.connect() as conn:
        row = conn.execute(
            select(
                func.count().label("total"),
                func.sum(
                    case(
                        (TASK_INSTANCES.c.state == TaskState.SUCCESS.value, 1),
                        else_=0,
                    )
                ).label("ok"),
            ).where(
                and_(
                    TASK_INSTANCES.c.run_id == run_id,
                    TASK_INSTANCES.c.task_name == task_name,
                )
            )
        ).first()
    if row is None:
        return False
    total = int(row[0] or 0)
    ok = int(row[1] or 0)
    return total > 0 and total == ok


def count_task_instances(
    engine: Engine,
    run_id: str,
    task_name: str,
) -> int:
    """Count instances of a task.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.
    task_name : str
        Task name.

    Returns
    -------
    int
        Instance count.
    """
    with engine.connect() as conn:
        row = conn.execute(
            select(func.count()).where(
                and_(
                    TASK_INSTANCES.c.run_id == run_id,
                    TASK_INSTANCES.c.task_name == task_name,
                )
            )
        ).first()
    return int(row[0] if row else 0)


# ── state transitions ──────────────────────────────────────────────────


def update_task_submitted(
    engine: Engine,
    run_id: str,
    task_name: str,
    job_id: str,
) -> None:
    """Mark all pending instances of a task as submitted.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.
    task_name : str
        Task name.
    job_id : str
        Workload manager job identifier.
    """
    with engine.begin() as conn:
        conn.execute(
            update(TASK_INSTANCES)
            .where(
                and_(
                    TASK_INSTANCES.c.run_id == run_id,
                    TASK_INSTANCES.c.task_name == task_name,
                    TASK_INSTANCES.c.state.in_(
                        [TaskState.PENDING.value, TaskState.RETRYING.value]
                    ),
                )
            )
            .values(
                state=TaskState.SUBMITTED.value,
                slurm_job_id=job_id,
                updated_at=utcnow(),
            )
        )


def update_task_running(engine: Engine, instance_id: int) -> None:
    """Mark one task instance as running.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    instance_id : int
        Database id.
    """
    with engine.begin() as conn:
        conn.execute(
            update(TASK_INSTANCES)
            .where(TASK_INSTANCES.c.id == instance_id)
            .values(state=TaskState.RUNNING.value, updated_at=utcnow())
        )


def update_task_success(
    engine: Engine,
    instance_id: int,
    output_payload: Dict[str, Any],
) -> None:
    """Mark one task instance as successful.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    instance_id : int
        Database id.
    output_payload : Dict[str, Any]
        Serializable output.
    """
    with engine.begin() as conn:
        conn.execute(
            update(TASK_INSTANCES)
            .where(TASK_INSTANCES.c.id == instance_id)
            .values(
                state=TaskState.SUCCESS.value,
                output_json=output_payload,
                updated_at=utcnow(),
            )
        )


def update_task_failed(
    engine: Engine,
    instance_id: int,
    error_text: str,
) -> None:
    """Mark one task instance as failed.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    instance_id : int
        Database id.
    error_text : str
        Traceback or error message.
    """
    with engine.begin() as conn:
        conn.execute(
            update(TASK_INSTANCES)
            .where(TASK_INSTANCES.c.id == instance_id)
            .values(
                state=TaskState.FAILED.value,
                error_text=error_text,
                updated_at=utcnow(),
            )
        )


def update_task_cancelled(
    engine: Engine,
    instance_id: int,
) -> None:
    """Mark one task instance as cancelled.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    instance_id : int
        Database id.
    """
    with engine.begin() as conn:
        conn.execute(
            update(TASK_INSTANCES)
            .where(TASK_INSTANCES.c.id == instance_id)
            .values(
                state=TaskState.CANCELLED.value,
                updated_at=utcnow(),
            )
        )


def mark_for_retry(
    engine: Engine,
    instance_id: int,
) -> None:
    """Reset a failed or cancelled instance for retry.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    instance_id : int
        Database id.
    """
    with engine.begin() as conn:
        conn.execute(
            update(TASK_INSTANCES)
            .where(
                and_(
                    TASK_INSTANCES.c.id == instance_id,
                    TASK_INSTANCES.c.state.in_(
                        [s.value for s in TaskState.retriable()]
                    ),
                )
            )
            .values(
                state=TaskState.RETRYING.value,
                error_text=None,
                output_json=None,
                slurm_job_id=None,
                updated_at=utcnow(),
            )
        )


def task_state_summary(
    engine: Engine,
    run_id: str,
) -> Dict[str, Dict[str, int]]:
    """Compute per-task state counts for a run.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine.
    run_id : str
        Run identifier.

    Returns
    -------
    Dict[str, Dict[str, int]]
        ``{task_name: {state: count, ...}, ...}``
    """
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                TASK_INSTANCES.c.task_name,
                TASK_INSTANCES.c.state,
                func.count().label("n"),
            )
            .where(TASK_INSTANCES.c.run_id == run_id)
            .group_by(TASK_INSTANCES.c.task_name, TASK_INSTANCES.c.state)
        ).all()

    summary: Dict[str, Dict[str, int]] = {}
    for task_name, state, n in rows:
        summary.setdefault(str(task_name), {})[str(state)] = int(n)
    return summary
