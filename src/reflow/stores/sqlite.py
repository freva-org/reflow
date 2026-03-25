"""SQLite-backed manifest store with Merkle-DAG caching.

Zero external dependencies. By default the database lives in the
user cache directory so cached task outputs and run metadata can be
reused across multiple run directories.
"""

from __future__ import annotations

import logging
import random
import sqlite3
import time
from collections.abc import Callable
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

from .._types import RunState, TaskState
from ..config import Config
from ..manifest import DEFAULT_CODEC
from . import Store
from .records import RunRecord, TaskInstanceRecord, TaskSpecRecord

UTC = timezone.utc
logger = logging.getLogger(__name__)

_MAX_RETRIES = 5
_BASE_DELAY = 0.5  # seconds

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _utcnow() -> str:
    return datetime.now(tz=UTC).isoformat()


def _retry_on_locked(func: Callable[_P, _R]) -> Callable[_P, _R]:
    """Retry a method on sqlite3.OperationalError (database locked).

    Uses exponential backoff with jitter, up to _MAX_RETRIES attempts.
    Only retries on locking errors; other OperationalErrors propagate.
    """

    @wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        for attempt in range(_MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as exc:
                msg = str(exc).lower()
                if "lock" not in msg and "busy" not in msg:
                    raise
                if attempt == _MAX_RETRIES - 1:
                    raise
                delay = _BASE_DELAY * (2**attempt) + random.uniform(0, 0.5)
                logger.warning(
                    "SQLite locked (attempt %d/%d), retrying in %.1fs: %s",
                    attempt + 1,
                    _MAX_RETRIES,
                    delay,
                    exc,
                )
                time.sleep(delay)
        return func(*args, **kwargs)  # unreachable but keeps mypy happy

    return wrapper


class SqliteStore(Store):
    """SQLite-backed manifest store.

    Parameters
    ----------
    path : Path or str
        Path to the SQLite database file.

    """

    def __init__(self, path: Path | str, readonly: bool = False) -> None:
        self.path = Path(path)
        self.readonly = readonly
        if not readonly:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None

    @classmethod
    def for_run_dir(cls, run_dir: Path | str, readonly: bool = False) -> SqliteStore:
        """Create a store at ``<run_dir>/manifest.db``.

        This remains available for callers that want a run-local store,
        but the default reflow behaviour uses [`default`][default] instead so
        one manifest database can serve multiple run directories.
        """
        rd = Path(run_dir).expanduser().resolve()
        if not readonly:
            rd.mkdir(parents=True, exist_ok=True)
        return cls(rd / "manifest.db", readonly=readonly)

    @classmethod
    def default_path(cls, config: Config | None = None) -> Path:
        """Return the default shared manifest path.

        By default this is the user cache directory, typically
        ``~/.cache/reflow/manifest.db`` on Linux, unless overridden by
        config or ``REFLOW_STORE_PATH``.
        """
        cfg = config or Config()
        return Path(cfg.default_store_path).expanduser().resolve()

    @classmethod
    def default(
        cls, config: Config | None = None, readonly: bool = False
    ) -> SqliteStore:
        """Create a store using the shared default manifest path."""
        return cls(cls.default_path(config), readonly=readonly)

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            if self.readonly:
                # Read-only URI mode: no locks, no pragmas, safe on
                # distributed filesystems with concurrent readers.
                uri = f"file:{self.path}?mode=ro&immutable=1"
                self._conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
                self._conn.row_factory = sqlite3.Row
            else:
                self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
                self._conn.row_factory = sqlite3.Row
                # WAL mode is faster but requires mmap/shared memory, which
                # fails on distributed filesystems (VAST, Lustre, GPFS).
                # Fall back to DELETE journal mode if WAL is not supported.
                try:
                    self._conn.execute("PRAGMA journal_mode=WAL")
                except sqlite3.OperationalError:
                    try:
                        self._conn.execute("PRAGMA journal_mode=DELETE")
                    except sqlite3.OperationalError:
                        pass  # use whatever mode the DB already has
                # 60s busy timeout for dispatch/submit contention.
                self._conn.execute("PRAGMA busy_timeout=60000")
        return self._conn

    # --- lifecycle ---------------------------------------------------------

    @_retry_on_locked
    def init(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id       TEXT PRIMARY KEY,
                graph_name   TEXT NOT NULL,
                user_id      TEXT NOT NULL DEFAULT '',
                created_at   TEXT NOT NULL,
                status       TEXT NOT NULL,
                parameters   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_specs (
                run_id       TEXT NOT NULL,
                task_name    TEXT NOT NULL,
                is_array     INTEGER NOT NULL,
                config       TEXT NOT NULL,
                PRIMARY KEY (run_id, task_name)
            );

            CREATE TABLE IF NOT EXISTS task_dependencies (
                run_id       TEXT NOT NULL,
                task_name    TEXT NOT NULL,
                depends_on   TEXT NOT NULL,
                PRIMARY KEY (run_id, task_name, depends_on)
            );

            CREATE TABLE IF NOT EXISTS task_instances (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id       TEXT NOT NULL,
                task_name    TEXT NOT NULL,
                array_index  INTEGER,
                state        TEXT NOT NULL,
                job_id       TEXT,
                input        TEXT NOT NULL DEFAULT '{}',
                output       TEXT,
                error_text   TEXT,
                identity     TEXT NOT NULL DEFAULT '',
                input_hash   TEXT NOT NULL DEFAULT '',
                output_hash  TEXT NOT NULL DEFAULT '',
                created_at   TEXT NOT NULL,
                updated_at   TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_instances_run
                ON task_instances(run_id, task_name);
            CREATE INDEX IF NOT EXISTS idx_instances_identity
                ON task_instances(task_name, identity, state);
        """)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _decode_run_record(self, row: sqlite3.Row | None) -> RunRecord | None:
        if row is None:
            return None
        return RunRecord(
            run_id=str(row["run_id"]),
            graph_name=str(row["graph_name"]),
            user_id=str(row["user_id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            status=RunState(str(row["status"])),
            parameters=DEFAULT_CODEC.loads(str(row["parameters"])),
        )

    def _decode_task_instance_record(
        self,
        row: sqlite3.Row | None,
    ) -> TaskInstanceRecord | None:
        if row is None:
            return None
        return TaskInstanceRecord(
            id=int(row["id"]),
            run_id=str(row["run_id"]),
            task_name=str(row["task_name"]),
            array_index=int(row["array_index"])
            if row["array_index"] is not None
            else None,
            state=TaskState(str(row["state"])),
            job_id=str(row["job_id"]) if row["job_id"] is not None else None,
            input=DEFAULT_CODEC.loads(str(row["input"])) if row["input"] else {},
            output=DEFAULT_CODEC.loads(str(row["output"])) if row["output"] else None,
            error_text=str(row["error_text"])
            if row["error_text"] is not None
            else None,
            identity=str(row["identity"]),
            input_hash=str(row["input_hash"]),
            output_hash=str(row["output_hash"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
        )

    def _decode_task_spec_record(
        self,
        row: sqlite3.Row | None,
        dependencies: list[str],
    ) -> TaskSpecRecord | None:
        if row is None:
            return None
        return TaskSpecRecord(
            run_id=str(row["run_id"]),
            task_name=str(row["task_name"]),
            is_array=bool(row["is_array"]),
            config=DEFAULT_CODEC.loads(str(row["config"])),
            dependencies=list(dependencies),
        )

    # --- runs --------------------------------------------------------------

    @_retry_on_locked
    def insert_run(
        self,
        run_id: str,
        graph_name: str,
        user_id: str,
        parameters: dict[str, Any],
    ) -> None:
        self.conn.execute(
            "INSERT INTO runs "
            "(run_id, graph_name, user_id, created_at, status, parameters) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                run_id,
                graph_name,
                user_id,
                _utcnow(),
                RunState.RUNNING.value,
                DEFAULT_CODEC.dumps(parameters),
            ),
        )
        self.conn.commit()

    def get_run_record(self, run_id: str) -> RunRecord | None:
        row = self.conn.execute(
            "SELECT * FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        return self._decode_run_record(row)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        record = self.get_run_record(run_id)
        return None if record is None else record.to_public_dict()

    def get_run_parameters(self, run_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT parameters FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"Unknown run_id: {run_id}")
        codec: dict[str, Any] = DEFAULT_CODEC.loads(row[0])
        return codec

    @_retry_on_locked
    def update_run_status(self, run_id: str, status: RunState) -> None:
        self.conn.execute(
            "UPDATE runs SET status = ? WHERE run_id = ?",
            (status.value, run_id),
        )
        self.conn.commit()

    def list_run_records(
        self,
        graph_name: str | None = None,
        user_id: str | None = None,
    ) -> list[RunRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if graph_name is not None:
            clauses.append("graph_name = ?")
            params.append(graph_name)
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(user_id)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM runs{where} ORDER BY created_at DESC",
            params,
        ).fetchall()
        return [
            record
            for row in rows
            if (record := self._decode_run_record(row)) is not None
        ]

    def list_runs(
        self,
        graph_name: str | None = None,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return [
            record.to_public_dict()
            for record in self.list_run_records(graph_name, user_id)
        ]

    # --- task specs --------------------------------------------------------

    @_retry_on_locked
    def insert_task_spec(
        self,
        run_id: str,
        task_name: str,
        is_array: bool,
        config_json: dict[str, Any],
        dependencies: list[str],
    ) -> None:
        c = self.conn
        c.execute(
            "INSERT OR REPLACE INTO task_specs (run_id, task_name, is_array, config) "
            "VALUES (?, ?, ?, ?)",
            (run_id, task_name, int(is_array), DEFAULT_CODEC.dumps(config_json)),
        )
        c.execute(
            "DELETE FROM task_dependencies WHERE run_id = ? AND task_name = ?",
            (run_id, task_name),
        )
        if dependencies:
            c.executemany(
                "INSERT INTO task_dependencies (run_id, task_name, depends_on) "
                "VALUES (?, ?, ?)",
                [(run_id, task_name, dep) for dep in dependencies],
            )
        c.commit()

    def get_task_spec_record(
        self, run_id: str, task_name: str
    ) -> TaskSpecRecord | None:
        row = self.conn.execute(
            "SELECT * FROM task_specs WHERE run_id = ? AND task_name = ?",
            (run_id, task_name),
        ).fetchone()
        deps = self.list_task_dependencies(run_id, task_name)
        return self._decode_task_spec_record(row, deps)

    def list_task_dependencies(self, run_id: str, task_name: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT depends_on FROM task_dependencies "
            "WHERE run_id = ? AND task_name = ? ORDER BY depends_on",
            (run_id, task_name),
        ).fetchall()
        return [r[0] for r in rows]

    # --- task instances -----------------------------------------------------

    @_retry_on_locked
    def insert_task_instance(
        self,
        run_id: str,
        task_name: str,
        array_index: int | None,
        state: TaskState,
        input_payload: dict[str, Any],
        identity: str = "",
        input_hash: str = "",
    ) -> int:
        now = _utcnow()
        cur = self.conn.execute(
            "INSERT INTO task_instances "
            "(run_id, task_name, array_index, state, job_id, input, output, "
            " error_text, identity, input_hash, output_hash, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, NULL, ?, NULL, NULL, ?, ?, '', ?, ?)",
            (
                run_id,
                task_name,
                array_index,
                state.value,
                DEFAULT_CODEC.dumps(input_payload),
                identity,
                input_hash,
                now,
                now,
            ),
        )
        self.conn.commit()
        return cur.lastrowid or 0

    def get_task_instance_record(
        self,
        run_id: str,
        task_name: str,
        array_index: int | None,
    ) -> TaskInstanceRecord | None:
        if array_index is None:
            row = self.conn.execute(
                "SELECT * FROM task_instances "
                "WHERE run_id = ? AND task_name = ? AND array_index IS NULL",
                (run_id, task_name),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT * FROM task_instances "
                "WHERE run_id = ? AND task_name = ? AND array_index = ?",
                (run_id, task_name, array_index),
            ).fetchone()
        return self._decode_task_instance_record(row)

    def get_task_instance(
        self,
        run_id: str,
        task_name: str,
        array_index: int | None,
    ) -> dict[str, Any] | None:
        record = self.get_task_instance_record(run_id, task_name, array_index)
        return None if record is None else record.to_public_dict()

    def list_task_instance_records(
        self,
        run_id: str,
        task_name: str | None = None,
        states: list[TaskState] | None = None,
    ) -> list[TaskInstanceRecord]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if task_name is not None:
            clauses.append("task_name = ?")
            params.append(task_name)
        if states:
            ph = ",".join("?" * len(states))
            clauses.append(f"state IN ({ph})")
            params.extend(s.value for s in states)
        rows = self.conn.execute(
            f"SELECT * FROM task_instances WHERE {' AND '.join(clauses)} "
            "ORDER BY task_name, array_index",
            params,
        ).fetchall()
        return [
            record
            for row in rows
            if (record := self._decode_task_instance_record(row)) is not None
        ]

    def list_task_instances(
        self,
        run_id: str,
        task_name: str | None = None,
        states: list[TaskState] | None = None,
    ) -> list[dict[str, Any]]:
        return [
            record.to_public_dict()
            for record in self.list_task_instance_records(run_id, task_name, states)
        ]

    def count_task_instances(self, run_id: str, task_name: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM task_instances WHERE run_id = ? AND task_name = ?",
            (run_id, task_name),
        ).fetchone()
        return row[0] if row else 0

    # --- output retrieval ---------------------------------------------------

    def get_singleton_output(self, run_id: str, task_name: str) -> Any:
        row = self.conn.execute(
            "SELECT output FROM task_instances "
            "WHERE run_id = ? AND task_name = ? AND array_index IS NULL AND state = ?",
            (run_id, task_name, TaskState.SUCCESS.value),
        ).fetchone()
        if row is None or row[0] is None:
            return None
        return DEFAULT_CODEC.loads(row[0])

    def get_array_outputs(self, run_id: str, task_name: str) -> list[Any]:
        rows = self.conn.execute(
            "SELECT output FROM task_instances "
            "WHERE run_id = ? AND task_name = ? AND state = ? "
            "AND array_index IS NOT NULL ORDER BY array_index",
            (run_id, task_name, TaskState.SUCCESS.value),
        ).fetchall()
        return [DEFAULT_CODEC.loads(r[0]) if r[0] else None for r in rows]

    def get_output_hash(
        self, run_id: str, task_name: str, array_index: int | None = None
    ) -> str:
        """Get the output hash for a specific task instance.

        Parameters
        ----------
        run_id : str
            Run identifier.
        task_name : str
            Task name.
        array_index : int or None
            Array index, or None for singleton.

        Returns
        -------
        str
            Output hash, or empty string if not found.

        """
        if array_index is None:
            row = self.conn.execute(
                "SELECT output_hash FROM task_instances "
                "WHERE run_id = ? AND task_name = ? AND "
                "array_index IS NULL AND state = ?",
                (run_id, task_name, TaskState.SUCCESS.value),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT output_hash FROM task_instances "
                "WHERE run_id = ? AND task_name = ? AND array_index = ? AND state = ?",
                (run_id, task_name, array_index, TaskState.SUCCESS.value),
            ).fetchone()
        return row[0] if row and row[0] else ""

    def get_all_output_hashes(self, run_id: str, task_name: str) -> list[str]:
        """Get output hashes for all successful instances of a task."""
        rows = self.conn.execute(
            "SELECT output_hash FROM task_instances "
            "WHERE run_id = ? AND task_name = ? AND state = ? "
            "ORDER BY array_index",
            (run_id, task_name, TaskState.SUCCESS.value),
        ).fetchall()
        return [r[0] if r[0] else "" for r in rows]

    # --- dependency check ---------------------------------------------------

    def dependency_is_satisfied(self, run_id: str, task_name: str) -> bool:
        row = self.conn.execute(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) AS ok "
            "FROM task_instances WHERE run_id = ? AND task_name = ?",
            (TaskState.SUCCESS.value, run_id, task_name),
        ).fetchone()
        if row is None:
            return False
        total, ok = row[0] or 0, row[1] or 0
        return total > 0 and total == ok

    # --- state transitions --------------------------------------------------

    @_retry_on_locked
    def update_task_submitted(
        self,
        run_id: str,
        task_name: str,
        job_id: str,
    ) -> None:
        self.conn.execute(
            "UPDATE task_instances SET state = ?, job_id = ?, updated_at = ? "
            "WHERE run_id = ? AND task_name = ? AND state IN (?, ?)",
            (
                TaskState.SUBMITTED.value,
                job_id,
                _utcnow(),
                run_id,
                task_name,
                TaskState.PENDING.value,
                TaskState.RETRYING.value,
            ),
        )
        self.conn.commit()

    @_retry_on_locked
    def update_task_running(self, instance_id: int) -> None:
        self.conn.execute(
            "UPDATE task_instances SET state = ?, updated_at = ? WHERE id = ?",
            (TaskState.RUNNING.value, _utcnow(), instance_id),
        )
        self.conn.commit()

    @_retry_on_locked
    def update_task_success(
        self,
        instance_id: int,
        output: Any,
        output_hash: str = "",
    ) -> None:
        self.conn.execute(
            "UPDATE task_instances SET state = ?, output = ?, output_hash = ?, "
            "updated_at = ? WHERE id = ?",
            (
                TaskState.SUCCESS.value,
                DEFAULT_CODEC.dumps(output),
                output_hash,
                _utcnow(),
                instance_id,
            ),
        )
        self.conn.commit()

    @_retry_on_locked
    def update_task_failed(self, instance_id: int, error_text: str) -> None:
        self.conn.execute(
            "UPDATE task_instances SET state = ?, error_text = ?, "
            "updated_at = ? WHERE id = ?",
            (TaskState.FAILED.value, error_text, _utcnow(), instance_id),
        )
        self.conn.commit()

    @_retry_on_locked
    def update_task_cancelled(self, instance_id: int) -> None:
        self.conn.execute(
            "UPDATE task_instances SET state = ?, updated_at = ? WHERE id = ?",
            (TaskState.CANCELLED.value, _utcnow(), instance_id),
        )
        self.conn.commit()

    @_retry_on_locked
    def mark_for_retry(self, instance_id: int) -> None:
        retriable = list(TaskState.retriable())
        ph = ",".join("?" * len(retriable))
        self.conn.execute(
            "UPDATE task_instances SET state = ?, error_text = NULL, "
            "output = NULL, output_hash = '', job_id = NULL, updated_at = ? "
            f"WHERE id = ? AND state IN ({ph})",
            (TaskState.RETRYING.value, _utcnow(), instance_id)
            + tuple(s.value for s in retriable),
        )
        self.conn.commit()

    # --- cache lookup -------------------------------------------------------

    def find_cached_record(
        self,
        task_name: str,
        identity: str,
    ) -> TaskInstanceRecord | None:
        """Find the most recent successful instance matching the identity.

        Searches across all runs.
        """
        if not identity:
            return None
        row = self.conn.execute(
            "SELECT * FROM task_instances "
            "WHERE task_name = ? AND identity = ? AND state = ? "
            "ORDER BY created_at DESC LIMIT 1",
            (task_name, identity, TaskState.SUCCESS.value),
        ).fetchone()
        return self._decode_task_instance_record(row)

    def find_cached(
        self,
        task_name: str,
        identity: str,
    ) -> dict[str, Any] | None:
        record = self.find_cached_record(task_name, identity)
        return None if record is None else record.to_public_dict()

    # --- summary ------------------------------------------------------------

    def task_state_summary(self, run_id: str) -> dict[str, dict[str, int]]:
        rows = self.conn.execute(
            "SELECT task_name, state, COUNT(*) AS n "
            "FROM task_instances WHERE run_id = ? GROUP BY task_name, state",
            (run_id,),
        ).fetchall()
        summary: dict[str, dict[str, int]] = {}
        for task_name, state, n in rows:
            summary.setdefault(task_name, {})[state] = n
        return summary

    # --- helpers ------------------------------------------------------------
