"""test_dispatch_waves.py — capped-wave array submission.

With ``Config.max_submit_jobs`` set, a large array task is submitted in
waves of at most ``cap`` indices; the remainder stay PENDING and are
submitted by later dispatches (which the real scheduler triggers via the
follow-up dependency). These tests drive ``_dispatch_array`` directly and
simulate each wave completing, asserting the ``--array`` string and the
PENDING/SUBMITTED bookkeeping.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from reflow import Config, Result, Workflow
from reflow._types import TaskState
from reflow.executors.local import LocalExecutor
from reflow.stores.sqlite import SqliteStore


def _store(tmp_path: Path) -> SqliteStore:
    st = SqliteStore(str(tmp_path / "db.sqlite"))
    st.init()
    return st


class _RecordingExecutor(LocalExecutor):
    """Records each submitted ``--array`` string and returns a fake job id."""

    def __init__(self) -> None:
        super().__init__()
        self.arrays: list[str] = []
        self._n = 0

    def submit(self, resources, command):  # type: ignore[override]
        self.arrays.append(resources.array)
        self._n += 1
        return f"job{self._n}"


def _wf(cap: int, n_items: int) -> tuple[Workflow, int]:
    wf = Workflow(
        "wf",
        config=Config(
            {"executor": {"mode": "dry-run", "max_submit_jobs": cap}}
        ),
    )

    @wf.job()
    def source() -> list[str]:
        return [f"x{i}" for i in range(n_items)]

    @wf.array_job(array_parallelism=4)
    def process(item: Annotated[str, Result(step="source")]) -> str:
        return item

    wf.validate()
    return wf, n_items


def _state_counts(st: SqliteStore) -> dict:
    counts: dict[str, int] = {}
    for r in st.list_task_instances("r1", task_name="process"):
        counts[r["state"]] = counts.get(r["state"], 0) + 1
    return counts


def _complete(st: SqliteStore, indices: list[int]) -> None:
    """Mark the given submitted process indices as SUCCESS (a wave finished)."""
    for idx in indices:
        row = st.get_task_instance("r1", "process", idx)
        st.update_task_success(int(row["id"]), f"out{idx}")


class TestArrayWaveCapping:
    def test_first_wave_respects_cap(self, tmp_path: Path) -> None:
        wf, _ = _wf(cap=2, n_items=5)
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance("r1", "source", None, TaskState.SUCCESS, {})
            st.update_task_success(iid, [f"x{i}" for i in range(5)])
            ex = _RecordingExecutor()

            jid = wf._dispatch_array("r1", tmp_path, wf.tasks["process"], st, ex)

            assert jid == "job1"
            # all 5 instances created; only 2 submitted this wave, 3 still pending
            assert _state_counts(st) == {"SUBMITTED": 2, "PENDING": 3}
            # array string is the capped, parallelism-tagged range
            assert ex.arrays == ["0-1%4"]

    def test_waves_drain_until_empty(self, tmp_path: Path) -> None:
        wf, _ = _wf(cap=2, n_items=5)
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance("r1", "source", None, TaskState.SUCCESS, {})
            st.update_task_success(iid, [f"x{i}" for i in range(5)])
            ex = _RecordingExecutor()
            spec = wf.tasks["process"]

            # wave 1: indices 0,1
            wf._dispatch_array("r1", tmp_path, spec, st, ex)
            _complete(st, [0, 1])
            # wave 2: indices 2,3
            wf._dispatch_array("r1", tmp_path, spec, st, ex)
            _complete(st, [2, 3])
            # wave 3: index 4 (single)
            wf._dispatch_array("r1", tmp_path, spec, st, ex)
            _complete(st, [4])
            # nothing left to submit
            jid_final = wf._dispatch_array("r1", tmp_path, spec, st, ex)

            assert ex.arrays == ["0-1%4", "2-3%4", "4%4"]
            assert jid_final is None
            assert _state_counts(st) == {"SUCCESS": 5}

    def test_no_cap_submits_whole_array_at_once(self, tmp_path: Path) -> None:
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def source() -> list[str]:
            return [f"x{i}" for i in range(5)]

        @wf.array_job(array_parallelism=4)
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance("r1", "source", None, TaskState.SUCCESS, {})
            st.update_task_success(iid, [f"x{i}" for i in range(5)])
            ex = _RecordingExecutor()
            wf._dispatch_array("r1", tmp_path, wf.tasks["process"], st, ex)
            # cap unset -> single submission of the full range
            assert ex.arrays == ["0-4%4"]
            assert _state_counts(st) == {"SUBMITTED": 5}

    def test_wave_uses_local_executor_for_real(self, tmp_path: Path) -> None:
        # sanity: the capped path also works with a real executor (LocalExecutor)
        wf, _ = _wf(cap=3, n_items=4)
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance("r1", "source", None, TaskState.SUCCESS, {})
            st.update_task_success(iid, [f"x{i}" for i in range(4)])
            jid = wf._dispatch_array(
                "r1", tmp_path, wf.tasks["process"], st, LocalExecutor()
            )
            assert jid is not None
            # 3 submitted (the cap), 1 still pending for the next wave
            counts = _state_counts(st)
            assert counts.get("PENDING", 0) == 1
