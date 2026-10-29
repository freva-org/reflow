"""Instances whose job ended without a result are failed.

The decision needs nothing from the scheduler: a follow-up dispatch job
only starts after the jobs it waits for ended (``afterany``), so their
instances without a result are lost.  The scheduler's state only adds
the reason (and can veto, e.g. for a requeued job).
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any

import pytest

from reflow import Config, Result, Workflow
from reflow._types import RunState, TaskState
from reflow.executors import ElementState, Executor, JobResources, JobStates, JobStatus
from reflow.results import write_result
from reflow.stores.sqlite import SqliteStore
from reflow.workflow import _reconcile


class FakeExecutor(Executor):
    """Records submissions; plays a scheduler that knows nothing by default."""

    def __init__(self) -> None:
        super().__init__()
        self.submitted: list[tuple[str, JobResources, list[str]]] = []
        self.states: dict[str, JobStates] = {}
        self.status: dict[str, ElementState] = {}

    def submit(self, resources: JobResources, command: list[str]) -> str:
        job_id = f"J{len(self.submitted) + 1}"
        self.submitted.append((job_id, resources, command))
        return job_id

    def cancel(self, job_id: str) -> None:
        pass

    def job_state(self, job_id: str) -> str | None:
        return None

    def job_states(self, job_id: str) -> JobStates:
        return self.states.get(job_id, JobStates())

    def job_status(self, job_id: str) -> ElementState:
        return self.status.get(job_id, ElementState(JobStatus.UNKNOWN))

    def task_jobs(self) -> list[str]:
        return [j for j, res, _ in self.submitted if not res.job_name.endswith("dispatch")]

    def last_after_ref(self) -> str | None:
        """``--after-ref`` of the most recent dispatch job."""
        for _, res, cmd in reversed(self.submitted):
            if res.job_name.endswith("dispatch"):
                return cmd[cmd.index("--after-ref") + 1] if "--after-ref" in cmd else None
        return None


def ended(reason: str) -> ElementState:
    return ElementState(JobStatus.ENDED, reason)


@pytest.fixture(autouse=True)
def _isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("REFLOW_RESULT_GRACE", "0")
    yield


def _workflow() -> Workflow:
    wf = Workflow("wf", config=Config({"executor": {"mode": "sbatch"}}))

    @wf.job()
    def source() -> list[str]:
        return ["a", "b", "c"]

    @wf.array_job()
    def process(item: Annotated[str, Result(step="source")]) -> str:
        return item.upper()

    wf.validate()
    return wf


class Harness:
    """Drive a run pass by pass, playing the scheduler's and workers' part."""

    def __init__(self, tmp_path: Path) -> None:
        self.wf = _workflow()
        self.exc = FakeExecutor()
        self.store = SqliteStore(tmp_path / "db.sqlite")
        self.store.init()
        self.run_dir = tmp_path / "run"
        self.run_id = self.wf.submit_run(
            run_dir=self.run_dir, parameters={}, store=self.store, executor=self.exc
        )

    def dispatch(self, after_ref: str | None = "queued") -> None:
        """Run the queued dispatch job, as the scheduler would."""
        if after_ref == "queued":
            after_ref = self.exc.last_after_ref()
        self.wf.dispatch(
            self.run_id, self.store, self.run_dir, executor=self.exc, after_ref=after_ref
        )

    def instance(self, task: str, index: int | None) -> dict[str, Any]:
        row = self.store.get_task_instance(self.run_id, task, index)
        assert row is not None
        return row

    def succeed(self, task: str, index: int | None, output: Any) -> None:
        write_result(
            self.run_id,
            task,
            index,
            int(self.instance(task, index)["id"]),
            TaskState.SUCCESS,
            output=output,
            output_hash=f"h-{task}-{index}",
        )

    def start_array(self) -> str:
        """Run the source task and get the array submitted."""
        self.dispatch()  # submits `source`
        self.succeed("source", None, ["a", "b", "c"])
        self.dispatch()  # after source: ingests it, submits the array
        return self.exc.task_jobs()[-1]

    def state(self, task: str, index: int | None) -> str:
        return str(self.instance(task, index)["state"])

    def run_status(self) -> str:
        row = self.store.get_run(self.run_id)
        assert row is not None
        return str(row["status"])


def test_lost_elements_fail_without_any_scheduler_state(tmp_path: Path) -> None:
    """A backend that cannot report states at all still gets this right."""
    h = Harness(tmp_path)
    h.start_array()
    h.succeed("process", 0, "A")
    h.succeed("process", 2, "C")
    h.dispatch()  # the dispatch job that waited for the array

    assert h.state("process", 0) == TaskState.SUCCESS.value
    assert h.state("process", 1) == TaskState.FAILED.value
    assert "scheduler state: unknown" in str(h.instance("process", 1)["error_text"])
    assert h.run_status() == RunState.FAILED.value


def test_scheduler_reason_is_reported(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    array_job = h.start_array()
    h.succeed("process", 0, "A")
    h.exc.states[array_job] = JobStates(
        {0: ended("COMPLETED"), 1: ended("OUT_OF_MEMORY"), 2: ended("COMPLETED")}
    )
    h.dispatch()

    assert "OUT_OF_MEMORY" in str(h.instance("process", 1)["error_text"])
    assert "completed but wrote no result" in str(h.instance("process", 2)["error_text"])


def test_scheduler_can_veto_a_requeued_job(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    array_job = h.start_array()
    h.exc.states[array_job] = JobStates(
        {0: ended("NODE_FAIL"), 1: ElementState(JobStatus.QUEUED, "REQUEUED")},
        default=ended("COMPLETED"),
    )
    h.dispatch()

    assert h.state("process", 0) == TaskState.FAILED.value
    assert h.state("process", 1) == TaskState.SUBMITTED.value
    assert h.state("process", 2) == TaskState.FAILED.value


def test_only_awaited_jobs_are_checked(tmp_path: Path) -> None:
    """A dispatch pass outside the chain (no ``--after-ref``) fails nothing."""
    h = Harness(tmp_path)
    h.start_array()
    h.dispatch(after_ref=None)
    h.dispatch(after_ref="some-other-pass")
    assert {h.state("process", i) for i in range(3)} == {TaskState.SUBMITTED.value}


def test_awaited_jobs_are_stored_per_pass(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    array_job = h.start_array()
    ref = h.exc.last_after_ref()
    assert ref is not None
    assert h.store.list_jobs(h.run_id, "awaited", ref=ref) == [array_job]


def test_late_result_within_grace_is_accepted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = Harness(tmp_path)
    h.start_array()
    for index in (0, 1):
        h.succeed("process", index, "X")
    waited: list[float] = []

    def sleep(seconds: float) -> None:
        waited.append(seconds)
        h.succeed("process", 2, "C")  # the file shows up while we wait

    monkeypatch.setenv("REFLOW_RESULT_GRACE", "7")
    monkeypatch.setattr(_reconcile.time, "sleep", sleep)
    h.dispatch()

    assert waited == [7.0]
    assert h.state("process", 2) == TaskState.SUCCESS.value
    assert h.run_status() == RunState.SUCCESS.value


class TestRetry:
    def test_retry_fails_and_resubmits_lost_elements(self, tmp_path: Path) -> None:
        """``retry`` runs outside the chain, so it asks the live queue."""
        h = Harness(tmp_path)
        array_job = h.start_array()
        h.succeed("process", 0, "A")
        h.succeed("process", 2, "C")
        h.exc.status[array_job] = ended("")

        assert h.wf.retry_failed(h.run_id, h.store, h.run_dir, executor=h.exc) == 1
        h.dispatch()
        retried = h.exc.task_jobs()[-1]
        assert retried != array_job
        assert h.instance("process", 1)["job_id"] == retried
        assert h.state("process", 0) == TaskState.SUCCESS.value

    @pytest.mark.parametrize("status", [JobStatus.RUNNING, JobStatus.UNKNOWN])
    def test_retry_leaves_live_or_unknown_jobs_alone(
        self, tmp_path: Path, status: JobStatus
    ) -> None:
        h = Harness(tmp_path)
        array_job = h.start_array()
        h.exc.status[array_job] = ElementState(status)
        assert h.wf.retry_failed(h.run_id, h.store, h.run_dir, executor=h.exc) == 0
        assert {h.state("process", i) for i in range(3)} == {TaskState.SUBMITTED.value}


def test_dispatch_job_ids_are_recorded(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    h.start_array()
    dispatch_jobs = [j for j, r, _ in h.exc.submitted if r.job_name.endswith("dispatch")]
    assert h.store.list_jobs(h.run_id, "dispatch") == dispatch_jobs
    assert len(dispatch_jobs) == 3  # submit, after source, after the array


def test_cli_passes_after_ref(tmp_path: Path) -> None:
    from reflow.cli import _build_dispatch_parser

    args = _build_dispatch_parser("wf").parse_args(
        ["--run-id", "r", "--after-ref", "abc123"]
    )
    assert args.after_ref == "abc123"


def test_grace_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REFLOW_RESULT_GRACE")
    assert _reconcile.result_grace() == _reconcile.DEFAULT_RESULT_GRACE
    monkeypatch.setenv("REFLOW_RESULT_GRACE", "nonsense")
    assert _reconcile.result_grace() == _reconcile.DEFAULT_RESULT_GRACE
    monkeypatch.setenv("REFLOW_RESULT_GRACE", "-3")
    assert _reconcile.result_grace() == 0.0


class PerElementExecutor(FakeExecutor):
    """Like Flux: one job per array element, ids returned comma separated."""

    def submit(self, resources: JobResources, command: list[str]) -> str:
        if not resources.array:
            return super().submit(resources, command)
        lo, _, hi = resources.array.partition("-")
        n = int(hi or lo) + 1
        ids = [f"E{len(self.submitted) + 1}.{i}" for i in range(n)]
        self.submitted.append((",".join(ids), resources, command))
        return ",".join(ids)

    def element_job_ids(self, job_id: str, indices: list[int]) -> dict[int, str] | None:
        return dict(zip(sorted(indices), job_id.split(","), strict=True))


def test_per_element_jobs_are_awaited_and_reconciled(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    h.exc = PerElementExecutor()
    h.dispatch()
    h.succeed("source", None, ["a", "b", "c"])
    h.dispatch()
    group = h.exc.task_jobs()[-1].split(",")
    assert [h.instance("process", i)["job_id"] for i in range(3)] == group
    assert h.store.list_jobs(h.run_id, "awaited", ref=h.exc.last_after_ref()) == group

    h.succeed("process", 0, "A")
    h.succeed("process", 1, "B")
    h.dispatch()
    assert h.state("process", 2) == TaskState.FAILED.value
    assert group[2] in str(h.instance("process", 2)["error_text"])
