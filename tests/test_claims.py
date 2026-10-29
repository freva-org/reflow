"""Exclusive resources: one active run per target (``Workflow(exclusive=...)``)."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any

import pytest

from reflow import Config, Param, Result, RunConflictError, Workflow
from reflow._types import RunState, TaskState
from reflow.claims import normalise_key, resource_keys
from reflow.executors import ElementState, JobResources, JobStates, JobStatus
from reflow.results import write_result
from reflow.stores.sqlite import SqliteStore

from .test_reconcile import FakeExecutor, ended

RUNNING = ElementState(JobStatus.RUNNING, "RUNNING")


@pytest.fixture(autouse=True)
def _isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("REFLOW_RESULT_GRACE", "0")
    yield


def _workflow(name: str = "conv", param: str = "target") -> Workflow:
    wf = Workflow(
        name, config=Config({"executor": {"mode": "sbatch"}}), exclusive=[param]
    )
    if param == "target":

        @wf.job()
        def init(target: Annotated[str, Param(help="out")]) -> list[str]:
            return ["a", "b"]

    else:

        @wf.job()
        def init(output: Annotated[str, Param(help="out")]) -> list[str]:
            return ["a", "b"]

    @wf.array_job()
    def write(item: Annotated[str, Result(step="init")]) -> str:
        return item

    wf.validate()
    return wf


@pytest.fixture
def store(tmp_path: Path) -> Iterator[SqliteStore]:
    st = SqliteStore(tmp_path / "db.sqlite")
    st.init()
    yield st
    st.close()


def _submit(
    wf: Workflow,
    store: SqliteStore,
    exc: FakeExecutor,
    run_dir: Path,
    target: str,
    param: str = "target",
) -> str:
    return wf.submit_run(
        run_dir=run_dir, parameters={param: target}, store=store, executor=exc
    )


def _dispatch_jobs(store: SqliteStore, run_id: str) -> list[str]:
    return store.list_jobs(run_id, "dispatch")


class TestKeys:
    def test_paths_are_resolved(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        assert normalise_key("./out") == str(tmp_path / "out")
        assert normalise_key(tmp_path / "out" / ".." / "out") == str(tmp_path / "out")

    def test_urls_lose_trailing_slashes(self) -> None:
        assert normalise_key("s3://bucket/era5/") == "s3://bucket/era5"

    def test_labels_and_lists(self, tmp_path: Path) -> None:
        keys = resource_keys(["target"], {"target": [str(tmp_path / "a"), "s3://b/x"]})
        assert keys == {
            str(tmp_path / "a"): f"target={tmp_path / 'a'}",
            "s3://b/x": "target=s3://b/x",
        }

    def test_callable(self) -> None:
        keys = resource_keys(lambda p: [f"s3://b/{p['year']}/"], {"year": 2020})
        assert keys == {"s3://b/2020": "s3://b/2020"}

    def test_unknown_parameter_is_rejected_at_validation(self) -> None:
        wf = Workflow("w", config=Config({}), exclusive=["nope"])

        @wf.job()
        def step(target: Annotated[str, Param(help="t")]) -> str:
            return target

        with pytest.raises(ValueError, match=r"exclusive=\['nope'\]"):
            wf.validate()


class TestSubmit:
    def test_second_run_on_same_target_is_refused(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        wf, exc = _workflow(), FakeExecutor()
        first = _submit(wf, store, exc, tmp_path / "r1", str(tmp_path / "out"))
        exc.status[_dispatch_jobs(store, first)[0]] = RUNNING

        with pytest.raises(RunConflictError) as info:
            _submit(wf, store, exc, tmp_path / "r2", str(tmp_path / "out"))
        message = str(info.value)
        assert first in message
        assert f"target={tmp_path / 'out'}" in message
        assert "a dispatch pass is queued or running" in message
        assert len(store.list_runs()) == 1, "the refused run must not be created"

    def test_other_targets_are_independent(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        wf, exc = _workflow(), FakeExecutor()
        first = _submit(wf, store, exc, tmp_path / "r1", str(tmp_path / "a"))
        exc.status[_dispatch_jobs(store, first)[0]] = RUNNING
        _submit(wf, store, exc, tmp_path / "r2", str(tmp_path / "b"))
        assert len(store.list_claims()) == 2

    def test_keys_are_global_across_workflows(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        exc = FakeExecutor()
        first = _submit(
            _workflow("a"), store, exc, tmp_path / "r1", str(tmp_path / "o")
        )
        exc.status[_dispatch_jobs(store, first)[0]] = RUNNING
        other = _workflow("b", param="output")
        with pytest.raises(RunConflictError):
            _submit(other, store, exc, tmp_path / "r2", str(tmp_path / "o"), "output")

    def test_unknown_liveness_refuses(self, tmp_path: Path, store: SqliteStore) -> None:
        wf, exc = _workflow(), FakeExecutor()  # no states: UNKNOWN
        _submit(wf, store, exc, tmp_path / "r1", str(tmp_path / "out"))
        with pytest.raises(RunConflictError, match="cannot tell"):
            _submit(wf, store, exc, tmp_path / "r2", str(tmp_path / "out"))

    def test_dead_holder_is_released(self, tmp_path: Path, store: SqliteStore) -> None:
        wf, exc = _workflow(), FakeExecutor()
        first = _submit(wf, store, exc, tmp_path / "r1", str(tmp_path / "out"))
        exc.status[_dispatch_jobs(store, first)[0]] = ended("CANCELLED")
        second = _submit(wf, store, exc, tmp_path / "r2", str(tmp_path / "out"))
        assert store.get_run(first)["status"] == RunState.FAILED.value
        assert [c["run_id"] for c in store.list_claims()] == [second]

    def test_live_task_jobs_keep_the_claim(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        wf, exc = _workflow(), FakeExecutor()
        first = _submit(wf, store, exc, tmp_path / "r1", str(tmp_path / "out"))
        wf.dispatch(first, store, tmp_path / "r1", executor=exc)  # submits init
        for job in _dispatch_jobs(store, first):
            exc.status[job] = ended("")
        task_job = exc.task_jobs()[-1]
        exc.status[task_job] = RUNNING
        with pytest.raises(RunConflictError, match="task jobs are queued or running"):
            _submit(wf, store, exc, tmp_path / "r2", str(tmp_path / "out"))

    def test_runs_without_claims_do_not_block(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        plain = Workflow("plain", config=Config({"executor": {"mode": "sbatch"}}))

        @plain.job()
        def init(target: Annotated[str, Param(help="out")]) -> str:
            return target

        exc = FakeExecutor()
        plain.submit_run(
            run_dir=tmp_path / "r1",
            parameters={"target": str(tmp_path / "out")},
            store=store,
            executor=exc,
        )
        _submit(_workflow(), store, exc, tmp_path / "r2", str(tmp_path / "out"))


class TestRelease:
    def _finish(
        self, wf: Workflow, store: SqliteStore, exc: FakeExecutor, run_id: str, rd: Path
    ) -> None:
        wf.dispatch(run_id, store, rd, executor=exc)
        row = store.get_task_instance(run_id, "init", None)
        write_result(
            run_id, "init", None, int(row["id"]), TaskState.SUCCESS, ["a"], "h"
        )
        wf.dispatch(run_id, store, rd, executor=exc)
        row = store.get_task_instance(run_id, "write", 0)
        write_result(run_id, "write", 0, int(row["id"]), TaskState.SUCCESS, "a", "h2")
        wf.dispatch(run_id, store, rd, executor=exc)

    def test_finished_run_releases(self, tmp_path: Path, store: SqliteStore) -> None:
        wf, exc = _workflow(), FakeExecutor()
        run_id = _submit(wf, store, exc, tmp_path / "r1", str(tmp_path / "out"))
        assert store.list_claims(run_id)
        self._finish(wf, store, exc, run_id, tmp_path / "r1")
        assert store.get_run(run_id)["status"] == RunState.SUCCESS.value
        assert store.list_claims(run_id) == []
        _submit(wf, store, exc, tmp_path / "r2", str(tmp_path / "out"))

    def test_cancel_releases(self, tmp_path: Path, store: SqliteStore) -> None:
        wf, exc = _workflow(), FakeExecutor()
        run_id = _submit(wf, store, exc, tmp_path / "r1", str(tmp_path / "out"))
        wf.cancel_run(run_id, store, executor=exc)
        assert store.list_claims() == []

    def test_retry_reclaims_and_respects_new_holders(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        wf, exc = _workflow(), FakeExecutor()
        first = _submit(wf, store, exc, tmp_path / "r1", str(tmp_path / "out"))
        wf.dispatch(first, store, tmp_path / "r1", executor=exc)
        exc.states[exc.task_jobs()[-1]] = JobStates({None: ended("OUT_OF_MEMORY")})
        # The dispatch job that waited for `init` runs: init was killed.
        wf.dispatch(
            first, store, tmp_path / "r1", executor=exc, after_ref=exc.last_after_ref()
        )
        assert store.get_run(first)["status"] == RunState.FAILED.value
        assert store.list_claims() == []

        # Someone else takes the target meanwhile ...
        second = _submit(wf, store, exc, tmp_path / "r2", str(tmp_path / "out"))
        exc.status[_dispatch_jobs(store, second)[0]] = RUNNING
        with pytest.raises(RunConflictError):
            wf.retry_failed(first, store, tmp_path / "r1", executor=exc)
        assert store.get_run(first)["status"] == RunState.FAILED.value

        # ... and once they are done, the retry gets it back.
        wf.cancel_run(second, store, executor=exc)
        assert wf.retry_failed(first, store, tmp_path / "r1", executor=exc) == 1
        assert [c["run_id"] for c in store.list_claims()] == [first]

    def test_retry_fails_lost_instances_first(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        """A run whose dispatcher died still has SUBMITTED instances."""
        wf, exc = _workflow(), FakeExecutor()
        run_id = _submit(wf, store, exc, tmp_path / "r1", str(tmp_path / "out"))
        wf.dispatch(run_id, store, tmp_path / "r1", executor=exc)
        exc.status[exc.task_jobs()[-1]] = ended("")
        # No further dispatch pass ran.  `retry` must still pick it up:
        assert wf.retry_failed(run_id, store, tmp_path / "r1", executor=exc) == 1


class TestLocal:
    def test_run_local_holds_and_releases(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        wf = Workflow("loc", config=Config({}), exclusive=["target"])
        seen: list[list[str]] = []

        @wf.job()
        def step(target: Annotated[str, Param(help="t")]) -> str:
            seen.append([c["label"] for c in store.list_claims()])
            return target

        wf.run_local(run_dir=tmp_path / "r", store=store, target=str(tmp_path / "o"))
        assert seen == [[f"target={tmp_path / 'o'}"]]
        assert store.list_claims() == []

    def test_failed_run_local_releases(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        wf = Workflow("loc", config=Config({}), exclusive=["target"])

        @wf.job()
        def step(target: Annotated[str, Param(help="t")]) -> str:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            wf.run_local(run_dir=tmp_path / "r", store=store, target="s3://b/x")
        assert store.list_claims() == []

    def test_local_holder_liveness(self, tmp_path: Path, store: SqliteStore) -> None:
        dead = subprocess.Popen([sys.executable, "-c", "pass"])
        dead.wait()
        for pid, should_block in ((dead.pid, False), (os.getpid(), True)):
            store.insert_run(f"holder-{pid}", "loc", "u", {})
            store.claim_resources(
                f"holder-{pid}",
                {"s3://b/x": "target=s3://b/x"},
                {"user": "u", "host": socket.gethostname(), "pid": pid},
            )
            wf, exc = _workflow(), FakeExecutor()
            if should_block:
                with pytest.raises(RunConflictError, match="runs locally here"):
                    _submit(wf, store, exc, tmp_path / "r", "s3://b/x")
            else:
                _submit(wf, store, exc, tmp_path / "r", "s3://b/x")
                store.release_resources(store.list_claims()[0]["run_id"])

    def test_local_holder_on_other_host_is_unknown(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        store.insert_run("holder", "loc", "u", {})
        store.claim_resources(
            "holder", {"s3://b/x": "t"}, {"user": "u", "host": "elsewhere", "pid": 1}
        )
        with pytest.raises(RunConflictError, match="host elsewhere"):
            _submit(_workflow(), store, FakeExecutor(), tmp_path / "r", "s3://b/x")


def test_concurrent_claims_have_one_winner(tmp_path: Path) -> None:
    path = tmp_path / "db.sqlite"
    SqliteStore(path).init()
    barrier = threading.Barrier(8)
    results: list[bool] = []
    lock = threading.Lock()

    def contender(n: int) -> None:
        st = SqliteStore(path)
        barrier.wait()
        won = not st.claim_resources(f"run-{n}", {"k": "k"}, {"user": "u"})
        with lock:
            results.append(won)
        st.close()

    threads = [threading.Thread(target=contender, args=(n,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results.count(True) == 1
    assert len(SqliteStore(path).list_claims()) == 1


def test_cli_reports_conflict_with_exit_code_2(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reflow.cli import parse_args, run_command

    from reflow.executors import Executor

    monkeypatch.setenv("REFLOW_MODE", "dry-run")
    # Dry-run jobs never run; pretend the first run's dispatch job does.
    monkeypatch.setattr(Executor, "jobs_status", lambda self, ids: RUNNING)
    monkeypatch.setenv("REFLOW_STORE_PATH", str(tmp_path / "db.sqlite"))
    wf = Workflow("cli-claims", exclusive=["target"])

    @wf.job()
    def step(target: Annotated[str, Param(help="t")]) -> str:
        return target

    def submit(run: str) -> int:
        argv = ["submit", "--run-dir", str(tmp_path / run), "--target", "s3://b/x/"]
        return run_command(wf, parse_args(wf, argv))

    assert submit("r1") == 0
    capsys.readouterr()
    assert submit("r2") == 2
    err = capsys.readouterr().err
    assert "target=s3://b/x is held by run cli-claims-" in err


class TestLiveness:
    """Liveness only uses `job_status`, so it holds for every backend."""

    def _holder(self, tmp_path: Path, store: SqliteStore) -> tuple[str, FakeExecutor]:
        wf, exc = _workflow(), FakeExecutor()
        run_id = _submit(wf, store, exc, tmp_path / "r1", str(tmp_path / "out"))
        wf.dispatch(run_id, store, tmp_path / "r1", executor=exc)  # submits init
        return run_id, exc

    def _check(self, run_id: str, store: SqliteStore, exc: FakeExecutor) -> Any:
        from reflow.claims import run_liveness

        return run_liveness(run_id, store, exc, store.list_claims(run_id)[0])

    def test_newest_dispatch_job_decides_first(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        run_id, exc = self._holder(tmp_path, store)
        asked: list[str] = []
        exc.job_status = lambda job_id: asked.append(job_id) or RUNNING  # type: ignore[method-assign]
        assert self._check(run_id, store, exc).alive is True
        assert asked == [_dispatch_jobs(store, run_id)[-1]]

    def test_dead_needs_every_job_ended(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        run_id, exc = self._holder(tmp_path, store)
        for job in [*_dispatch_jobs(store, run_id), *exc.task_jobs()]:
            exc.status[job] = ended("")
        assert self._check(run_id, store, exc).alive is False

        exc.status[_dispatch_jobs(store, run_id)[0]] = ElementState(JobStatus.UNKNOWN)
        assert self._check(run_id, store, exc).alive is None

    def test_run_without_recorded_jobs_is_unknown(
        self, tmp_path: Path, store: SqliteStore
    ) -> None:
        run_id, exc = self._holder(tmp_path, store)
        store.conn.execute("DELETE FROM run_jobs")
        store.conn.execute("UPDATE task_instances SET job_id = NULL")
        store.conn.commit()
        assert self._check(run_id, store, exc).alive is None

    def test_flux_asks_about_all_jobs_together(self) -> None:
        from unittest.mock import patch

        from reflow.executors.flux import FluxExecutor
        from reflow.executors.util import CommandResult

        out = CommandResult(cmd=[], stdout="ƒa INACTIVE COMPLETED\nƒb RUN", stderr="", returncode=0)
        with patch("reflow.executors.flux.run_cmd", return_value=out) as run:
            assert FluxExecutor().jobs_status(["ƒa", "ƒb"]).active
        run.assert_called_once()


def test_failed_first_submission_releases_the_claim(
    tmp_path: Path, store: SqliteStore
) -> None:
    """A run whose dispatch job could not be submitted must not block."""

    class Broken(FakeExecutor):
        def submit(self, resources: JobResources, command: list[str]) -> str:
            raise RuntimeError("sbatch: error: invalid partition")

    wf = _workflow()
    with pytest.raises(RuntimeError, match="invalid partition"):
        _submit(wf, store, Broken(), tmp_path / "r1", str(tmp_path / "out"))
    assert store.list_claims() == []
    (run,) = store.list_runs()
    assert run["status"] == RunState.FAILED.value
    _submit(wf, store, FakeExecutor(), tmp_path / "r2", str(tmp_path / "out"))
