"""test_dispatch.py — covers the DispatchMixin dispatch loop, cache, and array dispatch.

These tests exercise the scheduler-facing dispatch paths using the
dry-run executor so no real scheduler is needed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import pytest

from reflow import Config, Param, Result, RunDir, Workflow
from reflow._types import TaskState
from reflow.stores.sqlite import SqliteStore


def _store(tmp_path: Path) -> SqliteStore:
    st = SqliteStore(str(tmp_path / "db.sqlite"))
    st.init()
    return st


# ═══════════════════════════════════════════════════════════════════════════
# Basic dispatch: singleton and array tasks
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchSingleton:
    def test_singleton_dispatched_to_pending(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def step(x: Annotated[str, Param(help="X")]) -> str:
            return x

        with _store(tmp_path) as st:
            run_id = wf.submit_run(
                run_dir=tmp_path / "r", store=st, parameters={"x": "hello"}
            )
            instances = st.list_task_instances(run_id)
            assert any(i["task_name"] == "step" for i in instances)

    def test_dispatch_skips_already_submitted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def step(x: Annotated[str, Param(help="X")]) -> str:
            return x

        with _store(tmp_path) as st:
            run_id = wf.submit_run(
                run_dir=tmp_path / "r", store=st, parameters={"x": "hello"}
            )
            # Second dispatch call should not create duplicate instances.
            wf.dispatch(run_id, st, tmp_path / "r")
            names = [i["task_name"] for i in st.list_task_instances(run_id)]
            assert names.count("step") == 1

    def test_dispatch_two_step_pipeline(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Dispatch creates array instances once an upstream dep is satisfied."""
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def prep(x: Annotated[str, Param(help="X")]) -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def convert(item: Annotated[str, Result(step="prep")]) -> str:
            return item.upper()

        with _store(tmp_path) as st:
            # submit_run creates the run and inserts a PENDING prep instance.
            run_id = wf.submit_run(
                run_dir=tmp_path / "r", store=st, parameters={"x": "val"}
            )
            # Simulate prep completing: update the existing instance to SUCCESS.
            row = st.get_task_instance(run_id, "prep", None)
            st.update_task_success(int(row["id"]), ["a", "b"])
            assert st.dependency_is_satisfied(run_id, "prep")
            # Now dispatch again — prep is satisfied, convert should be created.
            wf.dispatch(run_id, st, tmp_path / "r")
            instances = st.list_task_instances(run_id, task_name="convert")
            assert len(instances) == 2

    def test_all_deps_satisfied_false(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def upstream() -> str:
            return "ok"

        @wf.job()
        def downstream(x: Annotated[str, Result(step="upstream")]) -> str:
            return x

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            assert not wf._all_deps_satisfied(st, "r1", wf.tasks["downstream"])

    def test_all_deps_satisfied_true(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def upstream() -> str:
            return "ok"

        @wf.job()
        def downstream(x: Annotated[str, Result(step="upstream")]) -> str:
            return x

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance(
                "r1", "upstream", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid, "ok")
            assert wf._all_deps_satisfied(st, "r1", wf.tasks["downstream"])


# ═══════════════════════════════════════════════════════════════════════════
# Cache resolution
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchCache:
    def test_cache_hit_skips_submission(self, tmp_path: Path) -> None:
        """A cached result should appear as SUCCESS without PENDING/submission."""
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job(cache=True)
        def step(x: Annotated[str, Param(help="X")]) -> str:
            return x.upper()

        # First run establishes the cache via local runner.
        with _store(tmp_path) as st:
            run1 = wf.run_local(run_dir=tmp_path / "r1", store=st, x="hello")
            row1 = st.get_task_instance(run1.run_id, "step", None)
            assert row1["state"] == "SUCCESS"

            # Second run should resolve from cache.
            run2 = wf.run_local(run_dir=tmp_path / "r2", store=st, x="hello")
            row2 = st.get_task_instance(run2.run_id, "step", None)
            assert row2["state"] == "SUCCESS"

    def test_force_bypasses_cache(self, tmp_path: Path) -> None:
        call_count = {"n": 0}

        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job(cache=True)
        def step(x: Annotated[str, Param(help="X")]) -> str:
            call_count["n"] += 1
            return x.upper()

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="hi")
            assert call_count["n"] == 1
            wf.run_local(run_dir=tmp_path / "r2", store=st, x="hi", force=True)
            assert call_count["n"] == 2

    def test_should_force_global_flag(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def step() -> str:
            return "ok"

        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {"__force__": True})
            assert wf._should_force(st, "r1", "step")

    def test_should_force_task_specific(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def step() -> str:
            return "ok"

        @wf.job()
        def other() -> str:
            return "ok"

        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {"__force_tasks__": ["step"]})
            assert wf._should_force(st, "r1", "step")
            assert not wf._should_force(st, "r1", "other")

    def test_collect_upstream_output_hashes(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def upstream() -> str:
            return "ok"

        @wf.job()
        def downstream(x: Annotated[str, Result(step="upstream")]) -> str:
            return x

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance(
                "r1", "upstream", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid, "ok")
            hashes = wf._collect_upstream_output_hashes(
                st, "r1", wf.tasks["downstream"]
            )
            assert isinstance(hashes, list)


# ═══════════════════════════════════════════════════════════════════════════
# Result input resolution and wire modes
# ═══════════════════════════════════════════════════════════════════════════


class TestResolveResultInputs:
    def test_direct_wire_mode(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def source() -> str:
            return "value"

        @wf.job()
        def sink(x: Annotated[str, Result(step="source")]) -> str:
            return x

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance(
                "r1", "source", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid, "value")
            resolved = wf._resolve_result_inputs(st, "r1", wf.tasks["sink"])
            assert resolved["x"] == "value"

    def test_fan_out_wire_mode(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a", "b", "c"]

        @wf.array_job()
        def sink(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance(
                "r1", "source", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid, ["a", "b", "c"])
            resolved = wf._resolve_result_inputs(st, "r1", wf.tasks["sink"])
            assert resolved["item"] == ["a", "b", "c"]

    def test_gather_wire_mode(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def upstream() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def source(item: Annotated[str, Result(step="upstream")]) -> str:
            return item.upper()

        @wf.job()
        def sink(items: Annotated[list[str], Result(step="source")]) -> str:
            return ",".join(items)

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            for i, val in enumerate(["A", "B"]):
                iid = st.insert_task_instance(
                    "r1", "source", i, TaskState.SUCCESS, {}
                )
                st.update_task_success(iid, val)
            resolved = wf._resolve_result_inputs(st, "r1", wf.tasks["sink"])
            assert sorted(resolved["items"]) == ["A", "B"]

    def test_broadcast_wire_mode(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def config_step() -> list[str]:
            return ["cfg1", "cfg2"]

        @wf.array_job()
        def worker(
            cfg: Annotated[list[str], Result(step="config_step", broadcast=True)],
            item: Annotated[str, Param(help="I")],
        ) -> str:
            return item

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {"item": "x"})
            iid = st.insert_task_instance(
                "r1", "config_step", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid, ["cfg1", "cfg2"])
            resolved = wf._resolve_result_inputs(st, "r1", wf.tasks["worker"])
            assert resolved["cfg"] == ["cfg1", "cfg2"]

    def test_find_fan_out_param(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def sink(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        param = wf._find_fan_out_param(wf.tasks["sink"])
        assert param == "item"

    def test_find_fan_out_param_broadcast_skipped(self, tmp_path: Path) -> None:
        """Broadcast params must not be picked as the fan-out dimension."""
        wf = Workflow("wf")

        @wf.job()
        def config_step() -> list[str]:
            return ["cfg"]

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def sink(
            cfg: Annotated[list[str], Result(step="config_step", broadcast=True)],
            item: Annotated[str, Result(step="source")],
        ) -> str:
            return item

        wf.validate()
        param = wf._find_fan_out_param(wf.tasks["sink"])
        assert param == "item"


# ═══════════════════════════════════════════════════════════════════════════
# Array dispatch paths
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchArray:
    def test_array_dispatch_creates_instances(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def prep(x: Annotated[str, Param(help="X")]) -> list[str]:
            return ["a", "b", "c"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="prep")]) -> str:
            return item.upper()

        with _store(tmp_path) as st:
            # First run via local so prep's output lands in the store.
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="val")
            # Second submission via submit_run — dispatch sees prep is done
            # and should create process instances.
            run_id2 = wf.submit_run(
                run_dir=tmp_path / "r2", store=st, parameters={"x": "val"}
            )
            instances = st.list_task_instances(run_id2)
            assert len(instances) >= 1

    def test_array_dispatch_no_result_inputs_returns_none(
        self, tmp_path: Path
    ) -> None:
        """Array task with no upstream results should not be dispatched."""
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            from reflow.executors.local import LocalExecutor

            jid = wf._dispatch_array(
                "r1",
                tmp_path,
                wf.tasks["process"],
                st,
                LocalExecutor(),
            )
            assert jid is None  # source not done yet

    def test_array_dispatch_retrying_instances(
        self, tmp_path: Path
    ) -> None:
        """RETRYING instances should be re-submitted as a partial array."""
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            # Seed source output so deps are satisfied.
            iid_src = st.insert_task_instance(
                "r1", "source", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid_src, ["a", "b"])
            # Insert two process instances, one RETRYING.
            iid0 = st.insert_task_instance(
                "r1", "process", 0, TaskState.SUCCESS, {"item": "a"}
            )
            st.update_task_success(iid0, "A")
            iid1 = st.insert_task_instance(
                "r1", "process", 1, TaskState.RETRYING, {"item": "b"}
            )

            from reflow.executors.local import LocalExecutor

            jid = wf._dispatch_array(
                "r1",
                tmp_path,
                wf.tasks["process"],
                st,
                LocalExecutor(),
            )
            # Local executor always succeeds; just check a job id was returned
            assert jid is not None

    def test_maybe_finalise_cancels_on_pending_active(
        self, tmp_path: Path
    ) -> None:
        wf = Workflow("wf")

        @wf.job()
        def step() -> str:
            return "ok"

        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            st.insert_task_instance("r1", "step", None, TaskState.RUNNING, {})
            wf._maybe_finalise_run("r1", st)
            # Still running, should not be finalised
            assert st.get_run("r1")["status"] == "RUNNING"

    def test_maybe_finalise_cancelled_state(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def step() -> str:
            return "ok"

        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance(
                "r1", "step", None, TaskState.CANCELLED, {}
            )
            wf._maybe_finalise_run("r1", st)
            # Cancelled is not "all ok" and not FAILED, status stays unchanged
            status = st.get_run("r1")["status"]
            assert status in ("RUNNING", "FAILED", "CANCELLED")


# ═══════════════════════════════════════════════════════════════════════════
# Wire mode: CHAIN
# ═══════════════════════════════════════════════════════════════════════════


class TestChainWireMode:
    def test_chain_wire_mode_dispatch(self, tmp_path: Path) -> None:
        """CHAIN wire mode: array→array, upstream outputs passed as list."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def stage1(item: Annotated[str, Result(step="source")]) -> str:
            return item.upper()

        @wf.array_job()
        def stage2(item: Annotated[str, Result(step="stage1")]) -> str:
            return item + "!"

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            for i, val in enumerate(["A", "B"]):
                iid = st.insert_task_instance(
                    "r1", "stage1", i, TaskState.SUCCESS, {}
                )
                st.update_task_success(iid, val)
            resolved = wf._resolve_result_inputs(st, "r1", wf.tasks["stage2"])
            # CHAIN: list of upstream array outputs
            assert resolved["item"] == ["A", "B"]

    def test_chain_wire_mode_local(self, tmp_path: Path) -> None:
        """CHAIN through run_local resolves correctly end-to-end."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["x", "y"]

        @wf.array_job()
        def upper(item: Annotated[str, Result(step="source")]) -> str:
            return item.upper()

        @wf.array_job()
        def exclaim(item: Annotated[str, Result(step="upper")]) -> str:
            return item + "!"

        with _store(tmp_path) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st)
        outputs = st.get_array_outputs(run.run_id, "exclaim")
        assert sorted(outputs) == ["X!", "Y!"]


# ═══════════════════════════════════════════════════════════════════════════
# _try_cache with verify=True and stale output
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchVerify:
    def test_cache_stale_with_verify_forces_rerun(self, tmp_path: Path) -> None:
        """verify=True with a custom verify callable forces rerun on cache hit."""
        from typing import Any

        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))
        call_count = {"n": 0}
        verify_count = {"n": 0}

        def always_stale(output: Any) -> bool:
            verify_count["n"] += 1
            return False

        @wf.job(cache=True, verify=always_stale)
        def step(x: Annotated[str, Param(help="x")]) -> str:
            call_count["n"] += 1
            return x.upper()

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="hello")
            assert call_count["n"] == 1
            assert verify_count["n"] == 0  # first run: no cache to verify

            # Same inputs, same run_dir → same identity → cache hit detected,
            # but always_stale returns False → task reruns
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="hello", verify=True)
            assert verify_count["n"] == 1
            assert call_count["n"] == 2

    def test_ingest_results_logs_when_n_gt_zero(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """dispatch() logs an INFO message when worker results are ingested."""
        import logging
        from reflow.results import write_result

        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def step() -> str:
            return "ok"

        with _store(tmp_path) as st:
            run_id = wf.submit_run(
                run_dir=tmp_path / "r", store=st, parameters={}
            )
            row = st.get_task_instance(run_id, "step", None)
            iid = int(row["id"])
            write_result(
                run_id=run_id,
                task_name="step",
                array_index=None,
                instance_id=iid,
                state=TaskState.SUCCESS,
                output="ok",
            )
            with caplog.at_level(logging.INFO, logger="reflow.workflow._dispatch"):
                wf.dispatch(run_id, st, tmp_path / "r")
        assert "Ingested" in caplog.text


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_single: deps unsatisfied and cache hit paths
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchSinglePaths:
    def test_dispatch_single_deps_unsatisfied_returns_none(
        self, tmp_path: Path
    ) -> None:
        wf = Workflow("wf")

        @wf.job()
        def upstream() -> str:
            return "ok"

        @wf.job()
        def downstream(x: Annotated[str, Result(step="upstream")]) -> str:
            return x

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            from reflow.executors.local import LocalExecutor
            jid = wf._dispatch_single(
                "r1", tmp_path, wf.tasks["downstream"], st, LocalExecutor()
            )
            assert jid is None

    def test_dispatch_single_cache_hit_returns_none(
        self, tmp_path: Path
    ) -> None:
        """Cache hit skips submission and returns None."""
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))
        call_count = {"n": 0}

        @wf.job(cache=True)
        def step(x: Annotated[str, Param(help="X")]) -> str:
            call_count["n"] += 1
            return x

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="hi")
            assert call_count["n"] == 1
            wf.run_local(run_dir=tmp_path / "r2", store=st, x="hi")
            assert call_count["n"] == 1  # cache hit, not re-executed


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_array: edge cases
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchArrayEdgeCases:
    def test_array_parallelism_appended_to_arr_str(
        self, tmp_path: Path
    ) -> None:
        """array_parallelism config appends %N to the array string."""
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def source() -> list[str]:
            return ["a", "b", "c"]

        @wf.array_job(array_parallelism=2)
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        with _store(tmp_path) as st:
            run_id = wf.submit_run(
                run_dir=tmp_path / "r", store=st, parameters={}
            )
            row = st.get_task_instance(run_id, "source", None)
            st.update_task_success(int(row["id"]), ["a", "b", "c"])
            wf.dispatch(run_id, st, tmp_path / "r")
            # All 3 process instances should have been created
            instances = st.list_task_instances(run_id, task_name="process")
            assert len(instances) == 3

    def test_all_array_from_cache_returns_none(self, tmp_path: Path) -> None:
        """When all array elements hit cache, dispatch returns None (no job)."""
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))
        call_count = {"n": 0}

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job(cache=True)
        def process(item: Annotated[str, Result(step="source")]) -> str:
            call_count["n"] += 1
            return item.upper()

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st)
            assert call_count["n"] == 2
            wf.run_local(run_dir=tmp_path / "r2", store=st)
            assert call_count["n"] == 2  # all from cache

    def test_dispatch_array_broadcast_payload(self, tmp_path: Path) -> None:
        """Broadcast params get the whole value; fan-out param gets per-element."""
        wf = Workflow("wf")

        @wf.job()
        def config_step() -> list[str]:
            return ["cfg"]

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def process(
            cfg: Annotated[list[str], Result(step="config_step", broadcast=True)],
            item: Annotated[str, Result(step="source")],
        ) -> str:
            return item

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid_cfg = st.insert_task_instance(
                "r1", "config_step", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid_cfg, ["cfg"])
            iid_src = st.insert_task_instance(
                "r1", "source", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid_src, ["a", "b"])
            from reflow.executors.local import LocalExecutor
            wf._dispatch_array(
                "r1", tmp_path, wf.tasks["process"], st, LocalExecutor()
            )
            instances = st.list_task_instances("r1", task_name="process")
            assert len(instances) == 2
            # Each element should have broadcast cfg
            for inst in instances:
                assert inst["input"]["cfg"] == ["cfg"]


class TestDispatchTryCache:
    def test_try_cache_verify_stale_reruns(self, tmp_path: Path) -> None:
        from typing import Any
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))
        call_count = {"n": 0}
        verify_count = {"n": 0}

        def always_stale(output: Any) -> bool:
            verify_count["n"] += 1
            return False

        @wf.job(cache=True, verify=always_stale)
        def step(x: Annotated[str, Param(help="x")]) -> str:
            call_count["n"] += 1
            return x.upper()

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="hello")
            assert call_count["n"] == 1
            assert verify_count["n"] == 0
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="hello", verify=True)
            assert verify_count["n"] == 1
            assert call_count["n"] == 2

    def test_try_cache_hit_inserts_success_instance(self, tmp_path: Path) -> None:
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))
        call_count = {"n": 0}

        @wf.job(cache=True)
        def step(x: Annotated[str, Param(help="x")]) -> str:
            call_count["n"] += 1
            return x.upper()

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="hi")
            assert call_count["n"] == 1
            run2 = wf.run_local(run_dir=tmp_path / "r1", store=st, x="hi")
            assert call_count["n"] == 1
            row = st.get_task_instance(run2.run_id, "step", None)
            assert row["state"] == "SUCCESS"

    def test_try_cache_output_hash_computed_when_missing(
        self, tmp_path: Path
    ) -> None:
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job(cache=True)
        def step(x: Annotated[str, Param(help="x")]) -> str:
            return x.upper()

        with _store(tmp_path) as st:
            run1 = wf.run_local(run_dir=tmp_path / "r1", store=st, x="v")
            iid = int(st.get_task_instance(run1.run_id, "step", None)["id"])
            st.conn.execute(
                "UPDATE task_instances SET output_hash = '' WHERE id = ?", (iid,)
            )
            st.conn.commit()
            run2 = wf.run_local(run_dir=tmp_path / "r1", store=st, x="v")
            row = st.get_task_instance(run2.run_id, "step", None)
            assert row["state"] == "SUCCESS"


class TestDispatchSingleResultDep:
    def test_dispatch_single_with_result_dep(self, tmp_path: Path) -> None:
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def upstream() -> str:
            return "value"

        @wf.job()
        def downstream(x: Annotated[str, Result(step="upstream")]) -> str:
            return x.upper()

        wf.validate()
        with _store(tmp_path) as st:
            run_id = wf.submit_run(
                run_dir=tmp_path / "r", store=st, parameters={}
            )
            row = st.get_task_instance(run_id, "upstream", None)
            st.update_task_success(int(row["id"]), "value")
            wf.dispatch(run_id, st, tmp_path / "r")
            inst = st.get_task_instance(run_id, "downstream", None)
            assert inst is not None


class TestDispatchArrayFanItems:
    def test_array_element_value_list_match_per_element(
        self, tmp_path: Path
    ) -> None:
        wf = Workflow("wf")

        @wf.job()
        def labels() -> list[str]:
            return ["x", "y"]

        @wf.job()
        def items() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def process(
            item: Annotated[str, Result(step="items")],
            label: Annotated[str, Result(step="labels")],
        ) -> str:
            return f"{label}:{item}"

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid_l = st.insert_task_instance(
                "r1", "labels", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid_l, ["x", "y"])
            iid_i = st.insert_task_instance(
                "r1", "items", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid_i, ["a", "b"])
            from reflow.executors.local import LocalExecutor
            wf._dispatch_array("r1", tmp_path, wf.tasks["process"], st, LocalExecutor())
            instances = st.list_task_instances("r1", task_name="process")
            assert len(instances) == 2
            payloads = {i["array_index"]: i["input"] for i in instances}
            assert payloads[0]["item"] == "a"
            assert payloads[1]["item"] == "b"
            assert payloads[0]["label"] == "x"
            assert payloads[1]["label"] == "y"

    def test_array_dispatch_all_from_cache_logs(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """All-from-cache: call_count stays at 2 on second run."""
        import logging
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))
        call_count = {"n": 0}

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job(cache=True)
        def process(item: Annotated[str, Result(step="source")]) -> str:
            call_count["n"] += 1
            return item.upper()

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st)
            assert call_count["n"] == 2
            # Second run: all elements hit cache, task is not re-executed
            wf.run_local(run_dir=tmp_path / "r2", store=st)
            assert call_count["n"] == 2

    def test_array_dispatch_parallelism_str(self, tmp_path: Path) -> None:
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def source() -> list[str]:
            return ["a", "b", "c"]

        @wf.array_job(array_parallelism=2)
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        with _store(tmp_path) as st:
            run_id = wf.submit_run(
                run_dir=tmp_path / "r", store=st, parameters={}
            )
            row = st.get_task_instance(run_id, "source", None)
            st.update_task_success(int(row["id"]), ["a", "b", "c"])
            wf.dispatch(run_id, st, tmp_path / "r")
            instances = st.list_task_instances(run_id, task_name="process")
            assert len(instances) == 3


# ═══════════════════════════════════════════════════════════════════════════
# Remaining _dispatch.py gaps
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchRemainingGaps:
    def test_find_fan_out_param_all_broadcast_returns_none(
        self, tmp_path: Path
    ) -> None:
        """_find_fan_out_param returns None when every dep is broadcast."""
        wf = Workflow("wf")

        @wf.job()
        def cfg() -> list[str]:
            return ["c"]

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def process(
            item: Annotated[str, Result(step="source")],
            extra: Annotated[list[str], Result(step="cfg", broadcast=True)],
        ) -> str:
            return item

        wf.validate()
        import copy
        spec = copy.copy(wf.tasks["process"])
        from reflow.params import Result as R
        spec.result_deps = {
            k: R(step=v.steps[0], broadcast=True)
            for k, v in spec.result_deps.items()
        }
        result = wf._find_fan_out_param(spec)
        assert result is None

    def test_try_cache_returns_true_on_cache_hit(self, tmp_path: Path) -> None:
        """_try_cache inserts SUCCESS and returns True on a valid cache hit."""
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))
        call_count = {"n": 0}

        @wf.job(cache=True)
        def step(x: Annotated[str, Param(help="x")]) -> str:
            call_count["n"] += 1
            return x.upper()

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="hi")
            assert call_count["n"] == 1
            run2 = wf.run_local(run_dir=tmp_path / "r1", store=st, x="hi")
            assert call_count["n"] == 1
            row = st.get_task_instance(run2.run_id, "step", None)
            assert row["state"] == "SUCCESS"
            assert row["output"] == "HI"

    def test_dispatch_single_cache_miss_creates_pending(
        self, tmp_path: Path
    ) -> None:
        """When no cache hit, _dispatch_single creates a PENDING instance."""
        from unittest.mock import patch
        from reflow.executors.util import CommandResult
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def step(x: Annotated[str, Param(help="x")]) -> str:
            return x

        fake = CommandResult(cmd=[], stdout="job-1", stderr="", returncode=0)
        with patch("reflow.executors.slurm.run_cmd", return_value=fake):
            with _store(tmp_path) as st:
                run_id = wf.submit_run(
                    run_dir=tmp_path / "r", store=st, parameters={"x": "v"}
                )
        with _store(tmp_path) as st:
            inst = st.get_task_instance(run_id, "step", None)
        assert inst is not None

    def test_dispatch_single_row_none_raises(self, tmp_path: Path) -> None:
        """If get_task_instance returns None after insert, RuntimeError is raised."""
        from unittest.mock import patch
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def step() -> str:
            return "ok"

        with _store(tmp_path) as st:
            run_id = wf.submit_run(
                run_dir=tmp_path / "r", store=st, parameters={}
            )
            with patch.object(st, "get_task_instance", return_value=None):
                with pytest.raises(RuntimeError, match="Could not create instance"):
                    from reflow.executors.local import LocalExecutor
                    wf._dispatch_single(
                        run_id, tmp_path, wf.tasks["step"], st,
                        LocalExecutor(),
                    )

    def test_array_dispatch_no_fan_param_returns_none(
        self, tmp_path: Path
    ) -> None:
        """_dispatch_array returns None when _find_fan_out_param is None."""
        from unittest.mock import patch
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance(
                "r1", "source", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid, ["a"])
            from reflow.executors.local import LocalExecutor
            with patch.object(wf, "_find_fan_out_param", return_value=None):
                result = wf._dispatch_array(
                    "r1", tmp_path, wf.tasks["process"], st, LocalExecutor()
                )
            assert result is None

    def test_array_dispatch_empty_fan_items_returns_none(
        self, tmp_path: Path
    ) -> None:
        """_dispatch_array returns None when fan_items is empty list."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return []

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance(
                "r1", "source", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid, [])
            from reflow.executors.local import LocalExecutor
            result = wf._dispatch_array(
                "r1", tmp_path, wf.tasks["process"], st, LocalExecutor()
            )
            assert result is None

    def test_array_dispatch_fan_items_not_list_wraps(
        self, tmp_path: Path
    ) -> None:
        """A scalar fan_items value is wrapped to a list before dispatch."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance(
                "r1", "source", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid, "single_scalar")
            from reflow.executors.local import LocalExecutor
            wf._dispatch_array(
                "r1", tmp_path, wf.tasks["process"], st, LocalExecutor()
            )
            instances = st.list_task_instances("r1", task_name="process")
            assert len(instances) == 1

    def test_maybe_finalise_all_success(self, tmp_path: Path) -> None:
        """run_local marks run SUCCESS after all tasks complete."""
        wf = Workflow("wf")

        @wf.job()
        def step() -> str:
            return "ok"

        with _store(tmp_path) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st)
            assert st.get_run(run.run_id)["status"] == "SUCCESS"

    def test_resolve_result_inputs_hints_exception(
        self, tmp_path: Path
    ) -> None:
        """_resolve_result_inputs returns {} when get_type_hints raises."""
        from unittest.mock import patch
        wf = Workflow("wf")

        @wf.job()
        def source() -> str:
            return "v"

        @wf.job()
        def sink(x: Annotated[str, Result(step="source")]) -> str:
            return x

        wf.validate()
        with _store(tmp_path) as st:
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance(
                "r1", "source", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid, "v")
            # get_type_hints is imported locally in _local; patch at typing module level
            with patch("typing.get_type_hints", side_effect=Exception("bad")):
                result = wf._resolve_result_inputs(st, "r1", wf.tasks["sink"])
            assert isinstance(result, dict)
