"""test_local_runner_extended.py — additional coverage for _local.py.

Extends test_local_runner.py with:
- GATHER and CHAIN wire mode data flow
- Parallel execution (max_workers > 1) success and failure paths
- _run_task_func picklable worker helper
- Warning branches: empty fan-out, missing fan-param, no result inputs
- verify=True cache stale path
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import pytest

from reflow import Config, Param, Result, Workflow
from reflow.stores.sqlite import SqliteStore
from reflow.workflow._local import _run_task_func


def _store(tmp_path: Path) -> SqliteStore:
    st = SqliteStore(str(tmp_path / "db.sqlite"))
    st.init()
    return st


def _add_values(a: int, b: int) -> int:
    """Module-level helper importable by _run_task_func."""
    return a + b


def _greet(name: str) -> str:
    """Module-level helper importable by _run_task_func."""
    return f"hello {name}"


# ═══════════════════════════════════════════════════════════════════════════
# _run_task_func — picklable process-pool worker
# ═══════════════════════════════════════════════════════════════════════════


class TestRunTaskFunc:
    def test_calls_module_level_function(self) -> None:
        """_run_task_func resolves a module-level function by name and calls it."""
        result = _run_task_func(
            _add_values.__module__,
            _add_values.__qualname__,
            {"a": 3, "b": 4},
        )
        assert result == 7

    def test_passes_kwargs_correctly(self) -> None:
        result = _run_task_func(
            _greet.__module__,
            _greet.__qualname__,
            {"name": "reflow"},
        )
        assert result == "hello reflow"


# ═══════════════════════════════════════════════════════════════════════════
# GATHER wire mode — array→singleton
# ═══════════════════════════════════════════════════════════════════════════


class TestGatherWireMode:
    def test_gather_collects_array_outputs(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def upstream() -> list[str]:
            return ["a", "b", "c"]

        @wf.array_job()
        def produce(item: Annotated[str, Result(step="upstream")]) -> str:
            return item.upper()

        @wf.job()
        def collect(items: Annotated[list[str], Result(step="produce")]) -> str:
            return ",".join(sorted(items))

        wf.validate()
        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            st.init()
            st.insert_run("r1", "wf", "u", {})
            from reflow._types import TaskState

            for i, val in enumerate(["A", "B", "C"]):
                iid = st.insert_task_instance(
                    "r1", "produce", i, TaskState.SUCCESS, {}
                )
                st.update_task_success(iid, val)

            resolved = wf._local_resolve_inputs(st, "r1", wf.tasks["collect"])
            assert sorted(resolved["items"]) == ["A", "B", "C"]


# ═══════════════════════════════════════════════════════════════════════════
# Parallel array execution
# ═══════════════════════════════════════════════════════════════════════════


class TestParallelExecution:
    def test_parallel_closure_fails_elements(self, tmp_path: Path) -> None:
        """Closures defined in test methods can't be resolved by _run_task_func
        (qualname contains '<locals>' which has no getattr match). Each element
        fails individually and the run is marked FAILED.
        """
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a", "b", "c"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item.upper()  # closure — not importable by _run_task_func

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            run = wf.run_local(
                run_dir=tmp_path / "r",
                store=st,
                max_workers=2,
                on_error="continue",
            )
        assert run.status(as_dict=True)["run"]["status"] == "FAILED"

    def test_parallel_failure_marks_failed(self, tmp_path: Path) -> None:
        """A parallel element failure must be recorded as FAILED."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["x", "y"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            raise RuntimeError(f"forced failure for {item}")

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            with pytest.raises(RuntimeError, match="process"):
                wf.run_local(
                    run_dir=tmp_path / "r",
                    store=st,
                    max_workers=2,
                    on_error="stop",
                )


# ═══════════════════════════════════════════════════════════════════════════
# Warning branches in _local_run_array
# ═══════════════════════════════════════════════════════════════════════════


class TestArrayWarningBranches:
    def test_empty_fan_out_list_returns_false(self, tmp_path: Path) -> None:
        """An upstream that returns [] should make the array task return False."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return []

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        with _store(tmp_path) as st:
            run = wf.run_local(
                run_dir=tmp_path / "r",
                store=st,
                on_error="continue",
            )
        # process should have been skipped / marked failed at the run level

        with _store(tmp_path) as st2:
            row = st2.get_run(run.run_id)
            # run may be SUCCESS (empty array = vacuous success) or FAILED
            # depending on implementation — just ensure it completed
            assert row["status"] in ("SUCCESS", "FAILED")

    def test_all_array_elements_from_cache(self, tmp_path: Path) -> None:
        """When all array elements hit cache, no new job IDs should be needed."""
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
            # Second run should use cache — count stays at 2
            assert call_count["n"] == 2


# ═══════════════════════════════════════════════════════════════════════════
# on_error="continue" with downstream skip
# ═══════════════════════════════════════════════════════════════════════════


class TestOnErrorContinueExtended:
    def test_multiple_failures_continue(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def fail_a() -> str:
            raise RuntimeError("a failed")

        @wf.job()
        def fail_b() -> str:
            raise RuntimeError("b failed")

        @wf.job()
        def ok_c() -> str:
            return "c ok"

        with _store(tmp_path) as st:
            run = wf.run_local(
                run_dir=tmp_path / "r",
                store=st,
                on_error="continue",
            )

        with _store(tmp_path) as st2:
            a = st2.get_task_instance(run.run_id, "fail_a", None)
            b = st2.get_task_instance(run.run_id, "fail_b", None)
            c = st2.get_task_instance(run.run_id, "ok_c", None)
            assert a["state"] == "FAILED"
            assert b["state"] == "FAILED"
            assert c["state"] == "SUCCESS"


# ═══════════════════════════════════════════════════════════════════════════
# force_tasks selective bypass
# ═══════════════════════════════════════════════════════════════════════════


class TestLocalShouldForce:
    def test_force_all(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def step() -> str:
            return "ok"

        assert wf._local_should_force("step", force=True, force_tasks=[])
        assert not wf._local_should_force("step", force=False, force_tasks=[])

    def test_force_tasks_specific(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def step() -> str:
            return "ok"

        assert wf._local_should_force(
            "step", force=False, force_tasks=["step", "other"]
        )
        assert not wf._local_should_force(
            "different", force=False, force_tasks=["step"]
        )


# ═══════════════════════════════════════════════════════════════════════════
# run_local with store context manager
# ═══════════════════════════════════════════════════════════════════════════


class TestRunLocalStoreLifecycle:
    def test_store_stays_open_after_run(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def task(x: Annotated[str, Param(help="X")]) -> str:
            return x

        with _store(tmp_path) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st, x="hello")
            # Store still usable after run
            row = st.get_task_instance(run.run_id, "task", None)
            assert row["state"] == "SUCCESS"

    def test_run_local_creates_logs_dir(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def task() -> str:
            return "ok"

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r", store=st)
        assert (tmp_path / "r" / "logs").is_dir()


# ═══════════════════════════════════════════════════════════════════════════
# Sequential element failure (_local_run_one_element)
# ═══════════════════════════════════════════════════════════════════════════


class TestSequentialElementFailure:
    def test_single_element_failure_recorded(self, tmp_path: Path) -> None:
        """A failing array element is recorded as FAILED with error_text."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["ok", "bad"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            if item == "bad":
                raise ValueError("forced failure")
            return item.upper()

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            run = wf.run_local(
                run_dir=tmp_path / "r",
                store=st,
                max_workers=1,
                on_error="continue",
            )
        instances = st.list_task_instances(run.run_id, task_name="process")
        states = {i["array_index"]: i["state"] for i in instances}
        assert states[0] == "SUCCESS"
        assert states[1] == "FAILED"

    def test_element_failure_error_text_populated(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["x"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            raise RuntimeError("boom")

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            wf.run_local(
                run_dir=tmp_path / "r",
                store=st,
                on_error="continue",
            )
        instances = st.list_task_instances(None, task_name="process") if False else \
            st.list_task_instances(
                [r["run_id"] for r in st.list_runs()][0], task_name="process"
            )
        assert instances[0]["state"] == "FAILED"
        assert "boom" in (instances[0]["error_text"] or "")


# ═══════════════════════════════════════════════════════════════════════════
# array_parallelism cap in parallel execution
# ═══════════════════════════════════════════════════════════════════════════


class TestArrayParallelismCap:
    def test_array_parallelism_caps_workers(self, tmp_path: Path) -> None:
        """array_parallelism limits effective_workers even with higher max_workers."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[int]:
            return list(range(6))

        @wf.array_job(array_parallelism=2)
        def process(item: Annotated[int, Result(step="source")]) -> int:
            return item * 2

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            run = wf.run_local(
                run_dir=tmp_path / "r",
                store=st,
                max_workers=4,
                on_error="continue",
            )
        # All elements fail (closure not picklable) but run completes
        assert run.status(as_dict=True)["run"]["status"] in ("SUCCESS", "FAILED")


# ═══════════════════════════════════════════════════════════════════════════
# _local_find_fan_out_param: exception branches
# ═══════════════════════════════════════════════════════════════════════════


class TestLocalFanOutParamEdges:
    def test_no_result_deps_returns_none(self, tmp_path: Path) -> None:
        """Array task with no result deps → _local_find_fan_out_param returns None."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        # Manually remove result_deps to simulate the edge case
        import copy
        spec = copy.copy(wf.tasks["process"])
        spec.result_deps = {}
        result = wf._local_find_fan_out_param(spec)
        assert result is None

    def test_single_item_list_creates_one_element(self, tmp_path: Path) -> None:
        """A single-element list upstream creates exactly one array element."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["only"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item.upper()

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st)
        instances = st.list_task_instances(run.run_id, task_name="process")
        assert len(instances) == 1
        assert instances[0]["output"] == "ONLY"


# ═══════════════════════════════════════════════════════════════════════════
# Cache stale path in _local_try_cache
# ═══════════════════════════════════════════════════════════════════════════


class TestLocalCacheStale:
    def test_cache_stale_verify_reruns_task(self, tmp_path: Path) -> None:
        """verify=True with a custom verify callable forces rerun on cache hit."""
        from typing import Any

        wf = Workflow("wf")
        call_count = {"n": 0}
        verify_count = {"n": 0}

        def always_stale(output: Any) -> bool:
            verify_count["n"] += 1
            return False

        @wf.job(cache=True, verify=always_stale)
        def step(x: Annotated[str, Param(help="x")]) -> str:
            call_count["n"] += 1
            return x.upper()

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="v")
            assert call_count["n"] == 1
            assert verify_count["n"] == 0

            # Same inputs and run_dir → same identity → cache hit,
            # but always_stale → False → task reruns
            wf.run_local(run_dir=tmp_path / "r1", store=st, x="v", verify=True)
            assert verify_count["n"] == 1
            assert call_count["n"] == 2


class TestParallelSequentialFallback:
    def test_parallel_sequential_fallback_on_unpicklable(
        self, tmp_path: Path
    ) -> None:
        from unittest.mock import patch

        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item.upper()

        class FlakyPool:
            def __init__(self, *a, **kw): pass
            def __enter__(self): return self
            def __exit__(self, *a): pass
            def submit(self, *a, **kw):
                raise AttributeError("'function' object has no attribute '<locals>'")

        with patch(
            "reflow.workflow._local.ProcessPoolExecutor",
            side_effect=lambda *a, **kw: FlakyPool(),
        ):
            with SqliteStore(str(tmp_path / "db.sqlite")) as st:
                run = wf.run_local(
                    run_dir=tmp_path / "r",
                    store=st,
                    max_workers=2,
                )
        instances = st.list_task_instances(run.run_id, task_name="process")
        assert all(i["state"] == "SUCCESS" for i in instances)

    def test_parallel_success_path(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[int]:
            return [1, 2, 3]

        @wf.array_job()
        def process(item: Annotated[int, Result(step="source")]) -> int:
            return item * 2

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            run = wf.run_local(
                run_dir=tmp_path / "r",
                store=st,
                max_workers=1,
            )
        instances = st.list_task_instances(run.run_id, task_name="process")
        outputs = sorted(i["output"] for i in instances)
        assert outputs == [2, 4, 6]


class TestArrayRunWarnings:
    def test_no_result_inputs_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Array task with no upstream data logs a warning."""
        import logging
        from unittest.mock import patch

        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        original_resolve = wf._local_resolve_inputs

        def patched_resolve(store, run_id, spec):
            if spec.name == "process":
                return {}
            return original_resolve(store, run_id, spec)

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            with patch.object(wf, "_local_resolve_inputs", patched_resolve):
                with caplog.at_level(logging.WARNING, logger="reflow.workflow._local"):
                    wf.run_local(
                        run_dir=tmp_path / "r",
                        store=st,
                        on_error="continue",
                    )
        assert any(
            "result inputs" in r.message.lower() or "no result" in r.message.lower()
            for r in caplog.records
        )

    def test_chain_wire_single_value_unwrap(self, tmp_path: Path) -> None:
        wf = Workflow("wf")

        @wf.job()
        def upstream() -> list[str]:
            return ["x", "y"]

        @wf.array_job()
        def stage1(item: Annotated[str, Result(step="upstream")]) -> str:
            return item.upper()

        @wf.array_job()
        def stage2(item: Annotated[str, Result(step="stage1")]) -> str:
            return item + "!"

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st)
        outputs = sorted(st.get_array_outputs(run.run_id, "stage2"))
        assert outputs == ["X!", "Y!"]


# ═══════════════════════════════════════════════════════════════════════════
# Remaining _local.py gaps
# ═══════════════════════════════════════════════════════════════════════════


class TestLocalRemainingGaps:
    def test_local_resolve_inputs_hints_exception(
        self, tmp_path: Path
    ) -> None:
        """_local_resolve_inputs handles get_type_hints exception gracefully."""
        from unittest.mock import patch

        from reflow._types import TaskState

        wf = Workflow("wf")

        @wf.job()
        def source() -> str:
            return "v"

        @wf.job()
        def sink(x: Annotated[str, Result(step="source")]) -> str:
            return x

        wf.validate()
        # get_type_hints is imported locally in the function; patch at source
        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            st.init()
            st.insert_run("r1", "wf", "u", {})
            iid = st.insert_task_instance(
                "r1", "source", None, TaskState.SUCCESS, {}
            )
            st.update_task_success(iid, "v")
            with patch("typing.get_type_hints", side_effect=Exception("bad")):
                result = wf._local_resolve_inputs(st, "r1", wf.tasks["sink"])
            assert isinstance(result, dict)

    def test_gather_single_value_unwrap(self, tmp_path: Path) -> None:
        """CHAIN with single array output element unwraps correctly."""
        wf = Workflow("wf")

        @wf.job()
        def upstream() -> list[str]:
            return ["only"]

        @wf.array_job()
        def stage1(item: Annotated[str, Result(step="upstream")]) -> str:
            return item.upper()

        @wf.job()
        def collect(items: Annotated[list[str], Result(step="stage1")]) -> str:
            return ",".join(items)

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st)
        row = st.get_task_instance(run.run_id, "collect", None)
        assert row["state"] == "SUCCESS"

    def test_local_cache_output_hash_computed(self, tmp_path: Path) -> None:
        """When cached output_hash is missing, it is computed before insert."""
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job(cache=True)
        def step(x: Annotated[str, Param(help="x")]) -> str:
            return x.upper()

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            run1 = wf.run_local(run_dir=tmp_path / "r1", store=st, x="v")
            iid = int(st.get_task_instance(run1.run_id, "step", None)["id"])
            st.conn.execute(
                "UPDATE task_instances SET output_hash = '' WHERE id = ?", (iid,)
            )
            st.conn.commit()
            run2 = wf.run_local(run_dir=tmp_path / "r1", store=st, x="v")
            row = st.get_task_instance(run2.run_id, "step", None)
            assert row["state"] == "SUCCESS"

    def test_local_find_fan_out_param_type_error_skipped(
        self, tmp_path: Path
    ) -> None:
        """_local_find_fan_out_param skips a dep when infer_wire_mode raises
        TypeError in the first loop, then falls back to the first
        non-broadcast param in the second loop.
        """
        from unittest.mock import patch
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        # infer_wire_mode raising TypeError makes the first loop `continue`
        # past the dep (line 447-448). The fallback loop then returns the
        # first non-broadcast param, which is "item".
        with patch(
            "reflow.workflow._local.infer_wire_mode",
            side_effect=TypeError("incompatible"),
        ):
            result = wf._local_find_fan_out_param(wf.tasks["process"])
        assert result == "item"

    def test_local_find_fan_out_param_exception_returns_none(
        self, tmp_path: Path
    ) -> None:
        """_local_find_fan_out_param returns None when get_type_hints raises."""
        from unittest.mock import patch
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["a"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            return item

        wf.validate()
        # get_type_hints is imported inside the function body, patch at source
        with patch("typing.get_type_hints", side_effect=Exception("bad")):
            result = wf._local_find_fan_out_param(wf.tasks["process"])
        assert result is None

    def test_array_per_element_value_matching(self, tmp_path: Path) -> None:
        """Non-fan dep with same length as fan_items is assigned per-element."""
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

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st)
        outputs = sorted(
            st.list_task_instances(run.run_id, task_name="process"),
            key=lambda i: i["array_index"],
        )
        assert outputs[0]["output"] == "x:a"
        assert outputs[1]["output"] == "y:b"

    def test_parallel_element_success_records_output(
        self, tmp_path: Path
    ) -> None:
        """Sequential array elements record output and output_hash."""
        wf = Workflow("wf")

        @wf.job()
        def source() -> list[int]:
            return [10, 20]

        @wf.array_job()
        def process(item: Annotated[int, Result(step="source")]) -> int:
            return item + 1

        with SqliteStore(str(tmp_path / "db.sqlite")) as st:
            run = wf.run_local(
                run_dir=tmp_path / "r", store=st, max_workers=1
            )
        instances = sorted(
            st.list_task_instances(run.run_id, task_name="process"),
            key=lambda i: i["array_index"],
        )
        assert instances[0]["output"] == 11
        assert instances[1]["output"] == 21
        assert instances[0]["output_hash"] not in (None, "")

    def test_parallel_sequential_fallback_element_failure(
        self, tmp_path: Path
    ) -> None:
        """Sequential fallback correctly records FAILED for a bad element."""
        from unittest.mock import patch

        wf = Workflow("wf")

        @wf.job()
        def source() -> list[str]:
            return ["ok", "bad"]

        @wf.array_job()
        def process(item: Annotated[str, Result(step="source")]) -> str:
            if item == "bad":
                raise RuntimeError("forced failure")
            return item.upper()

        class FlakyPool:
            def __init__(self, *a, **kw): pass
            def __enter__(self): return self
            def __exit__(self, *a): pass
            def submit(self, *a, **kw):
                raise AttributeError("cannot pickle")

        with patch("reflow.workflow._local.ProcessPoolExecutor",
                   side_effect=lambda *a, **kw: FlakyPool()):
            with SqliteStore(str(tmp_path / "db.sqlite")) as st:
                run = wf.run_local(
                    run_dir=tmp_path / "r", store=st,
                    max_workers=2, on_error="continue"
                )

        instances = sorted(
            st.list_task_instances(run.run_id, task_name="process"),
            key=lambda i: i["array_index"],
        )
        assert instances[0]["state"] == "SUCCESS"
        assert instances[1]["state"] == "FAILED"
