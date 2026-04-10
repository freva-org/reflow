"""test_local_runner.py - refactored reflow tests."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import pytest

from reflow import (
    Param,
    Result,
    Run,
    Workflow,
)
from reflow.stores.sqlite import SqliteStore

# ═══════════════════════════════════════════════════════════════════════════
# Feature 2: Broadcast mode
# ═══════════════════════════════════════════════════════════════════════════


class TestRunLocalBasic:
    def test_simple_singleton(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def greet(name: Annotated[str, Param(help="N")]) -> str:
            return f"Hello {name}"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", name="World", store=st)
        info = run.status(as_dict=True)
        assert info is not None
        assert info["run"]["status"] == "SUCCESS"
        assert st.get_singleton_output(run.run_id, "greet") == "Hello World"

    def test_fan_out_pipeline(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def gen(prefix: Annotated[str, Param(help="P")]) -> list[str]:
            return [f"{prefix}_1", f"{prefix}_2", f"{prefix}_3"]

        @wf.array_job()
        def upper(item: Annotated[str, Result(step="gen")]) -> str:
            return item.upper()

        @wf.job()
        def join(results: Annotated[list[str], Result(step="upper")]) -> str:
            return ",".join(sorted(results))

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", prefix="x", store=st)
        assert run.status(as_dict=True)["run"]["status"] == "SUCCESS"
        assert st.get_singleton_output(run.run_id, "join") == "X_1,X_2,X_3"

    def test_broadcast_in_local(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def items() -> list[str]:
            return ["a", "b"]

        @wf.job()
        def config() -> dict:
            return {"scale": 10}

        @wf.array_job()
        def process(
            item: Annotated[str, Result(step="items")],
            cfg: Annotated[dict, Result(step="config", broadcast=True)],
        ) -> str:
            return f"{item}:{cfg['scale']}"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", store=st)
        outs = st.get_array_outputs(run.run_id, "process")
        assert outs == ["a:10", "b:10"]


class TestRunLocalErrorHandling:
    def test_on_error_stop(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def boom() -> str:
            raise ValueError("intentional")

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        with pytest.raises(RuntimeError, match="boom.*failed"):
            wf.run_local(run_dir=tmp_path / "r", store=st, on_error="stop")

    def test_on_error_continue(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def good() -> str:
            return "ok"

        @wf.job()
        def bad() -> str:
            raise ValueError("boom")

        @wf.job()
        def after_bad(x: Annotated[str, Result(step="bad")]) -> str:
            return x

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(
            run_dir=tmp_path / "r",
            store=st,
            on_error="continue",
        )
        info = run.status(as_dict=True)
        assert info["run"]["status"] == "FAILED"
        # 'good' should have succeeded
        assert st.get_singleton_output(run.run_id, "good") == "ok"

    def test_downstream_skipped_on_failure(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def fail_step() -> list[str]:
            raise RuntimeError("boom")

        @wf.array_job()
        def child(item: Annotated[str, Result(step="fail_step")]) -> str:
            return item

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(
            run_dir=tmp_path / "r",
            store=st,
            on_error="continue",
        )
        assert run.status(as_dict=True)["run"]["status"] == "FAILED"
        # child should have 0 instances (skipped entirely)
        assert st.count_task_instances(run.run_id, "child") == 0


class TestRunLocalCaching:
    def test_cache_hit_on_second_run(self, tmp_path: Path) -> None:
        counter = {"n": 0}
        wf = Workflow("w")

        @wf.job()
        def counted(val: Annotated[str, Param(help="V")]) -> str:
            counter["n"] += 1
            return val.upper()

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        wf.run_local(run_dir=tmp_path / "r1", val="hi", store=st)
        assert counter["n"] == 1

        wf.run_local(run_dir=tmp_path / "r2", val="hi", store=st)
        assert counter["n"] == 1  # cache hit, not re-executed

    def test_force_bypasses_cache(self, tmp_path: Path) -> None:
        counter = {"n": 0}
        wf = Workflow("w")

        @wf.job()
        def counted(val: Annotated[str, Param(help="V")]) -> str:
            counter["n"] += 1
            return val.upper()

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        wf.run_local(run_dir=tmp_path / "r1", val="hi", store=st)
        assert counter["n"] == 1

        wf.run_local(run_dir=tmp_path / "r2", val="hi", store=st, force=True)
        assert counter["n"] == 2

    def test_force_tasks_selective(self, tmp_path: Path) -> None:
        counters = {"a": 0, "b": 0}
        wf = Workflow("w")

        @wf.job()
        def task_a(val: Annotated[str, Param(help="V")]) -> str:
            counters["a"] += 1
            return val

        @wf.job()
        def task_b(x: Annotated[str, Result(step="task_a")]) -> str:
            counters["b"] += 1
            return x.upper()

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        wf.run_local(run_dir=tmp_path / "r1", val="hi", store=st)
        assert counters == {"a": 1, "b": 1}

        wf.run_local(
            run_dir=tmp_path / "r2",
            val="hi",
            store=st,
            force_tasks=["task_b"],
        )
        # task_a should be cached, task_b forced
        assert counters["a"] == 1
        assert counters["b"] == 2

    def test_cache_disabled_per_task(self, tmp_path: Path) -> None:
        counter = {"n": 0}
        wf = Workflow("w")

        @wf.job(cache=False)
        def nocache(val: Annotated[str, Param(help="V")]) -> str:
            counter["n"] += 1
            return val

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        wf.run_local(run_dir=tmp_path / "r1", val="hi", store=st)
        wf.run_local(run_dir=tmp_path / "r2", val="hi", store=st)
        assert counter["n"] == 2  # never cached


class TestRunLocalParallel:
    def test_parallel_with_unpicklable_marks_failed(self, tmp_path: Path) -> None:
        """max_workers>1 with unpicklable test functions fails elements."""
        wf = Workflow("w")

        @wf.job()
        def gen() -> list[int]:
            return [1, 2]

        @wf.array_job()
        def double(item: Annotated[int, Result(step="gen")]) -> int:
            return item * 2

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        # Functions defined inside tests aren't picklable for
        # ProcessPoolExecutor.  The parallel path catches element
        # failures individually.
        run = wf.run_local(
            run_dir=tmp_path / "r",
            store=st,
            max_workers=2,
            on_error="continue",
        )
        assert run.status(as_dict=True)["run"]["status"] == "FAILED"

    def test_sequential_array(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def gen() -> list[int]:
            return [10, 20, 30]

        @wf.array_job()
        def inc(item: Annotated[int, Result(step="gen")]) -> int:
            return item + 1

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", store=st, max_workers=1)
        outs = st.get_array_outputs(run.run_id, "inc")
        assert outs == [11, 21, 31]

    def test_array_parallelism_honored(self, tmp_path: Path) -> None:
        """array_parallelism caps effective workers."""
        wf = Workflow("w")

        @wf.job()
        def gen() -> list[str]:
            return ["a", "b", "c"]

        @wf.array_job(array_parallelism=1)
        def proc(item: Annotated[str, Result(step="gen")]) -> str:
            return item.upper()

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", store=st, max_workers=1)
        outs = st.get_array_outputs(run.run_id, "proc")
        assert outs == ["A", "B", "C"]


class TestRunLocalMissing:
    def test_missing_required_param(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def task(x: Annotated[str, Param(help="X")]) -> str:
            return x

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        with pytest.raises(TypeError, match="missing required"):
            wf.run_local(run_dir=tmp_path / "r", store=st)
