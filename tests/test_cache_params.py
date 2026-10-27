"""Run parameters are part of a task's cache identity.

Regression tests: before, only ``Result`` inputs and upstream hashes were
hashed, so a task whose inputs are ``Param``s returned the output of its
first run for every later run, whatever the parameter values.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from reflow import Config, Param, Result, RunDir, Workflow
from reflow.cache import compute_input_hash, compute_output_hash
from reflow.stores.sqlite import SqliteStore
from reflow.workflow._helpers import parameter_inputs


def _store(tmp_path: Path) -> SqliteStore:
    st = SqliteStore(str(tmp_path / "db.sqlite"))
    st.init()
    return st


def _wf() -> Workflow:
    return Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))


class TestLocalRunner:
    def test_changed_param_reruns_singleton(self, tmp_path: Path) -> None:
        wf = _wf()
        calls: list[str] = []

        @wf.job()
        def echo(value: Annotated[str, Param(help="v")]) -> str:
            calls.append(value)
            return value

        with _store(tmp_path) as st:
            outputs = []
            for i, value in enumerate(["first", "second", "first"]):
                run = wf.run_local(run_dir=tmp_path / f"r{i}", store=st, value=value)
                outputs.append(st.get_singleton_output(run.run_id, "echo"))
        assert outputs == ["first", "second", "first"]
        assert calls == ["first", "second"], "the third run is a genuine cache hit"

    def test_changed_param_reruns_array_elements(self, tmp_path: Path) -> None:
        wf = _wf()
        calls: list[str] = []

        @wf.job()
        def items() -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def tag(
            item: Annotated[str, Result(step="items")],
            suffix: Annotated[str, Param(help="s")],
        ) -> str:
            calls.append(item + suffix)
            return item + suffix

        with _store(tmp_path) as st:
            run1 = wf.run_local(run_dir=tmp_path / "r1", store=st, suffix="1")
            run2 = wf.run_local(run_dir=tmp_path / "r2", store=st, suffix="2")
            assert st.get_array_outputs(run1.run_id, "tag") == ["a1", "b1"]
            assert st.get_array_outputs(run2.run_id, "tag") == ["a2", "b2"]
        assert calls == ["a1", "b1", "a2", "b2"]

    def test_params_a_task_does_not_take_keep_the_cache(self, tmp_path: Path) -> None:
        """Only the parameters in a task's signature belong to its identity."""
        wf = _wf()
        calls = {"stable": 0, "volatile": 0}

        @wf.job()
        def stable(base: Annotated[str, Param(help="b")]) -> str:
            calls["stable"] += 1
            return base

        @wf.job()
        def volatile(
            upstream: Annotated[str, Result(step="stable")],
            label: Annotated[str, Param(help="l")],
        ) -> str:
            calls["volatile"] += 1
            return upstream + label

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st, base="x", label="1")
            wf.run_local(run_dir=tmp_path / "r2", store=st, base="x", label="2")
        assert calls == {"stable": 1, "volatile": 2}

    def test_run_dir_is_not_part_of_the_identity(self, tmp_path: Path) -> None:
        wf = _wf()
        calls = {"n": 0}

        @wf.job()
        def step(run_dir: RunDir, x: Annotated[int, Param(help="x")]) -> int:
            calls["n"] += 1
            return x

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "r1", store=st, x=1)
            wf.run_local(run_dir=tmp_path / "r2", store=st, x=1)
        assert calls["n"] == 1


class TestDispatch:
    def test_dispatch_respects_param_values(self, tmp_path: Path) -> None:
        """The scheduler path uses the same identity as the local runner.

        Root tasks are pre-created by ``submit_run`` and always run on the
        scheduler path, so the cached task here sits below a root.
        """
        wf = _wf()

        @wf.job()
        def src() -> int:
            return 1

        @wf.job()
        def echo(
            up: Annotated[int, Result(step="src")],
            value: Annotated[str, Param(help="v")],
        ) -> str:
            return f"{value}{up}"

        def submit(value: str) -> str:
            run_id = wf.submit_run(
                run_dir=tmp_path / value, store=st, parameters={"value": value}
            )
            row = st.get_task_instance(run_id, "src", None)
            st.update_task_success(
                int(row["id"]), 1, output_hash=compute_output_hash(1)
            )
            wf.dispatch(run_id, st, tmp_path / value)
            return run_id

        with _store(tmp_path) as st:
            wf.run_local(run_dir=tmp_path / "local", store=st, value="first")

            other = submit("second")
            row = st.get_task_instance(other, "echo", None)
            assert row["state"] != "SUCCESS", "a different value must not hit the cache"

            same = submit("first")
            assert st.get_singleton_output(same, "echo") == "first1"


class TestHashing:
    def test_tasks_without_params_keep_their_identity(self) -> None:
        """Existing caches of parameter-less tasks stay valid after upgrading."""
        legacy = compute_input_hash("t", "1", {"x": 1})
        assert compute_input_hash("t", "1", {"x": 1}, {}) == legacy
        assert compute_input_hash("t", "1", {"x": 1}, None) == legacy
        assert compute_input_hash("t", "1", {"x": 1}, {"p": 2}) != legacy

    def test_parameter_inputs_excludes_results_and_run_dir(self) -> None:
        wf = _wf()

        @wf.job()
        def src() -> int:
            return 1

        @wf.job()
        def step(
            run_dir: RunDir,
            up: Annotated[int, Result(step="src")],
            a: Annotated[int, Param(help="a")],
            b: Annotated[int, Param(help="b")] = 3,
        ) -> int:
            return up + a + b

        run_params = {"run_dir": "/x", "a": 1, "b": 2, "unrelated": 9}
        params = parameter_inputs(wf.tasks["step"], run_params, {"up": 5})
        assert params == {"a": 1, "b": 2}

    def test_task_local_params_override_globals(self) -> None:
        wf = _wf()

        @wf.job()
        def step(a: Annotated[int, Param(help="a")]) -> int:
            return a

        run_params = {"a": 1, "__task_params__": {"step": {"a": 7}}}
        assert parameter_inputs(wf.tasks["step"], run_params, {}) == {"a": 7}
