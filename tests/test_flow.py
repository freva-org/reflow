"""test_flow.py - refactored reflow tests."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import pytest

from reflow import (
    Config,
    Flow,
    Result,
    Run,
    RunDir,
    Workflow,
)
from reflow.flow import JobConfig
from reflow.workflow._core import _suggest_name

# ═══════════════════════════════════════════════════════════════════════════
# _types.py
# ═══════════════════════════════════════════════════════════════════════════


class TestFlow:
    def test_registration(self) -> None:
        f = Flow("test")

        @f.job()
        def a(run_dir: RunDir) -> list[str]:
            return []

        @f.array_job()
        def b(item: Annotated[str, Result(step="a")]) -> str:
            return item

        assert set(f.tasks) == {"a", "b"}

    def test_prefix(self) -> None:
        f = Flow("test")

        @f.job()
        def prepare(run_dir: RunDir) -> list[str]:
            return []

        @f.array_job()
        def convert(item: Annotated[str, Result(step="prepare")]) -> str:
            return item

        tasks, order = f._prefixed_tasks("era5")
        assert "era5_prepare" in tasks
        conv = tasks["era5_convert"]
        for dep in conv.result_deps.values():
            assert dep.steps == ["era5_prepare"]


class TestFlowExtended:
    def test_duplicate_registration_raises(self) -> None:
        f = Flow("test")

        @f.job()
        def task_a() -> str:
            return "ok"

        with pytest.raises(ValueError, match="already registered"):

            @f.job(name="task_a")
            def task_a_dup() -> str:
                return "dup"

    def test_array_job_without_result_raises(self) -> None:
        f = Flow("test")
        with pytest.raises(ValueError, match="must have at least one"):

            @f.array_job()
            def bad_array(x: str) -> str:
                return x

    def test_custom_name(self) -> None:
        f = Flow("test")

        @f.job(name="custom_name")
        def my_func() -> str:
            return "ok"

        assert "custom_name" in f.tasks
        assert "my_func" not in f.tasks

    def test_prefix_none_returns_copies(self) -> None:
        f = Flow("test")

        @f.job()
        def a() -> str:
            return "ok"

        tasks, order = f._prefixed_tasks(None)
        assert "a" in tasks
        assert tasks["a"] is not f.tasks["a"]  # deep copy

    def test_job_config_properties(self) -> None:
        cfg = JobConfig(
            submit_options={
                "partition": "gpu",
                "account": "acc",
                "mail_user": "u@e",
                "mail_type": "FAIL",
                "signal": "B:INT@60",
            }
        )
        assert cfg.partition == "gpu"
        assert cfg.account == "acc"
        assert cfg.mail_user == "u@e"
        assert cfg.mail_type == "FAIL"
        assert cfg.signal == "B:INT@60"
        assert cfg.extra is cfg.submit_options


class TestSuggestName:
    def test_close_match(self) -> None:
        hint = _suggest_name("preprae", {"prepare", "compute", "finalize"})
        assert "prepare" in hint

    def test_no_match(self) -> None:
        hint = _suggest_name("zzzzzzz", {"prepare", "compute"})
        assert hint == ""

    def test_exact_match_not_suggested(self) -> None:
        # If user typo is very close to two names, still returns one
        hint = _suggest_name("comput", {"compute", "compile"})
        assert hint != ""

    def test_works_with_dict(self) -> None:
        hint = _suggest_name("preprae", {"prepare": 1, "compute": 2})
        assert "prepare" in hint


class TestDidYouMeanValidation:
    def test_result_step_typo(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def prepare() -> list[str]:
            return []

        @wf.array_job()
        def compute(item: Annotated[str, Result(step="preprae")]) -> str:
            return item

        with pytest.raises(ValueError, match="Did you mean 'prepare'"):
            wf.validate()

    def test_after_typo(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def step_a() -> str:
            return "ok"

        @wf.job(after=["step_aa"])
        def step_b() -> str:
            return "ok"

        with pytest.raises(ValueError, match="Did you mean 'step_a'"):
            wf.validate()

    def test_no_suggestion_for_unrelated(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def alpha() -> list[str]:
            return []

        @wf.array_job()
        def beta(v: Annotated[str, Result(step="zzzzzzzzz")]) -> str:
            return v

        with pytest.raises(ValueError, match="unknown task") as exc_info:
            wf.validate()
        assert "Did you mean" not in str(exc_info.value)

    def test_topological_order_typo_in_after(self) -> None:
        """_topological_order also gives suggestions."""
        wf = Workflow("w")

        @wf.job()
        def first() -> str:
            return "ok"

        @wf.job(after=["firs"])
        def second() -> str:
            return "ok"

        with pytest.raises(ValueError, match="Did you mean"):
            wf.validate()
