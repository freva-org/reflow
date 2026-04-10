"""test_params.py - refactored reflow tests."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Annotated, Any, Literal

import pytest

from reflow import (
    Config,
    Flow,
    Param,
    Result,
    Run,
    RunDir,
    Workflow,
)
from reflow.flow import JobConfig, TaskSpec
from reflow.params import (
    ResolvedParam,
    WireMode,
    argparse_type_callable,
    check_type_compatibility,
    collect_cli_params,
    collect_result_deps,
    get_return_type,
    infer_wire_mode,
    merge_resolved_params,
)
from reflow.workflow import build_kwargs

# ═══════════════════════════════════════════════════════════════════════════
# _types.py
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# Feature 2: Broadcast mode
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# Coverage: _dispatch.py  (dispatch loop, resolve, fan-out, finalize)
# ═══════════════════════════════════════════════════════════════════════════


class TestCollectCliParams:
    def test_skips_result_and_rundir(self) -> None:
        def fn(
            item: Annotated[str, Result(step="p")],
            wd: RunDir,
            bucket: Annotated[str, Param(help="B")],
        ) -> None:
            pass

        params = collect_cli_params("t", fn, inspect.signature(fn))
        names = [p.name for p in params]
        assert "bucket" in names
        assert "item" not in names
        assert "wd" not in names

    def test_literal_choices(self) -> None:
        def fn(
            model: Annotated[Literal["era5", "icon"], Param(help="M")] = "era5",
        ) -> None:
            pass

        params = collect_cli_params("t", fn, inspect.signature(fn))
        assert params[0].literal_choices == ("era5", "icon")

    def test_local_namespace(self) -> None:
        def fn(
            chunk: Annotated[int, Param(namespace="local")] = 256,
        ) -> None:
            pass

        params = collect_cli_params("convert", fn, inspect.signature(fn))
        assert params[0].cli_flag() == "--convert-chunk"


class TestBuildKwargs:
    @staticmethod
    def _spec(func):
        return TaskSpec(
            name="t",
            func=func,
            config=JobConfig(),
            signature=inspect.signature(func),
            result_deps=collect_result_deps(func),
            return_type=get_return_type(func),
        )

    def test_run_dir_injection(self) -> None:
        def fn(wd: RunDir) -> None:
            pass

        kw = build_kwargs(self._spec(fn), {"run_dir": "/scratch"}, {})
        assert kw["wd"] == Path("/scratch")

    def test_result_from_input(self) -> None:
        def fn(item: Annotated[str, Result(step="p")]) -> str:
            return item

        kw = build_kwargs(self._spec(fn), {}, {"item": "a.nc"})
        assert kw["item"] == "a.nc"

    def test_global_param(self) -> None:
        def fn(bucket: str) -> None:
            pass

        kw = build_kwargs(self._spec(fn), {"bucket": "b"}, {})
        assert kw["bucket"] == "b"


class TestBuildKwargsExtended:
    @staticmethod
    def _spec(func: Any) -> TaskSpec:
        return TaskSpec(
            name="t",
            func=func,
            config=JobConfig(),
            signature=inspect.signature(func),
            result_deps=collect_result_deps(func),
            return_type=get_return_type(func),
        )

    def test_task_local_params(self) -> None:
        def fn(chunk: int = 256) -> None:
            pass

        kw = build_kwargs(
            self._spec(fn),
            {"__task_params__": {"t": {"chunk": 512}}},
            {},
        )
        assert kw["chunk"] == 512

    def test_run_dir_by_name(self) -> None:
        """Parameter named 'run_dir' without RunDir annotation."""

        def fn(run_dir: str) -> None:
            pass

        kw = build_kwargs(self._spec(fn), {"run_dir": "/scratch"}, {})
        assert kw["run_dir"] == Path("/scratch")


class TestWireMode:
    def test_all_modes(self) -> None:
        assert infer_wire_mode(list[str], False, str, True) == WireMode.FAN_OUT
        assert infer_wire_mode(str, True, list[str], False) == WireMode.GATHER
        assert infer_wire_mode(str, True, str, True) == WireMode.CHAIN
        assert infer_wire_mode(list[str], True, str, True) == WireMode.CHAIN_FLATTEN
        assert (
            infer_wire_mode(list[str], True, list[str], False)
            == WireMode.GATHER_FLATTEN
        )
        assert infer_wire_mode(str, False, str, False) == WireMode.DIRECT

    def test_fan_out_error(self) -> None:
        with pytest.raises(TypeError, match="fan-out"):
            infer_wire_mode(str, False, str, True)

    def test_gather_error(self) -> None:
        with pytest.raises(TypeError, match="gather"):
            infer_wire_mode(str, True, str, False)


class TestWireModeBroadcast:
    def test_infer_broadcast_from_types(self) -> None:
        """list[str] -> list[str] with array downstream = BROADCAST."""
        mode = infer_wire_mode(list[str], False, list[str], True)
        assert mode == WireMode.BROADCAST

    def test_infer_fan_out_from_types(self) -> None:
        """list[str] -> str with array downstream = FAN_OUT (unchanged)."""
        mode = infer_wire_mode(list[str], False, str, True)
        assert mode == WireMode.FAN_OUT

    def test_explicit_broadcast_on_non_list(self) -> None:
        """Dict -> dict with broadcast=True = BROADCAST."""
        mode = infer_wire_mode(dict, False, dict, True, broadcast=True)
        assert mode == WireMode.BROADCAST

    def test_explicit_broadcast_array_to_array(self) -> None:
        """Array-to-array with broadcast=True = BROADCAST."""
        mode = infer_wire_mode(str, True, str, True, broadcast=True)
        assert mode == WireMode.BROADCAST

    def test_non_list_without_broadcast_raises(self) -> None:
        """Dict -> dict on array without broadcast raises helpful error."""
        with pytest.raises(TypeError, match="broadcast=True"):
            infer_wire_mode(dict, False, dict, True, broadcast=False)

    def test_check_type_compat_with_broadcast(self) -> None:
        mode = check_type_compatibility(
            dict,
            False,
            dict,
            True,
            "up",
            "down",
            "p",
            broadcast=True,
        )
        assert mode == WireMode.BROADCAST


class TestResultBroadcast:
    def test_broadcast_default_false(self) -> None:
        r = Result(step="foo")
        assert r.broadcast is False

    def test_broadcast_true(self) -> None:
        r = Result(step="foo", broadcast=True)
        assert r.broadcast is True

    def test_repr_without_broadcast(self) -> None:
        r = Result(step="foo")
        assert "broadcast" not in repr(r)

    def test_repr_with_broadcast(self) -> None:
        r = Result(step="foo", broadcast=True)
        assert "broadcast=True" in repr(r)

    def test_repr_multi_with_broadcast(self) -> None:
        r = Result(steps=["a", "b"], broadcast=True)
        assert "broadcast=True" in repr(r)
        assert "steps=" in repr(r)


class TestBroadcastValidation:
    def test_validate_with_broadcast(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def gen_items() -> list[str]:
            return ["a", "b"]

        @wf.job()
        def gen_config() -> dict:
            return {"key": "val"}

        @wf.array_job()
        def process(
            item: Annotated[str, Result(step="gen_items")],
            cfg: Annotated[dict, Result(step="gen_config", broadcast=True)],
        ) -> str:
            return item

        # Should not raise
        wf.validate()

    def test_broadcast_preserved_in_flow_prefix(self) -> None:
        f = Flow("f")

        @f.job()
        def cfg() -> dict:
            return {}

        @f.job()
        def items() -> list[str]:
            return []

        @f.array_job()
        def proc(
            item: Annotated[str, Result(step="items")],
            settings: Annotated[dict, Result(step="cfg", broadcast=True)],
        ) -> str:
            return item

        wf = Workflow("w")
        wf.include(f, prefix="pre")

        spec = wf.tasks["pre_proc"]
        assert spec.result_deps["settings"].broadcast is True
        assert spec.result_deps["item"].broadcast is False
        wf.validate()


class TestParamsBroadcastEdgeCases:
    def test_broadcast_direct_singleton_to_singleton(self) -> None:
        """broadcast=True on a singleton->singleton still returns DIRECT."""
        mode = infer_wire_mode(str, False, str, False, broadcast=False)
        assert mode == WireMode.DIRECT

    def test_gather_not_affected_by_broadcast(self) -> None:
        """Gather mode (array->singleton list) unaffected by broadcast flag."""
        mode = infer_wire_mode(str, True, list[str], False, broadcast=False)
        assert mode == WireMode.GATHER

    def test_chain_not_affected_by_broadcast(self) -> None:
        mode = infer_wire_mode(str, True, str, True, broadcast=False)
        assert mode == WireMode.CHAIN

    def test_chain_flatten_not_affected(self) -> None:
        mode = infer_wire_mode(list[str], True, str, True, broadcast=False)
        assert mode == WireMode.CHAIN_FLATTEN


class TestParamsExtended:
    def test_result_repr_single(self) -> None:
        r = Result(step="foo")
        assert "step=" in repr(r) and "foo" in repr(r)

    def test_result_repr_multi(self) -> None:
        r = Result(steps=["a", "b"])
        assert "steps=" in repr(r)

    def test_param_repr(self) -> None:
        p = Param(help="test", short="-t", namespace="local")
        r = repr(p)
        assert "help=" in r
        assert "short=" in r
        assert "namespace=" in r

    def test_argparse_type_callable_defaults(self) -> None:
        assert argparse_type_callable(str) is str
        assert argparse_type_callable(int) is int
        assert argparse_type_callable(float) is float
        assert argparse_type_callable(Path) is Path

    def test_argparse_type_callable_unknown(self) -> None:
        assert argparse_type_callable(object) is str

    def test_check_type_compatibility_error_message(self) -> None:
        with pytest.raises(TypeError, match="wiring"):
            check_type_compatibility(
                str,
                False,
                str,
                True,
                "up",
                "down",
                "param",
            )

    def test_collect_result_deps(self) -> None:
        def fn(
            item: Annotated[str, Result(step="p")],
            other: str,
        ) -> str:
            return item

        deps = collect_result_deps(fn)
        assert "item" in deps
        assert "other" not in deps

    def test_get_return_type(self) -> None:
        def fn() -> list[str]:
            return []

        assert get_return_type(fn) == list[str]

    def test_merge_resolved_params_dedup(self) -> None:
        p1 = ResolvedParam("x", "t1", str, False, True, None, Param(help="h"))
        p2 = ResolvedParam("x", "t2", str, False, False, "default", Param(help="h"))
        merged = merge_resolved_params([p1, p2])
        globals_ = [p for p in merged if p.namespace == "global"]
        assert len(globals_) == 1
        # required stays True since at least one task requires it
        assert globals_[0].required is True

    def test_resolved_param_local_flag(self) -> None:
        rp = ResolvedParam(
            "chunk_size",
            "convert",
            int,
            False,
            False,
            256,
            Param(namespace="local"),
        )
        assert rp.cli_flag() == "--convert-chunk-size"
        assert rp.dest_name() == "convert_chunk_size"
