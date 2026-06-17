"""test_dag_render.py - unit tests for the DAG rendering module.

These drive reflow._dag_render directly (rather than through the CLI) so the
render() dispatcher, the format guard, and each renderer are exercised in
isolation. The CLI-level behaviour (--format flag, phart fallback to text,
stderr hint) is covered separately in test_cli.py.
"""

from __future__ import annotations

from typing import Annotated

import pytest

from reflow import Result, Workflow
from reflow import _dag_render


def _make_wf() -> Workflow:
    """Workflow with an array task and a multi-parent node."""
    wf = Workflow("dagrender")

    @wf.job()
    def gather_sources() -> list[str]:
        return ["a", "b"]

    @wf.job()
    def prepare_shared(
        s: Annotated[list[str], Result(step="gather_sources")],
    ) -> str:
        return "shared"

    @wf.array_job()
    def download_source(
        item: Annotated[str, Result(step="gather_sources")],
    ) -> str:
        return item

    @wf.array_job()
    def convert_source(
        d: Annotated[str, Result(step="download_source")],
        p: Annotated[str, Result(step="prepare_shared", broadcast=True)],
    ) -> str:
        return d

    return wf


# ═══════════════════════════════════════════════════════════════════════════
# render() dispatcher
# ═══════════════════════════════════════════════════════════════════════════


class TestRenderDispatcher:
    def test_render_text(self) -> None:
        out = _dag_render.render(_make_wf(), "text")
        assert "gather_sources" in out
        assert "download_source [array]" in out

    def test_render_mermaid(self) -> None:
        out = _dag_render.render(_make_wf(), "mermaid")
        assert out.startswith("flowchart TD")

    def test_render_dot(self) -> None:
        out = _dag_render.render(_make_wf(), "dot")
        assert out.startswith("digraph reflow {")

    def test_render_phart_via_dispatcher(self) -> None:
        """The phart branch of render() (line 132-133) is reached."""
        pytest.importorskip("phart")
        pytest.importorskip("networkx")
        out = _dag_render.render(_make_wf(), "phart")
        assert "gather_sources" in out
        assert "download_source [array]" in out

    def test_render_phart_via_dispatcher_ascii(self) -> None:
        pytest.importorskip("phart")
        pytest.importorskip("networkx")
        out = _dag_render.render(_make_wf(), "phart", use_ascii=True)
        assert "\u2193" not in out  # no Unicode arrow
        assert "gather_sources" in out

    def test_render_unknown_format_raises(self) -> None:
        """The format guard (line 134) raises ValueError."""
        with pytest.raises(ValueError, match="Unknown DAG format"):
            _dag_render.render(_make_wf(), "nonsense")

    def test_formats_constant(self) -> None:
        assert _dag_render.FORMATS == ("text", "mermaid", "dot", "phart")


# ═══════════════════════════════════════════════════════════════════════════
# Individual renderers
# ═══════════════════════════════════════════════════════════════════════════


class TestRenderText:
    def test_dependencies_arrow(self) -> None:
        out = _dag_render.render_text(_make_wf())
        assert (
            "convert_source [array]  <- download_source, prepare_shared" in out
        )

    def test_root_has_no_arrow(self) -> None:
        out = _dag_render.render_text(_make_wf())
        lines = out.splitlines()
        root = next(l for l in lines if "gather_sources" in l)
        assert "<-" not in root


class TestRenderMermaid:
    def test_array_subroutine_shape(self) -> None:
        out = _dag_render.render_mermaid(_make_wf())
        assert "download_source[[download_source]]" in out
        assert "convert_source[[convert_source]]" in out

    def test_singleton_box_shape(self) -> None:
        out = _dag_render.render_mermaid(_make_wf())
        assert "gather_sources[gather_sources]" in out

    def test_edges_present(self) -> None:
        out = _dag_render.render_mermaid(_make_wf())
        assert "gather_sources --> download_source" in out
        assert "prepare_shared --> convert_source" in out

    def test_isolated_node_still_declared(self) -> None:
        """A task with no edges still appears as a declared node."""
        wf = Workflow("solo")

        @wf.job()
        def lonely() -> str:
            return "x"

        out = _dag_render.render_mermaid(wf)
        assert "lonely[lonely]" in out


class TestRenderDot:
    def test_array_doubled_border(self) -> None:
        out = _dag_render.render_dot(_make_wf())
        assert '"download_source" [peripheries=2];' in out
        assert '"convert_source" [peripheries=2];' in out

    def test_singleton_no_peripheries(self) -> None:
        out = _dag_render.render_dot(_make_wf())
        assert '"gather_sources";' in out

    def test_edges_quoted(self) -> None:
        out = _dag_render.render_dot(_make_wf())
        assert '"gather_sources" -> "download_source";' in out

    def test_well_formed(self) -> None:
        out = _dag_render.render_dot(_make_wf())
        assert out.startswith("digraph reflow {")
        assert out.rstrip().endswith("}")
        assert "rankdir=TB;" in out


class TestRenderPhart:
    def test_renders_all_nodes(self) -> None:
        pytest.importorskip("phart")
        pytest.importorskip("networkx")
        out = _dag_render.render_phart(_make_wf())
        for name in (
            "gather_sources",
            "prepare_shared",
            "download_source",
            "convert_source",
        ):
            assert name in out

    def test_array_and_singleton_labels(self) -> None:
        pytest.importorskip("phart")
        pytest.importorskip("networkx")
        out = _dag_render.render_phart(_make_wf())
        # Array tasks carry the [array] suffix; singletons do not.
        assert "download_source [array]" in out
        assert "convert_source [array]" in out
        # A singleton name appears without the suffix
        assert "gather_sources" in out
        assert "gather_sources [array]" not in out

    def test_ascii_mode_no_unicode(self) -> None:
        pytest.importorskip("phart")
        pytest.importorskip("networkx")
        out = _dag_render.render_phart(_make_wf(), use_ascii=True)
        assert "\u2193" not in out
        assert "\u2192" not in out

    def test_missing_dependency_raises_importerror(self) -> None:
        """render_phart propagates ImportError when phart is unavailable."""
        import builtins

        real_import = builtins.__import__

        def blocked(name, *a, **k):
            if name == "phart" or name.startswith("phart."):
                raise ImportError("No module named 'phart'")
            return real_import(name, *a, **k)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(builtins, "__import__", blocked)
            with pytest.raises(ImportError):
                _dag_render.render_phart(_make_wf())


# ═══════════════════════════════════════════════════════════════════════════
# Edge helpers
# ═══════════════════════════════════════════════════════════════════════════


class TestEdgeHelpers:
    def test_edges_topological(self) -> None:
        edges = _dag_render._edges(_make_wf())
        # Every edge is (dependency, task); gather_sources has no incoming edge
        assert ("gather_sources", "download_source") in edges
        assert ("download_source", "convert_source") in edges
        assert ("prepare_shared", "convert_source") in edges

    def test_array_tasks_detected(self) -> None:
        arrays = _dag_render._array_tasks(_make_wf())
        assert arrays == {"download_source", "convert_source"}
