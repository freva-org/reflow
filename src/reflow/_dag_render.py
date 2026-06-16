"""DAG rendering for the ``dag`` CLI command.

Renders a workflow's task graph in one of several formats:

- ``text``    : indented adjacency list (default, zero dependencies)
- ``mermaid`` : Mermaid ``flowchart`` source (zero dependencies)
- ``dot``     : Graphviz DOT source (zero dependencies)
- ``phart``   : pretty ASCII/Unicode rendered in the terminal, requires the
                optional ``reflow[pretty]`` extra (phart + networkx)

The text, mermaid, and dot renderers are pure string emission.  The phart
renderer imports lazily and the caller is responsible for falling back to
text when the import is unavailable.

Array tasks are marked consistently across formats: a ``[array]`` suffix in
text, a distinct node shape in mermaid/dot, and angle-bracket decorators in
phart.
"""

from __future__ import annotations

from .workflow import Workflow

FORMATS = ("text", "mermaid", "dot", "phart")


def _edges(wf: Workflow) -> list[tuple[str, str]]:
    """Return (dependency, task) edges in topological order."""
    edges: list[tuple[str, str]] = []
    for tname in wf._topological_order():
        spec = wf.tasks[tname]
        for dep in wf._effective_dependencies(spec):
            edges.append((dep, tname))
    return edges


def _array_tasks(wf: Workflow) -> set[str]:
    return {name for name, spec in wf.tasks.items() if spec.config.array}


def render_text(wf: Workflow) -> str:
    """Indented adjacency list (the original format)."""
    lines: list[str] = []
    for tname in wf._topological_order():
        spec = wf.tasks[tname]
        deps = wf._effective_dependencies(spec)
        dep_str = f"  <- {', '.join(deps)}" if deps else ""
        tag = " [array]" if spec.config.array else ""
        lines.append(f"  {tname}{tag}{dep_str}")
    return "\n".join(lines)


def render_mermaid(wf: Workflow) -> str:
    """Mermaid ``flowchart TD`` source.

    Array tasks use the subroutine shape ``[[name]]``; singletons use the
    default box ``[name]``.  Renders natively in mkdocs-material and GitHub.
    """
    array = _array_tasks(wf)
    lines = ["flowchart TD"]
    # Declare nodes first so isolated tasks (no edges) still appear.
    for tname in wf._topological_order():
        if tname in array:
            lines.append(f"    {tname}[[{tname}]]")
        else:
            lines.append(f"    {tname}[{tname}]")
    for dep, tname in _edges(wf):
        lines.append(f"    {dep} --> {tname}")
    return "\n".join(lines)


def render_dot(wf: Workflow) -> str:
    """Graphviz DOT source.

    Array tasks are drawn as boxes with doubled borders (``peripheries=2``)
    to distinguish them from singleton tasks.
    """
    array = _array_tasks(wf)
    lines = ["digraph reflow {", "    rankdir=TB;", "    node [shape=box];"]
    for tname in wf._topological_order():
        if tname in array:
            lines.append(f'    "{tname}" [peripheries=2];')
        else:
            lines.append(f'    "{tname}";')
    for dep, tname in _edges(wf):
        lines.append(f'    "{dep}" -> "{tname}";')
    lines.append("}")
    return "\n".join(lines)


def render_phart(wf: Workflow, *, use_ascii: bool = False) -> str:
    """Pretty ASCII/Unicode DAG via the optional phart + networkx extra.

    Array tasks are suffixed ``[array]`` in their label so they stand out
    from singleton tasks.  Raises ImportError if the extra is not installed;
    the caller should catch this and fall back to text.

    The marker is baked into the node label rather than using phart's
    ``custom_decorators``/``NodeStyle.CUSTOM``, because those are only
    available in newer phart releases and their behaviour varies across
    versions.  A plain label suffix renders consistently everywhere.
    """
    import networkx as nx  # noqa: PLC0415 - optional dependency
    from phart import ASCIIRenderer  # noqa: PLC0415

    array = _array_tasks(wf)

    def label(name: str) -> str:
        return f"{name} [array]" if name in array else name

    g = nx.DiGraph()
    # Add all nodes so isolated tasks still render.
    for tname in wf._topological_order():
        g.add_node(label(tname))
    g.add_edges_from((label(dep), label(t)) for dep, t in _edges(wf))

    renderer = ASCIIRenderer(g, use_ascii=use_ascii)
    result: str = renderer.render()
    return result.rstrip("\n")


def render(wf: Workflow, fmt: str, *, use_ascii: bool = False) -> str:
    """Render the DAG in *fmt*. phart import errors propagate to the caller."""
    if fmt == "text":
        return render_text(wf)
    if fmt == "mermaid":
        return render_mermaid(wf)
    if fmt == "dot":
        return render_dot(wf)
    if fmt == "phart":
        return render_phart(wf, use_ascii=use_ascii)
    raise ValueError(f"Unknown DAG format: {fmt!r}")
