"""Parameter descriptors and type introspection for reflow.

User-facing descriptor types:

* :class:`Param` -- CLI argument with help text, short flags, scoping.
* :class:`Result` -- data dependency on an upstream task's output.
* :class:`RunDir` -- marker for the run working directory.

Also provides helpers for type introspection, argparse integration,
wiring mode inference, and static type validation.
"""

from __future__ import annotations

import argparse
import enum
import inspect
import types as _types
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Literal,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

# --- user-facing descriptor types ------------------------------------------


class RunDir:
    """Marker type for the run working directory.

    A parameter typed as ``RunDir`` (or named ``run_dir``) is injected
    with a ``pathlib.Path`` at runtime.  It never appears on the CLI.

    Examples
    --------
    >>> @workflow.job()
    ... def ingest(work_dir: RunDir, start: str) -> list[str]:
    ...     output = work_dir / "output"
    ...     ...

    """


class Result:
    """Declare a data dependency on one or more upstream tasks.

    Parameters
    ----------
    step : str or None
        Single upstream task name.
    steps : list[str] or None
        Multiple upstream task names (outputs are concatenated).

    Raises
    ------
    ValueError
        If neither *step* nor *steps* is provided, or both are.

    Examples
    --------
    Single upstream::

        nc_file: Annotated[str, Result(step="prepare")]

    Multiple upstreams (concatenated)::

        item: Annotated[str, Result(steps=["prepare_a", "prepare_b"])]

    """

    def __init__(
        self,
        step: str | None = None,
        steps: list[str] | None = None,
    ) -> None:
        if step is not None and steps is not None:
            raise ValueError("Provide either 'step' or 'steps', not both.")
        if step is None and steps is None:
            raise ValueError("Must provide 'step' or 'steps'.")
        self.steps: list[str] = [step] if step is not None else list(steps)  # type: ignore[arg-type]

    def __repr__(self) -> str:
        if len(self.steps) == 1:
            return f"Result(step={self.steps[0]!r})"
        return f"Result(steps={self.steps!r})"


class Param:
    """CLI parameter descriptor.

    Parameters
    ----------
    help : str
        Help text shown in ``--help``.
    short : str or None
        Short flag, e.g. ``"-s"``.
    namespace : ``"global"`` or ``"local"``
        ``"global"`` -- one shared CLI flag.
        ``"local"`` -- flag is prefixed with the task name.

    Examples
    --------
    >>> start: Annotated[str, Param(help="Start date, ISO-8601")]
    >>> chunk: Annotated[int, Param(help="Chunk size", namespace="local")] = 256

    """

    def __init__(
        self,
        help: str = "",
        short: str | None = None,
        namespace: Literal["global", "local"] = "global",
        **extra: Any,
    ) -> None:
        self.help: str = help
        self.short: str | None = short
        self.namespace: Literal["global", "local"] = namespace
        self.extra: dict[str, Any] = extra

    def __repr__(self) -> str:
        parts = [f"help={self.help!r}"]
        if self.short:
            parts.append(f"short={self.short!r}")
        if self.namespace != "global":
            parts.append(f"namespace={self.namespace!r}")
        return f"Param({', '.join(parts)})"


# --- annotation extraction -------------------------------------------------


def extract_result(annotation: Any) -> Result | None:
    """Find the first :class:`Result` in an ``Annotated`` type."""
    if not hasattr(annotation, "__metadata__"):
        return None
    for meta in annotation.__metadata__:
        if isinstance(meta, Result):
            return meta
    return None


def extract_param(annotation: Any) -> Param | None:
    """Find the first :class:`Param` in an ``Annotated`` type."""
    if not hasattr(annotation, "__metadata__"):
        return None
    for meta in annotation.__metadata__:
        if isinstance(meta, Param):
            return meta
    return None


def extract_base_type(annotation: Any) -> Any:
    """Unwrap ``Annotated[T, ...]`` to ``T``."""
    if hasattr(annotation, "__metadata__"):
        args = get_args(annotation)
        return args[0] if args else annotation
    return annotation


def is_run_dir(annotation: Any) -> bool:
    """Check whether *annotation* is ``RunDir``."""
    base = extract_base_type(annotation)
    try:
        return base is RunDir or (isinstance(base, type) and issubclass(base, RunDir))
    except TypeError:
        return False


def unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    """Remove ``Optional[X]`` / ``X | None`` wrappers.

    Handles both ``typing.Union[X, None]`` and ``X | None``
    (``types.UnionType`` on 3.10+).
    """
    origin = get_origin(annotation)
    if origin is Union or isinstance(annotation, _types.UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0], True
    return annotation, False


def get_list_element_type(annotation: Any) -> Any | None:
    """Return the element type if *annotation* is ``list[T]``."""
    origin = get_origin(annotation)
    if origin is list:
        args = get_args(annotation)
        return args[0] if len(args) == 1 else None
    return None


def is_list_type(annotation: Any) -> bool:
    """Check whether *annotation* is ``list[...]``."""
    return get_origin(annotation) is list


def get_literal_values(annotation: Any) -> tuple[Any, ...] | None:
    """Return the allowed values if *annotation* is ``Literal[...]``.

    Parameters
    ----------
    annotation : Any
        Type annotation.

    Returns
    -------
    tuple or None
        The literal values, or ``None`` if not a Literal type.

    """
    if get_origin(annotation) is Literal:
        return get_args(annotation)
    return None


# --- wiring mode inference --------------------------------------------------


class WireMode(enum.Enum):
    """How an upstream task's output is wired to a downstream parameter.

    Attributes
    ----------
    DIRECT : str
        Singleton -> singleton, types match directly.
    FAN_OUT : str
        Singleton returns ``list[T]``, array_job parameter is ``T``.
    GATHER : str
        Array returns ``T``, singleton parameter is ``list[T]``.
    GATHER_FLATTEN : str
        Array returns ``list[T]``, singleton parameter is ``list[T]``.
    CHAIN : str
        Array returns ``T``, array parameter is ``T``.  1:1 by index.
    CHAIN_FLATTEN : str
        Array returns ``list[T]``, array parameter is ``T``.
        Gather + flatten + re-fan-out.

    """

    DIRECT = "direct"
    FAN_OUT = "fan_out"
    GATHER = "gather"
    GATHER_FLATTEN = "gather_flatten"
    CHAIN = "chain"
    CHAIN_FLATTEN = "chain_flatten"


def infer_wire_mode(
    upstream_return_type: Any,
    upstream_is_array: bool,
    downstream_param_type: Any,
    downstream_is_array: bool,
) -> WireMode:
    """Determine how data flows between two tasks.

    Raises
    ------
    TypeError
        If the types are incompatible.

    """
    up_is_list = is_list_type(upstream_return_type)

    if not upstream_is_array:
        if downstream_is_array:
            if up_is_list:
                return WireMode.FAN_OUT
            raise TypeError(
                f"Array job expects fan-out but upstream returns "
                f"{upstream_return_type!r}, not a list type."
            )
        return WireMode.DIRECT

    down_is_list = is_list_type(downstream_param_type)
    if not downstream_is_array:
        if down_is_list:
            if up_is_list:
                return WireMode.GATHER_FLATTEN
            return WireMode.GATHER
        raise TypeError(
            f"Singleton parameter type {downstream_param_type!r} cannot "
            f"gather from an array task.  Use list[...] to collect results."
        )

    if up_is_list:
        return WireMode.CHAIN_FLATTEN
    return WireMode.CHAIN


def check_type_compatibility(
    upstream_return_type: Any,
    upstream_is_array: bool,
    downstream_param_type: Any,
    downstream_is_array: bool,
    upstream_name: str,
    downstream_name: str,
    param_name: str,
) -> WireMode:
    """Validate and return the wire mode, with descriptive errors."""
    try:
        return infer_wire_mode(
            upstream_return_type,
            upstream_is_array,
            downstream_param_type,
            downstream_is_array,
        )
    except TypeError as exc:
        raise TypeError(
            f"Type mismatch wiring {upstream_name!r} -> "
            f"{downstream_name!r}.{param_name}: {exc}"
        ) from exc


# --- resolved parameter (for CLI building) ---------------------------------


class ResolvedParam:
    """Internal representation of one CLI parameter after introspection."""

    def __init__(
        self,
        name: str,
        task_name: str,
        base_type: Any,
        is_list: bool,
        required: bool,
        default: Any,
        param: Param | None,
        literal_choices: tuple[Any, ...] | None = None,
    ) -> None:
        self.name = name
        self.task_name = task_name
        self.base_type = base_type
        self.is_list = is_list
        self.required = required
        self.default = default
        self.param = param
        self.literal_choices = literal_choices

    @property
    def namespace(self) -> str:
        """Return the CLI namespace used for this bound parameter."""
        return self.param.namespace if self.param is not None else "global"

    @property
    def help_text(self) -> str:
        """Return the human-readable help text for the parameter."""
        return self.param.help if self.param is not None and self.param.help else ""

    @property
    def short_flag(self) -> str | None:
        """Return the short CLI flag, if one is defined."""
        return self.param.short if self.param is not None else None

    def cli_flag(self) -> str:
        """Return the long CLI flag for this bound parameter."""
        base = self.name.replace("_", "-")
        if self.namespace == "local":
            prefix = self.task_name.replace("_", "-")
            return f"--{prefix}-{base}"
        return f"--{base}"

    def dest_name(self) -> str:
        """Return the argparse destination name for this parameter."""
        if self.namespace == "local":
            return f"{self.task_name}_{self.name}"
        return self.name

    def add_to_parser(self, parser: argparse.ArgumentParser) -> None:
        """Add this parameter to an argparse parser."""
        names = [self.cli_flag()]
        if self.short_flag:
            names.append(self.short_flag)

        kw: dict[str, Any] = {
            "dest": self.dest_name(),
            "help": self.help_text or None,
            "default": self.default,
            "required": self.required,
        }

        if self.literal_choices is not None:
            kw["choices"] = list(self.literal_choices)
            kw["type"] = type(self.literal_choices[0]) if self.literal_choices else str
        else:
            kw["type"] = argparse_type_callable(self.base_type)

        if self.is_list:
            kw["nargs"] = "+"

        if self.param is not None:
            kw.update(self.param.extra)

        parser.add_argument(*names, **kw)


# --- per-task parameter collection ------------------------------------------

_SKIP_PARAMS: frozenset[str] = frozenset({"run_id"})


def collect_cli_params(
    task_name: str,
    func: Any,
    sig: inspect.Signature,
) -> list[ResolvedParam]:
    """Introspect a task function and return its CLI-eligible parameters.

    Parameters annotated with Result, RunDir, or named ``run_dir`` are
    excluded.
    """
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        hints = {}

    results: list[ResolvedParam] = []
    for pname, p in sig.parameters.items():
        if pname in _SKIP_PARAMS:
            continue

        raw_ann = hints.get(pname, p.annotation)

        if extract_result(raw_ann) is not None:
            continue
        if is_run_dir(raw_ann) or pname == "run_dir":
            continue

        param_meta = extract_param(raw_ann)
        base = extract_base_type(raw_ann)
        base, _ = unwrap_optional(base)

        # Check for Literal types.
        literal_vals = get_literal_values(base)
        if literal_vals is not None:
            has_default = p.default is not inspect.Parameter.empty
            results.append(
                ResolvedParam(
                    name=pname,
                    task_name=task_name,
                    base_type=type(literal_vals[0]) if literal_vals else str,
                    is_list=False,
                    required=not has_default,
                    default=p.default if has_default else None,
                    param=param_meta,
                    literal_choices=literal_vals,
                )
            )
            continue

        list_elem = get_list_element_type(base)
        has_default = p.default is not inspect.Parameter.empty
        results.append(
            ResolvedParam(
                name=pname,
                task_name=task_name,
                base_type=list_elem if list_elem is not None else base,
                is_list=list_elem is not None,
                required=not has_default,
                default=p.default if has_default else None,
                param=param_meta,
            )
        )

    return results


def merge_resolved_params(all_params: list[ResolvedParam]) -> list[ResolvedParam]:
    """Deduplicate global parameters; keep locals as-is."""
    seen_globals: dict[str, ResolvedParam] = {}
    result: list[ResolvedParam] = []
    for rp in all_params:
        if rp.namespace == "local":
            result.append(rp)
            continue
        if rp.name in seen_globals:
            if rp.required:
                seen_globals[rp.name].required = True
            continue
        seen_globals[rp.name] = rp
        result.append(rp)
    return result


# --- argparse type deduction ------------------------------------------------


def _parse_bool(value: str) -> bool:
    normalised = value.strip().lower()
    if normalised in {"1", "true", "yes", "on"}:
        return True
    if normalised in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value!r}")


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime: {value!r} ({exc})"
        ) from exc


_TYPE_MAP: dict[Any, Any] = {
    str: str,
    int: int,
    float: float,
    bool: _parse_bool,
    Path: Path,
    datetime: _parse_datetime,
}


def argparse_type_callable(python_type: Any) -> Any:
    """Map a Python type to an argparse ``type`` callable."""
    return _TYPE_MAP.get(python_type, str)


# --- introspection: Result deps and return type -----------------------------


def collect_result_deps(func: Any) -> dict[str, Result]:
    """Return ``{param_name: Result(...)}`` for all Result-annotated params."""
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        hints = {}
    result: dict[str, Result] = {}
    for pname, ann in hints.items():
        r = extract_result(ann)
        if r is not None:
            result[pname] = r
    return result


def get_return_type(func: Any) -> Any:
    """Return the return annotation of a function."""
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        hints = {}
    return hints.get("return", inspect.Parameter.empty)
