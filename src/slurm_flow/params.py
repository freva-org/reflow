"""Parameter descriptors for slurm_flow task functions.

Use :class:`Param` with :data:`typing.Annotated` to control how task
function parameters appear on the auto-generated CLI.

Examples
--------
>>> from typing import Annotated
>>> from slurm_flow import Graph, Param
>>>
>>> graph = Graph("pipeline")
>>>
>>> @graph.job()
... def prepare(
...     start: Annotated[str, Param(help="Start date, ISO-8601")],
...     end: Annotated[str, Param(help="End date, ISO-8601")],
...     s3_secret: Annotated[str, Param(help="Path to S3 credential file")],
...     run_dir: str,
... ) -> Dict[str, List[str]]:
...     ...
>>>
>>> @graph.array_job(expand="prepare.files")
... def convert(
...     item: str,
...     bucket: Annotated[str, Param(help="Target S3 bucket")],
...     chunk_size: Annotated[int, Param(
...         help="Chunk size in MB",
...         namespace="local",
...     )] = 256,
... ) -> Dict[str, str]:
...     ...

On the CLI the above produces::

    python flow.py submit \\
        --run-dir /scratch/run1 \\
        --start 2025-01-01 \\
        --end 2025-01-31 \\
        --s3-secret /path/to/creds.json \\
        --bucket my-bucket \\
        --convert-chunk-size 512          # local → prefixed with task name
"""

import argparse
import enum
import inspect
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)


class Param:
    """Descriptor that controls how a task parameter maps to a CLI flag.

    Attach this to a function parameter via ``Annotated``::

        start: Annotated[str, Param(help="Start date")]

    Parameters
    ----------
    help : str
        Help text shown on the CLI.
    short : str or None
        Optional short flag, e.g. ``"-s"``.
    namespace : ``"global"`` or ``"local"``
        ``"global"`` (default) — one shared CLI flag across all tasks.
        ``"local"`` — the flag is prefixed with the task name, e.g.
        ``--convert-chunk-size`` for a parameter ``chunk_size`` on
        task ``convert``.
    extra : Dict[str, Any] or None
        Additional keyword arguments forwarded to
        ``argparse.ArgumentParser.add_argument``.
    """

    def __init__(
        self,
        help: str = "",
        short: Optional[str] = None,
        namespace: Literal["global", "local"] = "global",
        **extra: Any,
    ) -> None:
        self.help: str = help
        self.short: Optional[str] = short
        self.namespace: Literal["global", "local"] = namespace
        self.extra: Dict[str, Any] = extra

    def __repr__(self) -> str:
        parts = [f"help={self.help!r}"]
        if self.short:
            parts.append(f"short={self.short!r}")
        if self.namespace != "global":
            parts.append(f"namespace={self.namespace!r}")
        if self.extra:
            parts.append(f"extra={self.extra!r}")
        return f"Param({', '.join(parts)})"


# ── introspection helpers ─────────────────────────────────────────────


def _is_annotated(annotation: Any) -> bool:
    """Check whether *annotation* is ``Annotated[...]``."""
    return (
        get_origin(annotation) is type(None).__class__.__mro__[0]
        if False
        else (hasattr(annotation, "__metadata__"))
    )


def extract_param(annotation: Any) -> Optional[Param]:
    """Extract the :class:`Param` instance from an ``Annotated`` type.

    Parameters
    ----------
    annotation : Any
        A type annotation, potentially ``Annotated[T, Param(...)]``.

    Returns
    -------
    Param or None
        The first :class:`Param` found in the annotation metadata,
        or ``None``.
    """
    if not hasattr(annotation, "__metadata__"):
        return None
    for meta in annotation.__metadata__:
        if isinstance(meta, Param):
            return meta
    return None


def extract_base_type(annotation: Any) -> Any:
    """Extract the base type from an ``Annotated`` type.

    Parameters
    ----------
    annotation : Any
        A type annotation.

    Returns
    -------
    Any
        The base type (first argument of ``Annotated``, or the
        annotation itself if it is not ``Annotated``).
    """
    if hasattr(annotation, "__metadata__"):
        args = get_args(annotation)
        return args[0] if args else annotation
    return annotation


def unwrap_optional(annotation: Any) -> Tuple[Any, bool]:
    """Remove ``Optional[...]`` wrapper.

    Parameters
    ----------
    annotation : Any
        Type annotation.

    Returns
    -------
    Tuple[Any, bool]
        ``(inner_type, is_optional)``.
    """
    origin = get_origin(annotation)
    if origin is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0], True
    return annotation, False


def list_element_type(annotation: Any) -> Optional[Any]:
    """Return the element type if *annotation* is ``List[T]``.

    Parameters
    ----------
    annotation : Any
        Type annotation.

    Returns
    -------
    Any or None
        Element type, or ``None``.
    """
    origin = get_origin(annotation)
    if origin in (list, List):
        args = get_args(annotation)
        return args[0] if len(args) == 1 else None
    return None


# ── per-task parameter collection ────────────────────────────────────


class ResolvedParam:
    """Internal representation of one CLI parameter after introspection.

    Parameters
    ----------
    name : str
        Python parameter name (e.g. ``"chunk_size"``).
    task_name : str
        Owning task name (e.g. ``"convert"``).
    base_type : Any
        Python type (``str``, ``int``, ...).
    is_list : bool
        Whether the type is ``List[T]``.
    required : bool
        Whether the parameter has no default.
    default : Any
        Default value, or ``None``.
    param : Param or None
        The attached :class:`Param` descriptor, if any.
    """

    def __init__(
        self,
        name: str,
        task_name: str,
        base_type: Any,
        is_list: bool,
        required: bool,
        default: Any,
        param: Optional[Param],
    ) -> None:
        self.name = name
        self.task_name = task_name
        self.base_type = base_type
        self.is_list = is_list
        self.required = required
        self.default = default
        self.param = param

    @property
    def namespace(self) -> str:
        """Effective namespace (``"global"`` or ``"local"``)."""
        if self.param is not None:
            return self.param.namespace
        return "global"

    @property
    def help_text(self) -> str:
        """Help string for the CLI."""
        if self.param is not None and self.param.help:
            return self.param.help
        return ""

    @property
    def short_flag(self) -> Optional[str]:
        """Short flag like ``-s``, or ``None``."""
        if self.param is not None:
            return self.param.short
        return None

    def cli_flag(self) -> str:
        """Compute the long ``--flag-name`` for this parameter.

        Returns
        -------
        str
            CLI flag string (e.g. ``"--chunk-size"`` or
            ``"--convert-chunk-size"``).
        """
        base = self.name.replace("_", "-")
        if self.namespace == "local":
            prefix = self.task_name.replace("_", "-")
            return f"--{prefix}-{base}"
        return f"--{base}"

    def dest_name(self) -> str:
        """Compute the argparse destination key.

        For global params this is the bare name; for local params
        it is ``taskname_paramname`` to avoid collisions.

        Returns
        -------
        str
            Destination key.
        """
        if self.namespace == "local":
            return f"{self.task_name}_{self.name}"
        return self.name

    def add_to_parser(self, parser: argparse.ArgumentParser) -> None:
        """Add this parameter as an ``--option`` to an argparse parser.

        Parameters
        ----------
        parser : argparse.ArgumentParser
            The parser (or subparser) to add the option to.
        """
        names = [self.cli_flag()]
        if self.short_flag:
            names.append(self.short_flag)

        kw: Dict[str, Any] = {
            "dest": self.dest_name(),
            "help": self.help_text or None,
        }

        type_fn = argparse_type_callable(self.base_type)

        if self.is_list:
            kw["nargs"] = "+"
            kw["type"] = type_fn
            kw["default"] = self.default
            kw["required"] = self.required
        elif type_fn is _parse_bool:
            # booleans: support --flag / --no-flag style
            kw["type"] = _parse_bool
            kw["default"] = self.default
            kw["required"] = self.required
        else:
            kw["type"] = type_fn
            kw["default"] = self.default
            kw["required"] = self.required

        # forward any extras from Param(...)
        if self.param is not None:
            kw.update(self.param.extra)

        parser.add_argument(*names, **kw)


# Internal parameters that should never appear on the submit CLI.
_SKIP_PARAMS: FrozenSet = frozenset({"item", "run_id"})


def collect_params_from_task(
    task_name: str,
    func: Any,
    sig: inspect.Signature,
) -> List[ResolvedParam]:
    """Introspect one task function and return its resolved parameters.

    Parameters
    ----------
    task_name : str
        Task name.
    func : callable
        The decorated function.
    sig : inspect.Signature
        Its signature.

    Returns
    -------
    List[ResolvedParam]
        One entry per eligible parameter.
    """
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        hints = {}

    results: List[ResolvedParam] = []
    for pname, p in sig.parameters.items():
        if pname in _SKIP_PARAMS:
            continue

        raw_ann = hints.get(pname, p.annotation)
        param_meta = extract_param(raw_ann)
        base = extract_base_type(raw_ann)
        base, is_opt = unwrap_optional(base)
        list_elem = list_element_type(base)

        has_default = p.default is not inspect.Parameter.empty
        results.append(
            ResolvedParam(
                name=pname,
                task_name=task_name,
                base_type=list_elem if list_elem is not None else base,
                is_list=list_elem is not None,
                required=not has_default and pname != "run_dir",
                default=p.default if has_default else None,
                param=param_meta,
            )
        )

    return results


# ── argparse type deduction ──────────────────────────────────────────


class ParamType(enum.Enum):
    """Supported CLI parameter types.

    Each member maps to a Python callable that ``argparse`` uses to
    parse the raw string from the command line.

    Attributes
    ----------
    STRING : str
        Plain string (default).
    INT : int
        Integer.
    FLOAT : float
        Floating-point number.
    BOOL : bool
        Boolean (accepts ``true/false/yes/no/1/0``).
    PATH : Path
        File-system path.
    DATETIME : datetime
        ISO-8601 date/time string.
    """

    STRING = "str"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    PATH = "path"
    DATETIME = "datetime"


def _parse_bool(value: str) -> bool:
    """Parse a boolean CLI value.

    Parameters
    ----------
    value : str
        Raw CLI string.

    Returns
    -------
    bool

    Raises
    ------
    argparse.ArgumentTypeError
        If the value cannot be interpreted as a boolean.
    """
    normalised = value.strip().lower()
    if normalised in {"1", "true", "yes", "on"}:
        return True
    if normalised in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value!r}")


def _parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 date/time string.

    Parameters
    ----------
    value : str
        Raw CLI string.

    Returns
    -------
    datetime

    Raises
    ------
    argparse.ArgumentTypeError
        If the value cannot be parsed.
    """
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime value: {value!r} ({exc})"
        ) from exc


# Map from Python type → argparse ``type`` callable.
_TYPE_MAP: Dict[Any, Callable[..., Any]] = {
    str: str,
    int: int,
    float: float,
    bool: _parse_bool,
    Path: Path,
    datetime: _parse_datetime,
}


def argparse_type_callable(python_type: Any) -> Callable[..., Any]:
    """Return the argparse ``type`` callable for a Python type.

    Parameters
    ----------
    python_type : Any
        A Python type annotation (``str``, ``int``, ``Path``, ...).

    Returns
    -------
    Callable
        Callable suitable for ``argparse.add_argument(type=...)``.
    """
    if python_type in _TYPE_MAP:
        return _TYPE_MAP[python_type]
    if python_type is inspect.Parameter.empty:
        return str
    return str


def merge_resolved_params(
    all_params: List[ResolvedParam],
) -> List[ResolvedParam]:
    """Deduplicate global parameters; keep locals as-is.

    When two tasks declare the same global parameter, they are merged:
    if *any* declaration marks it required, the merged result is required.

    Parameters
    ----------
    all_params : List[ResolvedParam]
        Flat list from all tasks.

    Returns
    -------
    List[ResolvedParam]
        Deduplicated list.
    """
    seen_globals: Dict[str, ResolvedParam] = {}
    result: List[ResolvedParam] = []

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
