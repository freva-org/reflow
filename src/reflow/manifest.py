"""Typed manifest serialization for reflow.

This module provides a small, dependency-free codec for values that need to be
stored in the manifest or exposed through workflow descriptions.  The goal is to
round-trip a useful subset of Python types without falling back to ad-hoc
``str(...)`` coercion.

Supported value types
---------------------

The default codec preserves these types across JSON storage:

- JSON primitives: ``None``, ``bool``, ``int``, ``float``, ``str``
- ``list`` and ``dict`` (string keys only)
- ``tuple``
- `pathlib.Path`
- `datetime.datetime`
- `uuid.UUID`
- `enum.Enum`
- dataclass instances

Unknown values raise `TypeError` instead of being silently flattened.
This keeps the manifest schema predictable and surfaces unsupported payloads
close to the point where they are produced.
"""

from __future__ import annotations

import importlib
import json
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar
from uuid import UUID

SCHEMA_VERSION = 1

_MARKER = "__reflow_manifest__"
_TYPE = "type"
_VALUE = "value"
_CLASS = "class"
_FIELDS = "fields"


@dataclass(frozen=True)
class CliParamDescription:
    """Typed description of one CLI parameter."""

    name: str
    task: str | None
    flag: str
    type_repr: str
    required: bool
    default: Any
    namespace: str
    help: str | None
    choices: list[Any] | None

    def to_manifest_dict(self, codec: ManifestCodec | None = None) -> dict[str, Any]:
        """Return a JSON-safe manifest representation."""
        c = codec or DEFAULT_CODEC
        return {
            "name": self.name,
            "task": self.task,
            "flag": self.flag,
            "type": self.type_repr,
            "required": self.required,
            "default": c.dump_value(self.default),
            "namespace": self.namespace,
            "help": self.help,
            "choices": c.dump_value(self.choices),
        }


@dataclass(frozen=True)
class TaskDescription:
    """Typed description of one registered workflow task."""

    name: str
    is_array: bool
    config: dict[str, Any]
    dependencies: list[str]
    result_deps: dict[str, dict[str, Any]]
    return_type: str | None
    has_verify: bool

    def to_manifest_dict(self, codec: ManifestCodec | None = None) -> dict[str, Any]:
        """Return a JSON-safe manifest representation."""
        c = codec or DEFAULT_CODEC
        return {
            "name": self.name,
            "is_array": self.is_array,
            "config": c.dump_value(self.config),
            "dependencies": list(self.dependencies),
            "result_deps": c.dump_value(self.result_deps),
            "return_type": self.return_type,
            "has_verify": self.has_verify,
        }


@dataclass(frozen=True)
class WorkflowDescription:
    """Typed description of a workflow definition."""

    name: str
    entrypoint: Path
    python: Path
    working_dir: Path
    tasks: list[TaskDescription]
    cli_params: list[CliParamDescription]
    schema_version: int = SCHEMA_VERSION

    def to_manifest_dict(self, codec: ManifestCodec | None = None) -> dict[str, Any]:
        """Return a JSON-safe manifest representation."""
        c = codec or DEFAULT_CODEC
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "entrypoint": c.dump_value(self.entrypoint),
            "python": c.dump_value(self.python),
            "working_dir": c.dump_value(self.working_dir),
            "tasks": [task.to_manifest_dict(c) for task in self.tasks],
            "cli_params": [param.to_manifest_dict(c) for param in self.cli_params],
        }


@dataclass(frozen=True)
class ManifestCodec:
    """Builtin-only JSON codec for manifest values.

    The codec converts selected Python types to JSON-safe tagged structures and
    can reconstruct them on load.
    """

    marker_key: str = _MARKER
    schema_version: int = SCHEMA_VERSION

    _json_scalar_types: ClassVar[tuple[type[Any], ...]] = (
        type(None),
        bool,
        int,
        float,
        str,
    )

    def dump_value(self, value: Any) -> Any:
        """Convert a Python value into a JSON-safe representation."""
        if isinstance(value, Enum):
            return {
                self.marker_key: True,
                _TYPE: "enum",
                _CLASS: self._qualname(type(value)),
                _VALUE: self.dump_value(value.value),
            }

        if isinstance(value, self._json_scalar_types):
            return value

        if isinstance(value, Path):
            return self._tag("path", str(value))

        if isinstance(value, datetime):
            return self._tag("datetime", value.isoformat())

        if isinstance(value, UUID):
            return self._tag("uuid", str(value))

        if is_dataclass(value) and not isinstance(value, type):
            payload = {
                name: self.dump_value(getattr(value, name))
                for name in (field.name for field in fields(value))
            }
            return {
                self.marker_key: True,
                _TYPE: "dataclass",
                _CLASS: self._qualname(type(value)),
                _FIELDS: payload,
            }

        if isinstance(value, tuple):
            return self._tag("tuple", [self.dump_value(item) for item in value])

        if isinstance(value, list):
            return [self.dump_value(item) for item in value]

        if isinstance(value, dict):
            bad_key = next((key for key in value if not isinstance(key, str)), None)
            if bad_key is not None:
                raise TypeError(
                    "Manifest dictionaries must use string keys; "
                    f"got key {bad_key!r} of type {type(bad_key).__name__}."
                )
            return {key: self.dump_value(item) for key, item in value.items()}

        raise TypeError(
            "Unsupported manifest value type: "
            f"{type(value).__module__}.{type(value).__qualname__}"
        )

    def load_value(self, value: Any) -> Any:
        """Reconstruct a Python value from a JSON-safe representation."""
        if isinstance(value, self._json_scalar_types):
            return value

        if isinstance(value, list):
            return [self.load_value(item) for item in value]

        if not isinstance(value, dict):
            raise TypeError(f"Unsupported manifest payload: {type(value).__name__}")

        if value.get(self.marker_key) is True:
            type_name = value.get(_TYPE)
            if type_name == "path":
                return Path(self._require(value, _VALUE, str))
            if type_name == "datetime":
                return datetime.fromisoformat(self._require(value, _VALUE, str))
            if type_name == "uuid":
                return UUID(self._require(value, _VALUE, str))
            if type_name == "tuple":
                items = self._require(value, _VALUE, list)
                return tuple(self.load_value(item) for item in items)
            if type_name == "enum":
                raw = self.load_value(value.get(_VALUE))
                cls = self._import_type(self._require(value, _CLASS, str))
                if cls is None or not issubclass(cls, Enum):
                    return raw
                return cls(raw)
            if type_name == "dataclass":
                cls_name = self._require(value, _CLASS, str)
                payload = self._require(value, _FIELDS, dict)
                restored = {key: self.load_value(item) for key, item in payload.items()}
                cls = self._import_type(cls_name)
                if cls is None or not is_dataclass(cls):
                    return restored
                return cls(**restored)
            raise TypeError(f"Unknown manifest payload type {type_name!r}")

        return {key: self.load_value(item) for key, item in value.items()}

    def dumps(self, value: Any, *, pretty: bool = False) -> str:
        """Serialise a Python value to JSON using manifest typing."""
        payload = self.dump_value(value)
        if pretty:
            return json.dumps(payload, sort_keys=True, indent=2)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def loads(self, payload: str | bytes | bytearray) -> Any:
        """Load a value previously written with [`dumps`][dumps]."""
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode()
        return self.load_value(json.loads(payload))

    def canonical_dumps(self, value: Any) -> str:
        """Return canonical JSON for hashing."""
        return self.dumps(value, pretty=False)

    def _tag(self, type_name: str, value: Any) -> dict[str, Any]:
        return {
            self.marker_key: True,
            _TYPE: type_name,
            _VALUE: value,
        }

    @staticmethod
    def _qualname(this: type[Any]) -> str:
        return f"{this.__module__}.{this.__qualname__}"

    @staticmethod
    def _require(mapping: dict[str, Any], key: str, expected: type[Any]) -> Any:
        value = mapping.get(key)
        if not isinstance(value, expected):
            raise TypeError(
                f"Manifest field {key!r} must be {expected.__name__}, got "
                f"{type(value).__name__}"
            )
        return value

    @staticmethod
    def _import_type(qualname: str) -> type[Any] | None:
        parts = qualname.split(".")
        for split_at in range(len(parts) - 1, 0, -1):
            module_name = ".".join(parts[:split_at])
            attr_parts = parts[split_at:]
            try:
                module = importlib.import_module(module_name)
            except Exception:
                continue
            obj: Any = module
            for part in attr_parts:
                obj = getattr(obj, part, None)
                if obj is None:
                    break
            else:
                return obj if isinstance(obj, type) else None
        return None


DEFAULT_CODEC = ManifestCodec()


def manifest_dumps(value: Any, *, pretty: bool = False) -> str:
    """Serialise a manifest value with the default codec."""
    return DEFAULT_CODEC.dumps(value, pretty=pretty)


def manifest_loads(payload: str | bytes | bytearray) -> Any:
    """Load a manifest value with the default codec."""
    return DEFAULT_CODEC.loads(payload)


def canonical_manifest_dumps(value: Any) -> str:
    """Return canonical JSON for hashing and equality checks."""
    return DEFAULT_CODEC.canonical_dumps(value)
