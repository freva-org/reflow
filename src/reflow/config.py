"""User configuration for reflow.

Loads settings from ``~/.config/reflow/config.toml`` (XDG standard).
Falls back to environment variables when the config file is absent.

Example config file::

    [executor]
    partition = "compute"
    account = "bm1159"
    python = "/sw/spack-levante/mambaforge-23.1/bin/python"
    mode = "sbatch"

    [defaults]
    run_dir = "/scratch/k204221/reflow"

    [server]
    url = "https://flow.dkrz.de"
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any


def _load_toml(path: Path) -> dict[str, Any]:
    """Load a TOML file, using tomllib (3.11+) or tomli (3.10)."""
    if sys.version_info >= (3, 11):
        import tomllib
    else:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError:
            return {}
    try:
        with open(path, "rb") as fh:
            return tomllib.load(fh)
    except FileNotFoundError:
        return {}


def _config_dir() -> Path:
    """Return the XDG config directory for reflow."""
    xdg = os.getenv("XDG_CONFIG_HOME")
    if xdg:
        return Path(xdg) / "reflow"
    return Path.home() / ".config" / "reflow"


def _cache_dir() -> Path:
    """Return the XDG cache directory for reflow."""
    xdg = os.getenv("XDG_CACHE_HOME")
    if xdg:
        return Path(xdg) / "reflow"
    return Path.home() / ".cache" / "reflow"


def _config_path() -> Path:
    """Return the default config file path."""
    return _config_dir() / "config.toml"


class Config:
    """Loaded user configuration.

    Values are resolved from the config file first, then from
    environment variables as fallback.

    Parameters
    ----------
    data : dict[str, Any]
        Raw parsed TOML data.

    Attributes
    ----------
    executor_partition : str or None
        Default Slurm partition.
    executor_account : str or None
        Default Slurm account.
    executor_python : str or None
        Default Python interpreter for workers.
    executor_mode : str or None
        Default executor mode (``"sbatch"`` or ``"dry-run"``).
    default_run_dir : str or None
        Default run directory.
    server_url : str or None
        Server URL for future registration.
    default_store_path : str
        Default path to the shared SQLite manifest database.
    """

    def __init__(self, data: dict[str, Any] | None = None) -> None:
        self._data: dict[str, Any] = data or {}

    def _get(self, section: str, key: str, env_var: str | None = None) -> str | None:
        """Look up a value: config file -> env var -> None."""
        val = self._data.get(section, {}).get(key)
        if val is not None:
            return str(val)
        if env_var is not None:
            return os.getenv(env_var)
        return None

    @property
    def executor_partition(self) -> str | None:
        return self._get("executor", "partition", "REFLOW_PARTITION")

    @property
    def executor_account(self) -> str | None:
        return self._get("executor", "account", "REFLOW_ACCOUNT")

    @property
    def executor_python(self) -> str | None:
        return self._get("executor", "python", "REFLOW_PYTHON")

    @property
    def executor_mode(self) -> str | None:
        return self._get("executor", "mode", "REFLOW_MODE")

    @property
    def mail_user(self) -> str | None:
        return self._get("notifications", "mail_user", "REFLOW_MAIL_USER")

    @property
    def mail_type(self) -> str | None:
        return self._get("notifications", "mail_type", "REFLOW_MAIL_TYPE")

    @property
    def signal(self) -> str | None:
        return self._get("executor", "signal", "REFLOW_SIGNAL")

    @property
    def default_run_dir(self) -> str | None:
        return self._get("defaults", "run_dir")

    @property
    def server_url(self) -> str | None:
        return self._get("server", "url", "REFLOW_SERVER_URL")

    @property
    def default_store_path(self) -> str:
        configured = self._get("defaults", "store_path", "REFLOW_STORE_PATH")
        if configured is not None:
            return configured
        return str(_cache_dir() / "manifest.db")


def load_config(path: Path | str | None = None) -> Config:
    """Load the user configuration.

    Parameters
    ----------
    path : Path, str, or None
        Explicit config file path.  ``None`` uses the default
        ``~/.config/reflow/config.toml``.

    Returns
    -------
    Config
    """
    p = Path(path) if path is not None else _config_path()
    data = _load_toml(p)
    return Config(data)
