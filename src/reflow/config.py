"""User configuration for reflow.

Loads settings from ``~/.config/reflow/config.toml`` (XDG standard).
Environment variables override values from the config file.

This module also provides helpers to create a fully commented example
configuration on first use.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from textwrap import dedent
from typing import Any

DEFAULT_CONFIG_TOML = dedent(
    """\
    # Reflow configuration
    #
    # All options are commented out by default so the file has no effect until
    # you opt in to a setting.
    #
    # Resolution order at runtime is typically:
    # 1. explicit values from workflow code
    # 2. environment variables
    # 3. values from this file
    # 4. built-in defaults

    # [executor]
    # Default execution mode for Slurm-backed runs.
    # Supported values today are typically "sbatch" and "dry-run".
    # mode = "sbatch"

    # Python interpreter used for worker jobs.
    # python = "/path/to/python"

    # Default Slurm partition for task jobs.
    # partition = "compute"

    # Default Slurm account for task jobs.
    # account = "my_account"

    # Signal sent shortly before walltime expires.
    # Example: B:INT@60 sends SIGINT 60 seconds before timeout.
    # signal = "B:INT@60"

    # Override the Slurm command paths if needed.
    # sbatch = "/usr/bin/sbatch"
    # scancel = "/usr/bin/scancel"
    # sacct = "/usr/bin/sacct"

    # [notifications]
    # Default email address for scheduler notifications.
    # mail_user = "you@example.org"

    # Default scheduler mail events.
    # Examples: "FAIL", "END", "FAIL,END", "ALL"
    # mail_type = "FAIL"

    # [dispatch]
    # Resources for the workflow dispatch/coordinator job.
    # cpus = 1
    # time = "00:10:00"
    # mem = "1G"

    # Optional Slurm defaults specifically for the dispatch job.
    # partition = "my_partition"
    # account = "my_account"

    # [defaults]
    # Default directory in which reflow creates runs.
    # run_dir = "/scratch/$USER/reflow"

    # [server]
    # Reserved for a future Reflow service / web UI.
    # url = "https://flowserver.org"
    """
)


def _load_toml(path: Path) -> dict[str, Any]:
    """Load a TOML file, using tomllib (3.11+) or tomli (3.10)."""
    if sys.version_info >= (3, 11):
        import tomllib
    else:
        try:
            import tomli as tomllib
        except ImportError:
            return {}
    try:
        with open(path, "rb") as fh:
            config: dict[str, Any] = tomllib.load(fh)
            return config
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


def config_path() -> Path:
    """Return the default config file path."""
    return _config_dir() / "config.toml"


def _config_path() -> Path:
    """Backward-compatible alias for :func:`config_path`."""
    return config_path()


def write_example_config(
    path: Path | str | None = None,
    *,
    overwrite: bool = False,
) -> Path:
    """Write a fully commented example config file."""
    target = Path(path) if path is not None else config_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and not overwrite:
        raise FileExistsError(f"Config file already exists: {target}")
    try:
        target.write_text(DEFAULT_CONFIG_TOML, encoding="utf-8")
    except (PermissionError, IsADirectoryError):
        pass
    return target


def ensure_config_exists(path: Path | str | None = None) -> Path:
    """Create the default config file if it does not exist."""
    target = Path(path) if path is not None else config_path()
    if not target.exists():
        write_example_config(target, overwrite=False)
    return target


class Config:
    """Loaded user configuration.

    Environment variables override the config file.
    """

    def __init__(self, data: dict[str, Any] | None = None) -> None:
        self._data: dict[str, Any] = data or {}

    def _get(
        self,
        section: str,
        key: str,
        env_var: str | None = None,
    ) -> str | None:
        """Look up a value: env var -> config file -> None."""
        if env_var is not None:
            env_val = os.getenv(env_var)
            if env_val is not None:
                return env_val
        val = self._data.get(section, {}).get(key)
        if val is not None:
            return str(val)
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
    def executor_sbatch(self) -> str | None:
        return self._get("executor", "sbatch", "REFLOW_SBATCH")

    @property
    def executor_scancel(self) -> str | None:
        return self._get("executor", "scancel", "REFLOW_SCANCEL")

    @property
    def executor_sacct(self) -> str | None:
        return self._get("executor", "sacct", "REFLOW_SACCT")

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
    def executor_submit_options(self) -> dict[str, str]:
        """Default scheduler-native submit options for task jobs."""
        options: dict[str, str] = {}
        mapping = {
            "partition": self.executor_partition,
            "account": self.executor_account,
            "signal": self.signal,
            "mail_user": self.mail_user,
            "mail_type": self.mail_type,
        }
        for key, value in mapping.items():
            if value is not None:
                options[key] = value
        return options

    @property
    def dispatch_cpus(self) -> str | None:
        return self._get("dispatch", "cpus", "REFLOW_DISPATCH_CPUS")

    @property
    def dispatch_time(self) -> str | None:
        return self._get("dispatch", "time", "REFLOW_DISPATCH_TIME")

    @property
    def dispatch_mem(self) -> str | None:
        return self._get("dispatch", "mem", "REFLOW_DISPATCH_MEM")

    @property
    def dispatch_partition(self) -> str | None:
        return self._get("dispatch", "partition", "REFLOW_DISPATCH_PARTITION")

    @property
    def dispatch_account(self) -> str | None:
        return self._get("dispatch", "account", "REFLOW_DISPATCH_ACCOUNT")

    @property
    def dispatch_submit_options(self) -> dict[str, str]:
        """Default scheduler-native submit options for the dispatch job."""
        options: dict[str, str] = {}
        mapping = {
            "partition": self.dispatch_partition,
            "account": self.dispatch_account,
        }
        for key, value in mapping.items():
            if value is not None:
                options[key] = value
        return options

    @property
    def default_run_dir(self) -> str | None:
        return self._get("defaults", "run_dir")

    @property
    def default_store_path(self) -> str:
        configured = self._get("defaults", "store_path", "REFLOW_STORE_PATH")
        if configured is not None:
            return configured
        return str(_cache_dir() / "manifest.db")

    @property
    def server_url(self) -> str | None:
        return self._get("server", "url", "REFLOW_SERVER_URL")


def load_config(path: Path | str | None = None) -> Config:
    """Load the user configuration."""
    p = Path(path) if path is not None else config_path()
    data = _load_toml(ensure_config_exists(p))
    return Config(data)
