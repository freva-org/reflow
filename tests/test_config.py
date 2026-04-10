"""test_config.py - refactored reflow tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from reflow import (
    Config,
    Run,
    RunDir,
    Workflow,
    ensure_config_exists,
)
from reflow.config import write_example_config, config_path
from reflow.config import load_config

# ═══════════════════════════════════════════════════════════════════════════
# _types.py
# ═══════════════════════════════════════════════════════════════════════════


class TestConfig:
    def test_load_missing(self, tmp_path: Path) -> None:
        from reflow.config import load_config

        cfg = load_config(tmp_path / "nonexistent.toml")
        assert cfg.executor_partition is None

    def test_env_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from reflow.config import load_config

        monkeypatch.setenv("REFLOW_PARTITION", "gpu")
        cfg = load_config(Path("/nonexistent"))
        assert cfg.executor_partition == "gpu"


class TestConfigProperties:
    def test_dispatch_properties(self) -> None:
        cfg = Config(
            {
                "dispatch": {
                    "cpus": 2,
                    "time": "01:00:00",
                    "mem": "4G",
                    "partition": "debug",
                    "account": "myacc",
                }
            }
        )
        assert cfg.dispatch_cpus == "2"
        assert cfg.dispatch_time == "01:00:00"
        assert cfg.dispatch_mem == "4G"
        assert cfg.dispatch_partition == "debug"
        assert cfg.dispatch_account == "myacc"

    def test_dispatch_submit_options(self) -> None:
        cfg = Config(
            {
                "dispatch": {"partition": "debug", "account": "acc"},
            }
        )
        opts = cfg.dispatch_submit_options
        assert opts["partition"] == "debug"
        assert opts["account"] == "acc"

    def test_empty_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Clear any env vars that might be set by other tests.
        for var in (
            "REFLOW_MODE",
            "REFLOW_PARTITION",
            "REFLOW_ACCOUNT",
            "REFLOW_PYTHON",
            "REFLOW_MAIL_USER",
        ):
            monkeypatch.delenv(var, raising=False)
        cfg = Config()
        assert cfg.executor_partition is None
        assert cfg.executor_account is None
        assert cfg.mail_user is None
        assert cfg.executor_mode is None
        assert cfg.server_url is None

    def test_default_store_path(self) -> None:
        cfg = Config()
        assert "manifest.db" in cfg.default_store_path

    def test_executor_submit_options_from_config(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        for var in (
            "REFLOW_MODE",
            "REFLOW_PARTITION",
            "REFLOW_ACCOUNT",
            "REFLOW_SIGNAL",
            "REFLOW_MAIL_USER",
            "REFLOW_MAIL_TYPE",
        ):
            monkeypatch.delenv(var, raising=False)
        cfg = Config(
            {
                "executor": {"submit_options": {"partition": "gpu", "account": "acc"}},
                "notifications": {"mail_user": "u@e.org", "mail_type": "FAIL"},
            }
        )
        opts = cfg.executor_submit_options
        assert opts["partition"] == "gpu"
        assert opts["account"] == "acc"
        assert opts["mail_user"] == "u@e.org"

    def test_env_override_executor_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        cfg = Config()
        assert cfg.executor_mode == "dry-run"

    def test_env_override_python(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_PYTHON", "/custom/python")
        cfg = Config()
        assert cfg.executor_python == "/custom/python"


class TestConfigFileHelpers:
    def test_write_example_config_overwrite_false(self, tmp_path: Path) -> None:
        p = tmp_path / "config.toml"
        write_example_config(p)
        with pytest.raises(FileExistsError):
            write_example_config(p, overwrite=False)

    def test_write_example_config_overwrite_true(self, tmp_path: Path) -> None:
        p = tmp_path / "config.toml"
        write_example_config(p)
        write_example_config(p, overwrite=True)
        assert p.exists()

    def test_config_path_xdg(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("XDG_CONFIG_HOME", "/custom/config")
        assert config_path() == Path("/custom/config/reflow/config.toml")


class TestConfigWiring:
    def test_workflow_loads_config(self) -> None:
        wf = Workflow("test")
        assert wf.config is not None

    def test_config_fallback_in_resources(self, tmp_path: Path) -> None:
        """Config values fill in where decorator doesn't specify."""
        from reflow.config import Config

        cfg = Config(
            {
                "executor": {"submit_options": {"account": "default_account"}},
                "notifications": {"mail_user": "user@dkrz.de", "mail_type": "FAIL"},
            }
        )
        wf = Workflow("test", config=cfg)

        @wf.job()
        def task_a(run_dir: RunDir = RunDir()) -> str:
            return "ok"

        spec = wf.tasks["task_a"]
        res = wf._single_resources(tmp_path, spec)
        assert res.account == "default_account"
        assert res.mail_user == "user@dkrz.de"
        assert res.mail_type == "FAIL"

    def test_decorator_overrides_config(self, tmp_path: Path) -> None:
        """Decorator values take priority over config."""
        from reflow.config import Config

        cfg = Config(
            {
                "executor": {"submit_options": {"account": "default_account"}},
                "notifications": {"mail_user": "default@dkrz.de"},
            }
        )
        wf = Workflow("test", config=cfg)

        @wf.job(account="override_account", mail_user="override@dkrz.de")
        def task_a(run_dir: RunDir = RunDir()) -> str:
            return "ok"

        spec = wf.tasks["task_a"]
        res = wf._single_resources(tmp_path, spec)
        assert res.account == "override_account"
        assert res.mail_user == "override@dkrz.de"


class TestConfigHelpers:
    def test_ensure_config_exists(self, tmp_path: Path) -> None:
        path = ensure_config_exists(tmp_path / "config.toml")
        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert "[executor]" in content
        assert "# partition =" in content
