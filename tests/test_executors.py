"""Tests for multi-backend executor support.

Covers PBSExecutor, LSFExecutor, SGEExecutor, FluxExecutor,
the updated Executor ABC (dependency_options, _render_submit_option),
the extended resolve_executor / resolve_index helpers, and the
extended _rosetta_stone config translator.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from reflow import (
    Config,
    FluxExecutor,
    JobResources,
    LocalExecutor,
    LSFExecutor,
    PBSExecutor,
    SGEExecutor,
    SlurmExecutor,
)
from reflow.config import _rosetta_stone
from reflow.workflow._helpers import default_executor, resolve_executor, resolve_index

# ═══════════════════════════════════════════════════════════════════════════
# Executor ABC — base class features
# ═══════════════════════════════════════════════════════════════════════════


class TestExecutorBase:
    """Test features pulled up to the Executor ABC."""

    def test_default_dependency_options(self) -> None:
        """Default dependency_options produces Slurm-style string."""
        exc = LocalExecutor()
        opts = exc.dependency_options(["123", "456"])
        assert opts == {"dependency": "afterany:123:456"}

    def test_render_submit_option_inherited(self) -> None:
        """_render_submit_option is available on all executors."""
        exc = LocalExecutor()
        assert exc._render_submit_option("partition", "gpu") == [
            "--partition",
            "gpu",
        ]
        assert exc._render_submit_option("exclusive", True) == ["--exclusive"]
        assert exc._render_submit_option("key", None) == []
        assert exc._render_submit_option("key", False) == []

    def test_render_list_option(self) -> None:
        exc = LocalExecutor()
        result = exc._render_submit_option("exclude", ["n1", "n2"])
        assert result == ["--exclude", "n1", "--exclude", "n2"]

    def test_all_executors_have_required_attributes(self) -> None:
        for cls in (
            SlurmExecutor,
            PBSExecutor,
            LSFExecutor,
            SGEExecutor,
            FluxExecutor,
            LocalExecutor,
        ):
            assert hasattr(cls, "array_index_env_var")
            assert hasattr(cls, "_internal_keys")
            assert isinstance(cls._internal_keys, frozenset)

    def test_array_index_vars_are_distinct(self) -> None:
        seen: set[str] = set()
        for cls in (SlurmExecutor, PBSExecutor, LSFExecutor, SGEExecutor, FluxExecutor):
            var = cls.array_index_env_var
            assert var not in seen, f"Duplicate env var: {var}"
            seen.add(var)


# ═══════════════════════════════════════════════════════════════════════════
# PBSExecutor
# ═══════════════════════════════════════════════════════════════════════════


class TestPBSExecutor:
    def test_dry_run(self) -> None:
        exc = PBSExecutor(mode="dry-run")
        jid = exc.submit(JobResources(job_name="test"), ["echo", "hi"])
        assert jid == "DRYRUN"

    def test_cancel_dryrun(self) -> None:
        exc = PBSExecutor(mode="dry-run")
        exc.cancel("DRYRUN")  # no-op

    def test_job_state_dryrun(self) -> None:
        exc = PBSExecutor(mode="dry-run")
        assert exc.job_state("DRYRUN") is None

    def test_dependency_options(self) -> None:
        exc = PBSExecutor(mode="dry-run")
        opts = exc.dependency_options(["100", "200"])
        assert opts == {"depend": "afterany:100:200"}

    def test_array_index_var(self) -> None:
        assert PBSExecutor.array_index_env_var == "PBS_ARRAY_INDEX"

    def test_build_qsub_basic(self) -> None:
        exc = PBSExecutor(mode="dry-run", array_flag="-J")
        res = JobResources(
            job_name="myjob",
            cpus=4,
            time_limit="01:00:00",
            mem="8G",
        )
        cmd = exc._build_qsub(res, ["python", "worker.py"])
        assert cmd[0] == "qsub"
        assert "-N" in cmd and "myjob" in cmd
        assert "-l" in cmd
        assert "ncpus=4" in cmd
        assert "walltime=01:00:00" in cmd
        assert "mem=8G" in cmd
        assert "python" in cmd

    def test_build_qsub_array_pbs_pro(self) -> None:
        exc = PBSExecutor(mode="dry-run", array_flag="-J")
        res = JobResources(job_name="arr", array="0-9")
        cmd = exc._build_qsub(res, ["echo"])
        assert "-J" in cmd
        assert "0-9" in cmd

    def test_build_qsub_array_torque(self) -> None:
        exc = PBSExecutor(mode="dry-run", array_flag="-t")
        res = JobResources(job_name="arr", array="0-9")
        cmd = exc._build_qsub(res, ["echo"])
        assert "-t" in cmd
        assert "0-9" in cmd

    def test_build_qsub_output_error(self, tmp_path: Path) -> None:
        exc = PBSExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            output_path=tmp_path / "out.log",
            error_path=tmp_path / "err.log",
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-o" in cmd
        assert "-e" in cmd

    def test_build_qsub_queue_and_account(self) -> None:
        exc = PBSExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"queue": "batch", "account": "myproj"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-q" in cmd and "batch" in cmd
        assert "-A" in cmd and "myproj" in cmd

    def test_build_qsub_dependency(self) -> None:
        exc = PBSExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"depend": "afterany:100:200"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-W" in cmd
        idx_w = cmd.index("-W")
        assert cmd[idx_w + 1] == "depend=afterany:100:200"

    def test_internal_keys_filtered(self) -> None:
        exc = PBSExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"python": "/usr/bin/python", "qsub": "/usr/bin/qsub"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "--python" not in cmd
        assert "--qsub" not in cmd

    def test_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        monkeypatch.setenv("REFLOW_QSUB", "/custom/qsub")
        exc = PBSExecutor.from_environment()
        assert exc.mode == "dry-run"
        assert exc.qsub == "/custom/qsub"


# ═══════════════════════════════════════════════════════════════════════════
# LSFExecutor
# ═══════════════════════════════════════════════════════════════════════════


class TestLSFExecutor:
    def test_dry_run(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        jid = exc.submit(JobResources(job_name="test"), ["echo", "hi"])
        assert jid == "DRYRUN"

    def test_cancel_dryrun(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        exc.cancel("DRYRUN")

    def test_job_state_dryrun(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        assert exc.job_state("DRYRUN") is None

    def test_dependency_options(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        opts = exc.dependency_options(["100", "200"])
        assert opts == {"dependency_expr": "ended(100) && ended(200)"}

    def test_array_index_var(self) -> None:
        assert LSFExecutor.array_index_env_var == "LSB_JOBINDEX"

    def test_build_bsub_basic(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        res = JobResources(
            job_name="myjob",
            cpus=4,
            time_limit="01:00:00",
            mem="8G",
        )
        cmd = exc._build_bsub(res, ["python", "worker.py"])
        assert cmd[0] == "bsub"
        assert "-J" in cmd and "myjob" in cmd
        assert "-n" in cmd and "4" in cmd
        assert "-W" in cmd and "01:00:00" in cmd
        assert "-M" in cmd and "8G" in cmd

    def test_build_bsub_array(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        res = JobResources(job_name="arr", array="0-9")
        cmd = exc._build_bsub(res, ["echo"])
        # LSF array: -J "name[0-9]"
        idx_j = cmd.index("-J")
        assert "[0-9]" in cmd[idx_j + 1]

    def test_build_bsub_queue_and_account(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"queue": "normal", "account": "mygrp"},
        )
        cmd = exc._build_bsub(res, ["echo"])
        assert "-q" in cmd and "normal" in cmd
        assert "-G" in cmd and "mygrp" in cmd

    def test_build_bsub_dependency(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"dependency_expr": "ended(100) && ended(200)"},
        )
        cmd = exc._build_bsub(res, ["echo"])
        assert "-w" in cmd
        idx = cmd.index("-w")
        assert "ended(100)" in cmd[idx + 1]

    def test_build_bsub_output_error(self, tmp_path: Path) -> None:
        exc = LSFExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            output_path=tmp_path / "out.log",
            error_path=tmp_path / "err.log",
        )
        cmd = exc._build_bsub(res, ["echo"])
        assert "-o" in cmd
        assert "-e" in cmd

    def test_internal_keys_filtered(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"python": "/usr/bin/python", "bsub": "/usr/bin/bsub"},
        )
        cmd = exc._build_bsub(res, ["echo"])
        assert "--python" not in cmd
        assert "--bsub" not in cmd

    def test_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        monkeypatch.setenv("REFLOW_BSUB", "/custom/bsub")
        exc = LSFExecutor.from_environment()
        assert exc.mode == "dry-run"
        assert exc.bsub == "/custom/bsub"


# ═══════════════════════════════════════════════════════════════════════════
# SGEExecutor
# ═══════════════════════════════════════════════════════════════════════════


class TestSGEExecutor:
    def test_dry_run(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        jid = exc.submit(JobResources(job_name="test"), ["echo", "hi"])
        assert jid == "DRYRUN"

    def test_cancel_dryrun(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        exc.cancel("DRYRUN")

    def test_job_state_dryrun(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        assert exc.job_state("DRYRUN") is None

    def test_dependency_options(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        opts = exc.dependency_options(["100", "200"])
        assert opts == {"hold_jid": "100,200"}

    def test_array_index_var(self) -> None:
        assert SGEExecutor.array_index_env_var == "SGE_TASK_ID"

    def test_build_qsub_basic(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        res = JobResources(
            job_name="myjob",
            cpus=4,
            time_limit="01:00:00",
            mem="8G",
        )
        cmd = exc._build_qsub(res, ["python", "worker.py"])
        assert cmd[0] == "qsub"
        assert "-N" in cmd and "myjob" in cmd
        assert "-pe" in cmd and "smp" in cmd and "4" in cmd
        assert "h_rt=01:00:00" in cmd
        assert "h_vmem=8G" in cmd
        assert "-cwd" in cmd
        assert "-V" in cmd
        assert "-b" in cmd and "y" in cmd

    def test_build_qsub_array(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        res = JobResources(job_name="arr", array="0-9")
        cmd = exc._build_qsub(res, ["echo"])
        assert "-t" in cmd and "0-9" in cmd

    def test_build_qsub_hold_jid(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"hold_jid": "100,200"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-hold_jid" in cmd
        idx = cmd.index("-hold_jid")
        assert cmd[idx + 1] == "100,200"

    def test_build_qsub_queue_and_account(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"queue": "all.q", "account": "proj"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-q" in cmd and "all.q" in cmd
        assert "-A" in cmd and "proj" in cmd

    def test_build_qsub_output_error(self, tmp_path: Path) -> None:
        exc = SGEExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            output_path=tmp_path / "out.log",
            error_path=tmp_path / "err.log",
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-o" in cmd
        assert "-e" in cmd

    def test_slurm_dependency_translation(self) -> None:
        """Slurm-style dependency key is translated to -hold_jid."""
        exc = SGEExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"dependency": "afterany:100:200"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-hold_jid" in cmd
        idx = cmd.index("-hold_jid")
        assert cmd[idx + 1] == "100,200"

    def test_internal_keys_filtered(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"python": "/usr/bin/python"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "--python" not in cmd

    def test_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        monkeypatch.setenv("REFLOW_QSUB", "/custom/qsub")
        exc = SGEExecutor.from_environment()
        assert exc.mode == "dry-run"
        assert exc.qsub == "/custom/qsub"


# ═══════════════════════════════════════════════════════════════════════════
# FluxExecutor
# ═══════════════════════════════════════════════════════════════════════════


class TestFluxExecutor:
    def test_dry_run(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        jid = exc.submit(JobResources(job_name="test"), ["echo", "hi"])
        assert jid == "DRYRUN"

    def test_cancel_dryrun(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        exc.cancel("DRYRUN")

    def test_job_state_dryrun(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        assert exc.job_state("DRYRUN") is None

    def test_dependency_options(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        opts = exc.dependency_options(["f123", "f456"])
        assert opts == {"dependency": "afterany:f123,f456"}

    def test_array_index_var(self) -> None:
        assert FluxExecutor.array_index_env_var == "FLUX_JOB_CC"

    def test_build_submit_basic(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        res = JobResources(
            job_name="myjob",
            cpus=4,
            time_limit="01:00:00",
            mem="8G",
        )
        cmd = exc._build_submit(res, ["python", "worker.py"])
        assert cmd[0] == "flux" and cmd[1] == "submit"
        assert "--job-name" in cmd and "myjob" in cmd
        assert "--cores" in cmd and "4" in cmd
        assert "-t" in cmd and "01:00:00" in cmd
        # mem is set via --setattr
        mem_flags = [c for c in cmd if "alloc.mem" in c]
        assert len(mem_flags) == 1

    def test_build_submit_array(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        res = JobResources(job_name="arr", array="0-9")
        cmd = exc._build_submit(res, ["echo"])
        cc_flags = [c for c in cmd if c.startswith("--cc=")]
        assert len(cc_flags) == 1
        assert "0-9" in cc_flags[0]

    def test_build_submit_dependency(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"dependency": "afterany:f1,f2"},
        )
        cmd = exc._build_submit(res, ["echo"])
        dep_flags = [c for c in cmd if c.startswith("--dependency=")]
        assert len(dep_flags) == 1
        assert "afterany:f1,f2" in dep_flags[0]

    def test_build_submit_queue_and_account(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"queue": "batch", "account": "mybank"},
        )
        cmd = exc._build_submit(res, ["echo"])
        assert "--queue" in cmd and "batch" in cmd
        bank_flags = [c for c in cmd if "system.bank" in c]
        assert len(bank_flags) == 1

    def test_build_submit_output_error(self, tmp_path: Path) -> None:
        exc = FluxExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            output_path=tmp_path / "out.log",
            error_path=tmp_path / "err.log",
        )
        cmd = exc._build_submit(res, ["echo"])
        assert "--output" in cmd
        assert "--error" in cmd

    def test_internal_keys_filtered(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"python": "/usr/bin/python", "flux": "/usr/bin/flux"},
        )
        cmd = exc._build_submit(res, ["echo"])
        assert "--python" not in cmd
        assert "--flux" not in cmd

    def test_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        monkeypatch.setenv("REFLOW_FLUX", "/custom/flux")
        exc = FluxExecutor.from_environment()
        assert exc.mode == "dry-run"
        assert exc.flux == "/custom/flux"


# ═══════════════════════════════════════════════════════════════════════════
# resolve_executor — new shorthands
# ═══════════════════════════════════════════════════════════════════════════


class TestResolveExecutorExtended:
    def test_pbs(self) -> None:
        exc = resolve_executor("pbs")
        assert isinstance(exc, PBSExecutor)

    def test_lsf(self) -> None:
        exc = resolve_executor("lsf")
        assert isinstance(exc, LSFExecutor)

    def test_sge(self) -> None:
        exc = resolve_executor("sge")
        assert isinstance(exc, SGEExecutor)

    def test_flux(self) -> None:
        exc = resolve_executor("flux")
        assert isinstance(exc, FluxExecutor)

    def test_local(self) -> None:
        exc = resolve_executor("local")
        assert isinstance(exc, LocalExecutor)

    def test_case_insensitive(self) -> None:
        assert isinstance(resolve_executor("PBS"), PBSExecutor)
        assert isinstance(resolve_executor("Lsf"), LSFExecutor)

    def test_unknown(self) -> None:
        with pytest.raises(ValueError, match="Unknown executor"):
            resolve_executor("unicorn")

    def test_none_passthrough(self) -> None:
        assert resolve_executor(None) is None

    def test_instance_passthrough(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        assert resolve_executor(exc) is exc


# ═══════════════════════════════════════════════════════════════════════════
# resolve_index — multi-backend env vars
# ═══════════════════════════════════════════════════════════════════════════


class TestResolveIndexMultiBackend:
    def test_explicit_wins(self) -> None:
        assert resolve_index(42) == 42

    def test_slurm_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "3")
        assert resolve_index(None) == 3

    def test_pbs_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in ("SLURM_ARRAY_TASK_ID",):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("PBS_ARRAY_INDEX", "7")
        assert resolve_index(None) == 7

    def test_lsf_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in ("SLURM_ARRAY_TASK_ID", "PBS_ARRAY_INDEX", "PBS_ARRAYID"):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("LSB_JOBINDEX", "5")
        assert resolve_index(None) == 5

    def test_sge_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in (
            "SLURM_ARRAY_TASK_ID",
            "PBS_ARRAY_INDEX",
            "PBS_ARRAYID",
            "LSB_JOBINDEX",
        ):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("SGE_TASK_ID", "11")
        assert resolve_index(None) == 11

    def test_flux_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in (
            "SLURM_ARRAY_TASK_ID",
            "PBS_ARRAY_INDEX",
            "PBS_ARRAYID",
            "LSB_JOBINDEX",
            "SGE_TASK_ID",
        ):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("FLUX_JOB_CC", "2")
        assert resolve_index(None) == 2

    def test_no_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in (
            "SLURM_ARRAY_TASK_ID",
            "PBS_ARRAY_INDEX",
            "PBS_ARRAYID",
            "LSB_JOBINDEX",
            "SGE_TASK_ID",
            "FLUX_JOB_CC",
            "OAR_ARRAY_INDEX",
        ):
            monkeypatch.delenv(var, raising=False)
        assert resolve_index(None) is None

    def test_invalid_value_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Non-integer env var is skipped."""
        for var in ("SLURM_ARRAY_TASK_ID",):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("PBS_ARRAY_INDEX", "not_a_number")
        monkeypatch.setenv("LSB_JOBINDEX", "5")
        # PBS var is invalid, falls through to LSF
        assert resolve_index(None) == 5

    def test_priority_slurm_over_pbs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Slurm var takes priority when both are set."""
        monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "1")
        monkeypatch.setenv("PBS_ARRAY_INDEX", "2")
        assert resolve_index(None) == 1


# ═══════════════════════════════════════════════════════════════════════════
# _rosetta_stone — extended backends
# ═══════════════════════════════════════════════════════════════════════════


class TestRosettaStoneExtended:
    def test_sbatch_backend(self) -> None:
        assert _rosetta_stone("sbatch", "partition") == "partition"
        assert _rosetta_stone("sbatch", "account") == "account"

    def test_dry_run_maps_to_sbatch(self) -> None:
        assert _rosetta_stone("dry-run", "partition") == "partition"

    def test_pbs_backend(self) -> None:
        assert _rosetta_stone("qsub-pbs", "partition") == "queue"
        assert _rosetta_stone("qsub-pbs", "account") == "account"

    def test_lsf_backend(self) -> None:
        assert _rosetta_stone("bsub", "partition") == "queue"
        assert _rosetta_stone("bsub", "account") == "account"

    def test_sge_backend(self) -> None:
        assert _rosetta_stone("qsub-sge", "partition") == "queue"
        assert _rosetta_stone("qsub-sge", "account") == "account"

    def test_flux_backend(self) -> None:
        assert _rosetta_stone("flux", "partition") == "queue"
        assert _rosetta_stone("flux", "account") == "account"

    def test_unknown_backend_raises(self) -> None:
        with pytest.raises(KeyError, match="Unknown workload manager"):
            _rosetta_stone("unicorn", "partition")

    def test_unknown_key_raises(self) -> None:
        with pytest.raises(KeyError, match="Unknown config key"):
            _rosetta_stone("sbatch", "nonexistent_key")


# ═══════════════════════════════════════════════════════════════════════════
# SlurmExecutor — verify _render_submit_option now inherited
# ═══════════════════════════════════════════════════════════════════════════


class TestSlurmExecutorRefactored:
    def test_render_inherited(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        assert exc._render_submit_option("partition", "gpu") == [
            "--partition",
            "gpu",
        ]

    def test_sbatch_still_works(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"partition": "compute"},
        )
        cmd = exc._build_sbatch(res, ["echo"])
        assert "--partition" in cmd
        assert "compute" in cmd
        assert "--wrap" in cmd

    def test_dependency_options_slurm(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        opts = exc.dependency_options(["123", "456"])
        assert opts == {"dependency": "afterany:123:456"}

    def test_internal_keys_include_slurm_commands(self) -> None:
        assert "sbatch" in SlurmExecutor._internal_keys
        assert "scancel" in SlurmExecutor._internal_keys
        assert "sacct" in SlurmExecutor._internal_keys


# ═══════════════════════════════════════════════════════════════════════════
# Cross-scheduler key normalisation
# ═══════════════════════════════════════════════════════════════════════════


class TestKeyNormalization:
    """Users write scheduler-agnostic config and it works on any backend."""

    # --- Slurm: "queue" → "partition" ---

    def test_slurm_queue_becomes_partition(self) -> None:
        """PBS user's queue=batch becomes --partition batch on Slurm."""
        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"queue": "batch"},
        )
        cmd = exc._build_sbatch(res, ["echo"])
        assert "--partition" in cmd and "batch" in cmd
        assert "--queue" not in cmd

    def test_slurm_partition_still_works(self) -> None:
        """Native partition=gpu still works on Slurm."""
        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"partition": "gpu"},
        )
        cmd = exc._build_sbatch(res, ["echo"])
        assert "--partition" in cmd and "gpu" in cmd

    def test_slurm_native_wins_over_alias(self) -> None:
        """If both partition and queue are set, partition wins."""
        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"partition": "gpu", "queue": "batch"},
        )
        cmd = exc._build_sbatch(res, ["echo"])
        # partition (native) should appear, queue (alias) should not
        idx = cmd.index("--partition")
        assert cmd[idx + 1] == "gpu"
        assert cmd.count("--partition") == 1

    # --- PBS: "partition" → "queue" ---

    def test_pbs_partition_becomes_queue(self) -> None:
        """Slurm user's partition=compute becomes -q compute on PBS."""
        exc = PBSExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"partition": "compute"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-q" in cmd and "compute" in cmd
        assert "--partition" not in cmd

    def test_pbs_queue_still_works(self) -> None:
        """Native queue=batch still works on PBS."""
        exc = PBSExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"queue": "batch"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-q" in cmd and "batch" in cmd

    def test_pbs_native_wins_over_alias(self) -> None:
        """If both queue and partition are set, queue wins."""
        exc = PBSExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"queue": "batch", "partition": "compute"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        idx = cmd.index("-q")
        assert cmd[idx + 1] == "batch"
        assert cmd.count("-q") == 1

    # --- LSF: "partition" → "queue" ---

    def test_lsf_partition_becomes_queue(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"partition": "normal"},
        )
        cmd = exc._build_bsub(res, ["echo"])
        assert "-q" in cmd and "normal" in cmd
        assert "--partition" not in cmd

    def test_lsf_queue_still_works(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"queue": "normal"},
        )
        cmd = exc._build_bsub(res, ["echo"])
        assert "-q" in cmd and "normal" in cmd

    # --- SGE: "partition" → "queue" ---

    def test_sge_partition_becomes_queue(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"partition": "all.q"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-q" in cmd and "all.q" in cmd
        assert "--partition" not in cmd

    def test_sge_queue_still_works(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"queue": "all.q"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-q" in cmd and "all.q" in cmd

    # --- Flux: "partition" → "queue" ---

    def test_flux_partition_becomes_queue(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"partition": "batch"},
        )
        cmd = exc._build_submit(res, ["echo"])
        assert "--queue" in cmd and "batch" in cmd
        assert "--partition" not in cmd

    def test_flux_queue_still_works(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"queue": "batch"},
        )
        cmd = exc._build_submit(res, ["echo"])
        assert "--queue" in cmd and "batch" in cmd

    # --- account works on all backends ---

    def test_account_on_slurm(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"account": "myproj"},
        )
        cmd = exc._build_sbatch(res, ["echo"])
        assert "--account" in cmd and "myproj" in cmd

    def test_account_on_pbs(self) -> None:
        exc = PBSExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"account": "myproj"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-A" in cmd and "myproj" in cmd

    def test_account_on_lsf(self) -> None:
        exc = LSFExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"account": "mygrp"},
        )
        cmd = exc._build_bsub(res, ["echo"])
        assert "-G" in cmd and "mygrp" in cmd

    def test_account_on_sge(self) -> None:
        exc = SGEExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"account": "myproj"},
        )
        cmd = exc._build_qsub(res, ["echo"])
        assert "-A" in cmd and "myproj" in cmd

    def test_account_on_flux(self) -> None:
        exc = FluxExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"account": "mybank"},
        )
        cmd = exc._build_submit(res, ["echo"])
        bank_flags = [c for c in cmd if "system.bank=mybank" in c]
        assert len(bank_flags) == 1

    # --- _normalize_options unit tests ---

    def test_normalize_strips_internal_keys(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        result = exc._normalize_options(
            {
                "python": "/usr/bin/python",
                "partition": "gpu",
                "sbatch": "/usr/bin/sbatch",
            }
        )
        assert "python" not in result
        assert "sbatch" not in result
        assert result["partition"] == "gpu"

    def test_normalize_alias_maps(self) -> None:
        exc = PBSExecutor(mode="dry-run")
        result = exc._normalize_options({"partition": "compute", "signal": "USR1"})
        assert result == {"queue": "compute", "signal": "USR1"}

    def test_normalize_native_wins(self) -> None:
        exc = PBSExecutor(mode="dry-run")
        result = exc._normalize_options({"queue": "batch", "partition": "compute"})
        # "queue" is native, "partition" is alias → native wins
        assert result["queue"] == "batch"
        assert "partition" not in result

    def test_normalize_no_aliases_passthrough(self) -> None:
        exc = LocalExecutor()
        result = exc._normalize_options(
            {"partition": "gpu", "account": "proj", "python": "/usr/bin/python"}
        )
        assert result == {"partition": "gpu", "account": "proj"}


# ═══════════════════════════════════════════════════════════════════════════
# _default_executor — config-driven backend selection
# ═══════════════════════════════════════════════════════════════════════════


class TestDefaultExecutor:
    """_default_executor reads mode from config/env to pick the backend."""

    def test_sbatch_gives_slurm(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "sbatch")
        exc = default_executor(Config())
        assert isinstance(exc, SlurmExecutor)

    def test_dry_run_gives_slurm(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        exc = default_executor(Config())
        assert isinstance(exc, SlurmExecutor)

    def test_empty_gives_slurm(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("REFLOW_MODE", raising=False)
        exc = default_executor(Config())
        assert isinstance(exc, SlurmExecutor)

    def test_qsub_pbs_gives_pbs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "qsub-pbs")
        exc = default_executor(Config())
        assert isinstance(exc, PBSExecutor)

    def test_qsub_shorthand_gives_pbs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "qsub")
        exc = default_executor(Config())
        assert isinstance(exc, PBSExecutor)

    def test_bsub_gives_lsf(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "bsub")
        exc = default_executor(Config())
        assert isinstance(exc, LSFExecutor)

    def test_qsub_sge_gives_sge(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "qsub-sge")
        exc = default_executor(Config())
        assert isinstance(exc, SGEExecutor)

    def test_flux_gives_flux(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "flux")
        exc = default_executor(Config())
        assert isinstance(exc, FluxExecutor)

    def test_config_mode_used_when_no_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("REFLOW_MODE", raising=False)
        cfg = Config({"executor": {"mode": "bsub"}})
        exc = default_executor(cfg)
        assert isinstance(exc, LSFExecutor)

    def test_env_overrides_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "flux")
        cfg = Config({"executor": {"mode": "bsub"}})
        exc = default_executor(cfg)
        assert isinstance(exc, FluxExecutor)
