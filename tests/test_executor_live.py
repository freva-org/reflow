"""test_executor_live.py — mocked live paths for all executor backends.

Covers submit(), cancel(), and job_state() branches that require
a real scheduler binary, by patching run_cmd at the executor module level.
These are deliberately separate from test_executors.py which focuses on
command-building and dry-run behaviour.
"""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from reflow.executors import JobResources
from reflow.executors.flux import FluxExecutor
from reflow.executors.lsf import LSFExecutor
from reflow.executors.pbs import PBSExecutor, _detect_pbs_variant
from reflow.executors.sge import SGEExecutor
from reflow.executors.slurm import SlurmExecutor
from reflow.executors.util import CommandResult, ExecutorError


def _ok(stdout: str = "", stderr: str = "") -> CommandResult:
    return CommandResult(cmd=[], stdout=stdout, stderr=stderr, returncode=0)


def _fail(stderr: str = "error") -> CommandResult:
    return CommandResult(cmd=[], stdout="", stderr=stderr, returncode=1)


# ═══════════════════════════════════════════════════════════════════════════
# Slurm live paths
# ═══════════════════════════════════════════════════════════════════════════


class TestSlurmLive:
    def test_submit_live_returns_job_id(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        res = JobResources(job_name="test")
        with patch("reflow.executors.slurm.run_cmd", return_value=_ok("67890")) as m:
            jid = exc.submit(res, ["python", "w.py"])
        assert jid == "67890"
        assert m.called

    def test_cancel_live_calls_scancel(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        with patch("reflow.executors.slurm.run_cmd", return_value=_ok()) as m:
            exc.cancel("12345")
        assert m.called
        cmd = m.call_args[0][0]
        assert "scancel" in cmd
        assert "12345" in cmd

    def test_cancel_logs_warning_on_error(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        err_result = _fail("no such job")
        with patch(
            "reflow.executors.slurm.run_cmd",
            side_effect=ExecutorError(err_result),
        ):
            exc.cancel("99999")  # must not raise

    def test_job_state_running(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        with patch(
            "reflow.executors.slurm.run_cmd",
            return_value=_ok("RUNNING"),
        ):
            assert exc.job_state("12345") == "RUNNING"

    def test_job_state_empty_output(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        with patch("reflow.executors.slurm.run_cmd", return_value=_ok("")):
            assert exc.job_state("12345") is None

    def test_job_state_executor_error_returns_none(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        with patch(
            "reflow.executors.slurm.run_cmd",
            side_effect=ExecutorError(_fail()),
        ):
            assert exc.job_state("12345") is None


# ═══════════════════════════════════════════════════════════════════════════
# PBS live paths
# ═══════════════════════════════════════════════════════════════════════════


class TestPBSLive:
    def test_submit_live_parses_job_id(self) -> None:
        exc = PBSExecutor(mode="qsub", array_flag="-J")
        res = JobResources(job_name="test")
        with patch("reflow.executors.pbs.run_cmd", return_value=_ok("12345.server")):
            jid = exc.submit(res, ["python", "w.py"])
        assert jid == "12345"

    def test_cancel_live_calls_qdel(self) -> None:
        exc = PBSExecutor(mode="qsub", array_flag="-J")
        with patch("reflow.executors.pbs.run_cmd", return_value=_ok()) as m:
            exc.cancel("12345")
        cmd = m.call_args[0][0]
        assert "qdel" in cmd
        assert "12345" in cmd

    def test_cancel_logs_warning_on_error(self) -> None:
        exc = PBSExecutor(mode="qsub", array_flag="-J")
        with patch(
            "reflow.executors.pbs.run_cmd",
            side_effect=ExecutorError(_fail()),
        ):
            exc.cancel("99999")  # must not raise

    def test_job_state_running(self) -> None:
        exc = PBSExecutor(mode="qsub", array_flag="-J")
        qstat_out = "Job Id: 12345\n    job_state = R\n"
        with patch("reflow.executors.pbs.run_cmd", return_value=_ok(qstat_out)):
            assert exc.job_state("12345") == "RUNNING"

    def test_job_state_pending(self) -> None:
        exc = PBSExecutor(mode="qsub", array_flag="-J")
        with patch(
            "reflow.executors.pbs.run_cmd",
            return_value=_ok("job_state = Q"),
        ):
            assert exc.job_state("12345") == "PENDING"

    def test_job_state_completed(self) -> None:
        exc = PBSExecutor(mode="qsub", array_flag="-J")
        with patch(
            "reflow.executors.pbs.run_cmd",
            return_value=_ok("job_state = F"),
        ):
            assert exc.job_state("12345") == "COMPLETED"

    def test_job_state_unknown_state_passthrough(self) -> None:
        exc = PBSExecutor(mode="qsub", array_flag="-J")
        with patch(
            "reflow.executors.pbs.run_cmd",
            return_value=_ok("job_state = X"),
        ):
            assert exc.job_state("12345") == "X"

    def test_job_state_no_match_returns_none(self) -> None:
        exc = PBSExecutor(mode="qsub", array_flag="-J")
        with patch("reflow.executors.pbs.run_cmd", return_value=_ok("no state here")):
            assert exc.job_state("12345") is None

    def test_job_state_error_returns_none(self) -> None:
        exc = PBSExecutor(mode="qsub", array_flag="-J")
        with patch(
            "reflow.executors.pbs.run_cmd",
            side_effect=ExecutorError(_fail()),
        ):
            assert exc.job_state("12345") is None

    def test_job_state_file_not_found_returns_none(self) -> None:
        exc = PBSExecutor(mode="qsub", array_flag="-J")
        with patch(
            "reflow.executors.pbs.run_cmd",
            side_effect=FileNotFoundError,
        ):
            assert exc.job_state("12345") is None

    def test_detect_pbs_variant_pro(self) -> None:
        with patch(
            "reflow.executors.pbs.run_cmd",
            return_value=_ok("PBS Pro 22.05"),
        ):
            assert _detect_pbs_variant() == "-J"

    def test_detect_pbs_variant_openpbs(self) -> None:
        with patch(
            "reflow.executors.pbs.run_cmd",
            return_value=_ok("OpenPBS 23.06"),
        ):
            assert _detect_pbs_variant() == "-J"

    def test_detect_pbs_variant_torque(self) -> None:
        with patch(
            "reflow.executors.pbs.run_cmd",
            return_value=_ok("Torque 6.1"),
        ):
            assert _detect_pbs_variant() == "-t"

    def test_detect_pbs_variant_error_defaults_to_pro(self) -> None:
        with patch(
            "reflow.executors.pbs.run_cmd",
            side_effect=ExecutorError(_fail()),
        ):
            assert _detect_pbs_variant() == "-J"

    def test_detect_pbs_variant_not_found_defaults_to_pro(self) -> None:
        with patch(
            "reflow.executors.pbs.run_cmd",
            side_effect=FileNotFoundError,
        ):
            assert _detect_pbs_variant() == "-J"


# ═══════════════════════════════════════════════════════════════════════════
# LSF live paths
# ═══════════════════════════════════════════════════════════════════════════


class TestLSFLive:
    def test_submit_live_parses_job_id(self) -> None:
        exc = LSFExecutor(mode="bsub")
        res = JobResources(job_name="test")
        with patch(
            "reflow.executors.lsf.run_cmd",
            return_value=_ok("Job <67890> is submitted to queue <normal>."),
        ):
            jid = exc.submit(res, ["python", "w.py"])
        assert jid == "67890"

    def test_submit_live_unparseable_returns_raw(self) -> None:
        exc = LSFExecutor(mode="bsub")
        res = JobResources(job_name="test")
        with patch(
            "reflow.executors.lsf.run_cmd",
            return_value=_ok("something unexpected"),
        ):
            jid = exc.submit(res, ["python", "w.py"])
        assert jid == "something unexpected"

    def test_cancel_live_calls_bkill(self) -> None:
        exc = LSFExecutor(mode="bsub")
        with patch("reflow.executors.lsf.run_cmd", return_value=_ok()) as m:
            exc.cancel("67890")
        cmd = m.call_args[0][0]
        assert "bkill" in cmd
        assert "67890" in cmd

    def test_cancel_logs_warning_on_error(self) -> None:
        exc = LSFExecutor(mode="bsub")
        with patch(
            "reflow.executors.lsf.run_cmd",
            side_effect=ExecutorError(_fail()),
        ):
            exc.cancel("99999")  # must not raise

    def test_job_state_running(self) -> None:
        exc = LSFExecutor(mode="bsub")
        with patch("reflow.executors.lsf.run_cmd", return_value=_ok("RUN")):
            assert exc.job_state("67890") == "RUNNING"

    def test_job_state_done(self) -> None:
        exc = LSFExecutor(mode="bsub")
        with patch("reflow.executors.lsf.run_cmd", return_value=_ok("DONE")):
            assert exc.job_state("67890") == "COMPLETED"

    def test_job_state_exit(self) -> None:
        exc = LSFExecutor(mode="bsub")
        with patch("reflow.executors.lsf.run_cmd", return_value=_ok("EXIT")):
            assert exc.job_state("67890") == "FAILED"

    def test_job_state_empty_output_returns_none(self) -> None:
        exc = LSFExecutor(mode="bsub")
        with patch("reflow.executors.lsf.run_cmd", return_value=_ok("")):
            assert exc.job_state("67890") is None

    def test_job_state_error_returns_none(self) -> None:
        exc = LSFExecutor(mode="bsub")
        with patch(
            "reflow.executors.lsf.run_cmd",
            side_effect=ExecutorError(_fail()),
        ):
            assert exc.job_state("67890") is None

    def test_job_state_file_not_found_returns_none(self) -> None:
        exc = LSFExecutor(mode="bsub")
        with patch(
            "reflow.executors.lsf.run_cmd",
            side_effect=FileNotFoundError,
        ):
            assert exc.job_state("67890") is None


# ═══════════════════════════════════════════════════════════════════════════
# SGE live paths
# ═══════════════════════════════════════════════════════════════════════════


class TestSGELive:
    def test_submit_live_parses_job_id(self) -> None:
        exc = SGEExecutor(mode="qsub")
        res = JobResources(job_name="test")
        with patch(
            "reflow.executors.sge.run_cmd",
            return_value=_ok('Your job 54321 ("test") has been submitted'),
        ):
            jid = exc.submit(res, ["python", "w.py"])
        assert jid == "54321"

    def test_submit_live_array_parses_job_id(self) -> None:
        exc = SGEExecutor(mode="qsub")
        res = JobResources(job_name="test")
        with patch(
            "reflow.executors.sge.run_cmd",
            return_value=_ok('Your job-array 54321.1-10:1 ("test") has been submitted'),
        ):
            jid = exc.submit(res, ["python", "w.py"])
        assert jid == "54321"

    def test_submit_live_unparseable_returns_first_word(self) -> None:
        exc = SGEExecutor(mode="qsub")
        res = JobResources(job_name="test")
        with patch(
            "reflow.executors.sge.run_cmd",
            return_value=_ok("unexpected output here"),
        ):
            jid = exc.submit(res, ["python", "w.py"])
        assert jid == "unexpected"

    def test_cancel_live_calls_qdel(self) -> None:
        exc = SGEExecutor(mode="qsub")
        with patch("reflow.executors.sge.run_cmd", return_value=_ok()) as m:
            exc.cancel("54321")
        cmd = m.call_args[0][0]
        assert "qdel" in cmd
        assert "54321" in cmd

    def test_cancel_logs_warning_on_error(self) -> None:
        exc = SGEExecutor(mode="qsub")
        with patch(
            "reflow.executors.sge.run_cmd",
            side_effect=ExecutorError(_fail()),
        ):
            exc.cancel("99999")  # must not raise

    def test_job_state_running(self) -> None:
        exc = SGEExecutor(mode="qsub")
        qstat_out = "job_state         1: r\n"
        with patch("reflow.executors.sge.run_cmd", return_value=_ok(qstat_out)):
            assert exc.job_state("54321") == "RUNNING"

    def test_job_state_pending(self) -> None:
        exc = SGEExecutor(mode="qsub")
        with patch(
            "reflow.executors.sge.run_cmd",
            return_value=_ok("job_state         1: qw\n"),
        ):
            assert exc.job_state("54321") == "PENDING"

    def test_job_state_error_state(self) -> None:
        """Pure error state 'E' (no q/w/r) maps to FAILED."""
        exc = SGEExecutor(mode="qsub")
        with patch(
            "reflow.executors.sge.run_cmd",
            return_value=_ok("job_state         1: E\n"),
        ):
            assert exc.job_state("54321") == "FAILED"

    def test_job_state_eqw_returns_pending(self) -> None:
        """Eqw contains 'q' so is classified as PENDING (error-in-queue)."""
        exc = SGEExecutor(mode="qsub")
        with patch(
            "reflow.executors.sge.run_cmd",
            return_value=_ok("job_state         1: Eqw\n"),
        ):
            assert exc.job_state("54321") == "PENDING"

    def test_job_state_no_match_defaults_running(self) -> None:
        """Still in qstat but no parseable state → assume running."""
        exc = SGEExecutor(mode="qsub")
        with patch("reflow.executors.sge.run_cmd", return_value=_ok("some output")):
            assert exc.job_state("54321") == "RUNNING"

    def test_job_state_executor_error_falls_back_to_qacct(self) -> None:
        exc = SGEExecutor(mode="qsub")
        qacct_out = "exit_status    0\n"
        with patch(
            "reflow.executors.sge.run_cmd",
            side_effect=[
                ExecutorError(_fail()),  # qstat fails
                _ok(qacct_out),          # qacct succeeds
            ],
        ):
            assert exc.job_state("54321") == "COMPLETED"

    def test_job_state_qacct_failed_exit(self) -> None:
        exc = SGEExecutor(mode="qsub")
        with patch(
            "reflow.executors.sge.run_cmd",
            side_effect=[
                ExecutorError(_fail()),
                _ok("exit_status    1\n"),
            ],
        ):
            assert exc.job_state("54321") == "FAILED"

    def test_job_state_qacct_no_exit_status_returns_completed(self) -> None:
        exc = SGEExecutor(mode="qsub")
        with patch(
            "reflow.executors.sge.run_cmd",
            side_effect=[
                ExecutorError(_fail()),
                _ok("owner    user\nhostname  node1\n"),
            ],
        ):
            assert exc.job_state("54321") == "COMPLETED"

    def test_job_state_file_not_found_returns_none(self) -> None:
        exc = SGEExecutor(mode="qsub")
        with patch(
            "reflow.executors.sge.run_cmd",
            side_effect=FileNotFoundError,
        ):
            assert exc.job_state("54321") is None

    def test_check_qacct_error_returns_none(self) -> None:
        exc = SGEExecutor(mode="qsub")
        with patch(
            "reflow.executors.sge.run_cmd",
            side_effect=[
                ExecutorError(_fail()),   # qstat
                ExecutorError(_fail()),   # qacct
            ],
        ):
            assert exc.job_state("54321") is None


# ═══════════════════════════════════════════════════════════════════════════
# Flux live paths
# ═══════════════════════════════════════════════════════════════════════════


class TestFluxLive:
    def test_submit_live_returns_job_id(self) -> None:
        exc = FluxExecutor(mode="flux")
        res = JobResources(job_name="test")
        with patch(
            "reflow.executors.flux.run_cmd",
            return_value=_ok("f123abc456"),
        ):
            jid = exc.submit(res, ["python", "w.py"])
        assert jid == "f123abc456"

    def test_submit_live_multiline_takes_last(self) -> None:
        """flux submit may print info before the job ID."""
        exc = FluxExecutor(mode="flux")
        res = JobResources(job_name="test")
        with patch(
            "reflow.executors.flux.run_cmd",
            return_value=_ok("note: something\nf123abc456"),
        ):
            jid = exc.submit(res, ["python", "w.py"])
        assert jid == "f123abc456"

    def test_cancel_live_calls_flux_cancel(self) -> None:
        exc = FluxExecutor(mode="flux")
        with patch("reflow.executors.flux.run_cmd", return_value=_ok()) as m:
            exc.cancel("f123abc456")
        cmd = m.call_args[0][0]
        assert "cancel" in cmd
        assert "f123abc456" in cmd

    def test_cancel_logs_warning_on_error(self) -> None:
        exc = FluxExecutor(mode="flux")
        with patch(
            "reflow.executors.flux.run_cmd",
            side_effect=ExecutorError(_fail()),
        ):
            exc.cancel("f123")  # must not raise

    def test_job_state_running(self) -> None:
        exc = FluxExecutor(mode="flux")
        with patch("reflow.executors.flux.run_cmd", return_value=_ok("R")):
            assert exc.job_state("f123") == "RUNNING"

    def test_job_state_completed(self) -> None:
        exc = FluxExecutor(mode="flux")
        with patch("reflow.executors.flux.run_cmd", return_value=_ok("CD")):
            assert exc.job_state("f123") == "COMPLETED"

    def test_job_state_failed(self) -> None:
        exc = FluxExecutor(mode="flux")
        with patch("reflow.executors.flux.run_cmd", return_value=_ok("F")):
            assert exc.job_state("f123") == "FAILED"

    def test_job_state_unknown_passthrough(self) -> None:
        exc = FluxExecutor(mode="flux")
        with patch("reflow.executors.flux.run_cmd", return_value=_ok("XY")):
            assert exc.job_state("f123") == "XY"

    def test_job_state_empty_returns_none(self) -> None:
        exc = FluxExecutor(mode="flux")
        with patch("reflow.executors.flux.run_cmd", return_value=_ok("")):
            assert exc.job_state("f123") is None

    def test_job_state_error_returns_none(self) -> None:
        exc = FluxExecutor(mode="flux")
        with patch(
            "reflow.executors.flux.run_cmd",
            side_effect=ExecutorError(_fail()),
        ):
            assert exc.job_state("f123") is None

    def test_job_state_file_not_found_returns_none(self) -> None:
        exc = FluxExecutor(mode="flux")
        with patch(
            "reflow.executors.flux.run_cmd",
            side_effect=FileNotFoundError,
        ):
            assert exc.job_state("f123") is None
