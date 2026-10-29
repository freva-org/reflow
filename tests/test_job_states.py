"""Normalised, per-element job states (``Executor.job_states``)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from reflow.executors import ElementState, JobStates, JobStatus
from reflow.executors.local import LocalExecutor
from reflow.executors.pbs import PBSExecutor
from reflow.executors.slurm import (
    SlurmExecutor,
    _expand_indices,
    _normalise_state,
    _parse_job_lines,
)
from reflow.executors.util import CommandResult, ExecutorError


def _result(stdout: str = "", stderr: str = "", code: int = 0) -> CommandResult:
    return CommandResult(cmd=[], stdout=stdout, stderr=stderr, returncode=code)


class TestSlurmParsing:
    @pytest.mark.parametrize(
        ("raw", "status", "reason"),
        [
            ("PENDING", JobStatus.QUEUED, "PENDING"),
            ("REQUEUED", JobStatus.QUEUED, "REQUEUED"),
            ("RUNNING", JobStatus.RUNNING, "RUNNING"),
            ("COMPLETING", JobStatus.RUNNING, "COMPLETING"),
            ("COMPLETED", JobStatus.ENDED, "COMPLETED"),
            ("OUT_OF_MEMORY", JobStatus.ENDED, "OUT_OF_MEMORY"),
            ("CANCELLED by 12345", JobStatus.ENDED, "CANCELLED"),
            ("CANCELLED+", JobStatus.ENDED, "CANCELLED"),
            ("TIMEOUT", JobStatus.ENDED, "TIMEOUT"),
            ("NODE_FAIL", JobStatus.ENDED, "NODE_FAIL"),
            ("SOMETHING_NEW", JobStatus.UNKNOWN, "SOMETHING_NEW"),
        ],
    )
    def test_normalise(self, raw: str, status: JobStatus, reason: str) -> None:
        assert _normalise_state(raw) == ElementState(status, reason)

    @pytest.mark.parametrize(
        ("spec", "indices"),
        [
            ("5", [5]),
            ("[4-6]", [4, 5, 6]),
            ("[0-3%2]", [0, 1, 2, 3]),
            ("[1,3,7-8]", [1, 3, 7, 8]),
        ],
    )
    def test_expand(self, spec: str, indices: list[int]) -> None:
        assert _expand_indices(spec) == indices

    def test_parse_array_with_pending_range_and_steps(self) -> None:
        text = "\n".join(
            [
                "123_0|COMPLETED",
                "123_0.batch|COMPLETED",
                "123_1|OUT_OF_MEMORY",
                "123_2|RUNNING",
                "123_[3-5%2]|PENDING",
                "999_0|FAILED",
            ]
        )
        elements = _parse_job_lines(text, "123")
        assert elements[0] == ElementState(JobStatus.ENDED, "COMPLETED")
        assert elements[1] == ElementState(JobStatus.ENDED, "OUT_OF_MEMORY")
        assert elements[2].status is JobStatus.RUNNING
        assert {i for i, s in elements.items() if s.status is JobStatus.QUEUED} == {
            3,
            4,
            5,
        }
        assert set(elements) == {0, 1, 2, 3, 4, 5}

    def test_parse_plain_job(self) -> None:
        assert _parse_job_lines("77|TIMEOUT", "77") == {
            None: ElementState(JobStatus.ENDED, "TIMEOUT")
        }


class TestSlurmJobStates:
    def test_sacct(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        out = _result("123_0|COMPLETED\n123_1|RUNNING")
        with patch("reflow.executors.slurm.run_cmd", return_value=out) as run:
            states = exc.job_states("123")
        assert run.call_args[0][0][0] == "sacct"
        assert "--allocations" in run.call_args[0][0]
        assert states.get(0).reason == "COMPLETED"
        assert states.get(1).active
        assert states.get(7).status is JobStatus.UNKNOWN  # not listed by sacct

    def test_cluster_suffix_is_stripped(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        with patch(
            "reflow.executors.slurm.run_cmd", return_value=_result("123|RUNNING")
        ) as run:
            assert exc.job_states("123;levante").get(None).active
        assert "123" in run.call_args[0][0]

    def test_squeue_fallback_when_sacct_fails(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        calls = [
            ExecutorError(_result(stderr="accounting disabled", code=1)),
            _result("123_4|RUNNING"),
        ]
        with patch("reflow.executors.slurm.run_cmd", side_effect=calls) as run:
            states = exc.job_states("123")
        assert run.call_args[0][0][0] == "squeue"
        assert states.get(4).active
        assert states.get(5) == ElementState(JobStatus.ENDED)  # no longer queued

    def test_squeue_fallback_when_sacct_lags(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        calls = [_result(""), _result("")]
        with patch("reflow.executors.slurm.run_cmd", side_effect=calls):
            states = exc.job_states("123")
        assert states.get(0).status is JobStatus.ENDED

    def test_invalid_job_id_means_ended(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        calls = [
            _result(""),
            _result(stderr="slurm_load_jobs error: Invalid job id specified", code=1),
        ]
        with patch("reflow.executors.slurm.run_cmd", side_effect=calls):
            assert exc.job_states("123").get(3).status is JobStatus.ENDED

    def test_other_squeue_errors_are_unknown(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        calls = [_result(""), _result(stderr="Unable to contact controller", code=1)]
        with patch("reflow.executors.slurm.run_cmd", side_effect=calls):
            assert exc.job_states("123").get(3).status is JobStatus.UNKNOWN

    def test_missing_binaries_are_unknown(self) -> None:
        exc = SlurmExecutor(mode="sbatch")
        with patch("reflow.executors.slurm.run_cmd", side_effect=OSError("no sacct")):
            assert exc.job_states("123").get(0).status is JobStatus.UNKNOWN

    def test_dry_run_is_unknown(self) -> None:
        assert SlurmExecutor(mode="dry-run").job_states("DRYRUN") == JobStates()


class TestOtherBackends:
    def test_local_jobs_have_always_ended(self) -> None:
        exc = LocalExecutor()
        assert exc.job_states("local-unknown").get(None).status is JobStatus.ENDED

    def test_backends_without_support_are_unknown(self) -> None:
        exc = PBSExecutor(mode="qsub")
        assert exc.job_states("1.server").get(0).status is JobStatus.UNKNOWN

    def test_non_array_listing_covers_all_indices(self) -> None:
        states = JobStates(elements={None: ElementState(JobStatus.RUNNING)})
        assert states.get(17).active
