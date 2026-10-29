"""``Executor.job_status``: is a job still in the queue, on every backend.

Each backend asks its live queue.  The contract is the same everywhere:
listed and live means QUEUED/RUNNING, "not there (any more)" means
ENDED, and a query that could not be made means UNKNOWN.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from reflow.executors import (
    ElementState,
    Executor,
    JobResources,
    JobStates,
    JobStatus,
    summarise,
)
from reflow.executors.flux import FluxExecutor
from reflow.executors.local import LocalExecutor
from reflow.executors.lsf import LSFExecutor
from reflow.executors.pbs import PBSExecutor
from reflow.executors.sge import SGEExecutor
from reflow.executors.slurm import SlurmExecutor
from reflow.executors.util import CommandResult

Q, R, E, U = JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.ENDED, JobStatus.UNKNOWN


def _out(stdout: str = "", returncode: int = 0, stderr: str = "") -> CommandResult:
    return CommandResult(cmd=[], stdout=stdout, stderr=stderr, returncode=returncode)


class TestSummarise:
    @pytest.mark.parametrize(
        ("statuses", "expected"),
        [
            ([E, R, Q], R),
            ([E, Q, U], Q),
            ([E, U], U),
            ([E, E], E),
        ],
    )
    def test_alive_wins(self, statuses: list[JobStatus], expected: JobStatus) -> None:
        assert summarise(ElementState(s) for s in statuses).status is expected

    def test_nothing_listed_gives_default(self) -> None:
        assert summarise([]).status is U
        assert JobStates(default=ElementState(E)).overall().status is E


class _Minimal(Executor):
    def submit(self, resources: JobResources, command: list[str]) -> str:
        return "1"

    def cancel(self, job_id: str) -> None:
        pass

    def job_state(self, job_id: str) -> str | None:
        return None


def test_default_is_unknown_and_dry_run_ended() -> None:
    exc = _Minimal()
    assert exc.job_status("1").status is U
    assert exc.job_status("DRYRUN").status is E


def test_local_jobs_have_ended() -> None:
    assert LocalExecutor().job_status("local-1").status is E


class TestSlurm:
    RUN = "reflow.executors.slurm.run_cmd"

    def test_any_live_element(self) -> None:
        with patch(self.RUN, return_value=_out("12_0|RUNNING\n12_[1-4]|PENDING")) as run:
            assert SlurmExecutor().job_status("12;cluster").status is R
        assert run.call_args.args[0][:3] == ["squeue", "-j", "12"]

    def test_not_listed_has_ended(self) -> None:
        with patch(self.RUN, return_value=_out("")):
            assert SlurmExecutor().job_status("12").status is E

    def test_invalid_job_id_has_ended(self) -> None:
        err = _out(returncode=1, stderr="slurm_load_jobs error: Invalid job id specified")
        with patch(self.RUN, return_value=err):
            assert SlurmExecutor().job_status("12").status is E

    def test_controller_down_is_unknown(self) -> None:
        err = _out(returncode=1, stderr="Unable to contact slurm controller")
        with patch(self.RUN, return_value=err):
            assert SlurmExecutor().job_status("12").status is U
        with patch(self.RUN, side_effect=OSError("no squeue")):
            assert SlurmExecutor().job_status("12").status is U


class TestPBS:
    RUN = "reflow.executors.pbs.run_cmd"

    def _exc(self) -> PBSExecutor:
        return PBSExecutor(array_flag="-J")

    @pytest.mark.parametrize(
        ("code", "expected"), [("Q", Q), ("H", Q), ("R", R), ("B", R), ("F", E), ("C", E)]
    )
    def test_states(self, code: str, expected: JobStatus) -> None:
        with patch(self.RUN, return_value=_out(f"Job Id: 7\n    job_state = {code}\n")):
            assert self._exc().job_status("7[]").status is expected

    @pytest.mark.parametrize(
        "stderr",
        ["qstat: Unknown Job Id 7.srv", "qstat: 7.srv Job has finished, use -x or -H"],
    )
    def test_gone_has_ended(self, stderr: str) -> None:
        with patch(self.RUN, return_value=_out(returncode=153, stderr=stderr)):
            assert self._exc().job_status("7").status is E

    def test_server_down_is_unknown(self) -> None:
        err = _out(returncode=15010, stderr="Connection refused")
        with patch(self.RUN, return_value=err):
            assert self._exc().job_status("7").status is U


class TestLSF:
    RUN = "reflow.executors.lsf.run_cmd"

    def test_array_elements(self) -> None:
        with patch(self.RUN, return_value=_out("DONE\nPEND\nEXIT")):
            assert LSFExecutor().job_status("9").status is Q
        with patch(self.RUN, return_value=_out("DONE\nEXIT")):
            assert LSFExecutor().job_status("9").status is E

    def test_not_found_has_ended(self) -> None:
        err = _out(returncode=255, stderr="Job <9> is not found")
        with patch(self.RUN, return_value=err):
            assert LSFExecutor().job_status("9").status is E

    def test_daemon_down_is_unknown(self) -> None:
        err = _out(returncode=255, stderr="LSF is down. Please wait ...")
        with patch(self.RUN, return_value=err):
            assert LSFExecutor().job_status("9").status is U


class TestSGE:
    RUN = "reflow.executors.sge.run_cmd"

    def test_running_task(self) -> None:
        text = "job_number: 5\njob_state    1:    qw\njob_state    2:    r\n"
        with patch(self.RUN, return_value=_out(text)):
            assert SGEExecutor().job_status("5").status is R

    def test_listed_without_state_is_queued(self) -> None:
        with patch(self.RUN, return_value=_out("job_number: 5\n")):
            assert SGEExecutor().job_status("5").status is Q

    def test_gone_has_ended(self) -> None:
        err = _out(returncode=1, stdout="Following jobs do not exist: \n5")
        with patch(self.RUN, return_value=err):
            assert SGEExecutor().job_status("5").status is E

    def test_qmaster_down_is_unknown(self) -> None:
        err = _out(returncode=1, stderr="error: commlib error: can't connect")
        with patch(self.RUN, return_value=err):
            assert SGEExecutor().job_status("5").status is U


class TestFlux:
    RUN = "reflow.executors.flux.run_cmd"

    def test_group_alive_while_one_runs(self) -> None:
        listing = _out("ƒa INACTIVE COMPLETED\nƒb RUN")
        with patch(self.RUN, return_value=listing):
            assert FluxExecutor().job_status("ƒa,ƒb").status is R
        other = _out(returncode=1, stderr="flux-jobs: ERROR: JobID ƒz unknown")
        with patch(self.RUN, return_value=other):
            assert FluxExecutor().job_status("ƒa").status is U

    def test_all_inactive_has_ended(self) -> None:
        with patch(self.RUN, return_value=_out("ƒa INACTIVE COMPLETED")):
            assert FluxExecutor().job_status("ƒa").status is E

    def test_ascii_and_unicode_ids_match(self) -> None:
        with patch(self.RUN, return_value=_out("ƒa SCHED")):
            assert FluxExecutor().job_status("fa").status is Q

    def test_purged_job_has_ended(self) -> None:
        err = _out("ƒb RUN", returncode=1, stderr="flux-jobs: ERROR: JobID ƒa unknown")
        with patch(self.RUN, return_value=err):
            assert FluxExecutor().job_status("ƒa").status is E
            assert FluxExecutor().job_status("ƒa,ƒb").status is R
        other = _out(returncode=1, stderr="flux-jobs: ERROR: JobID ƒz unknown")
        with patch(self.RUN, return_value=other):
            assert FluxExecutor().job_status("ƒa").status is U

    def test_broker_down_is_unknown(self) -> None:
        err = _out(
            returncode=1,
            stderr="flux-jobs: ERROR: Unable to connect to Flux: No such file or directory",
        )
        with patch(self.RUN, return_value=err):
            assert FluxExecutor().job_status("ƒa").status is U
        with patch(self.RUN, side_effect=OSError("no flux")):
            assert FluxExecutor().job_status("ƒa").status is U
