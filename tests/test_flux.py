"""Flux executor: one job per array element, repeated dependencies.

``flux submit --cc=IDSET`` creates a separate job per index and prints
one id per line.  These tests pin how reflow keeps all of them.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from reflow.executors import JobResources, JobStatus
from reflow.executors.flux import FluxExecutor
from reflow.executors.util import CommandResult, ExecutorError

RUN = "reflow.executors.flux.run_cmd"


def _out(stdout: str = "", returncode: int = 0, stderr: str = "") -> CommandResult:
    return CommandResult(cmd=[], stdout=stdout, stderr=stderr, returncode=returncode)


def _flux() -> FluxExecutor:
    return FluxExecutor(mode="flux", flux="flux")


def _array(spec: str = "0-2") -> JobResources:
    return JobResources(job_name="wf-convert", array=spec)


class TestBuildSubmit:
    def test_array_job_name_carries_the_index(self) -> None:
        cmd = _flux()._build_submit(_array(), ["echo"])
        assert "--cc=0-2" in cmd
        assert cmd[cmd.index("--job-name") + 1] == "wf-convert.{cc}"

    def test_plain_job_name_unchanged(self) -> None:
        cmd = _flux()._build_submit(JobResources(job_name="wf-one"), ["echo"])
        assert cmd[cmd.index("--job-name") + 1] == "wf-one"
        assert not any(c.startswith("--cc") for c in cmd)

    def test_parallelism_limit_is_dropped_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            cmd = _flux()._build_submit(_array("0-99%10"), ["echo"])
        assert "--cc=0-99" in cmd
        assert "array_parallelism=10" in caplog.text

    def test_one_dependency_flag_per_job(self) -> None:
        exc = _flux()
        res = JobResources(
            job_name="x", submit_options=exc.dependency_options(["ƒa,ƒb", "ƒc"])
        )
        flags = [c for c in exc._build_submit(res, ["echo"]) if "dependency" in c]
        assert flags == [
            "--dependency=afterany:ƒa",
            "--dependency=afterany:ƒb",
            "--dependency=afterany:ƒc",
        ]


class TestSubmit:
    def test_single_job_returns_its_id(self) -> None:
        with patch(RUN, return_value=_out("note: something\nƒ2abc")):
            assert _flux().submit(JobResources(job_name="x"), ["echo"]) == "ƒ2abc"

    def test_array_returns_all_ids_in_index_order(self) -> None:
        printed = _out("ƒA\nƒB\nƒC")
        # Flux printed them out of index order; the names say which is which.
        names = _out("ƒA wf-convert.2\nƒB wf-convert.0\nƒC wf-convert.1")
        with patch(RUN, side_effect=[printed, names]) as run:
            job_id = _flux().submit(_array(), ["echo"])
        assert job_id == "ƒB,ƒC,ƒA"
        listing = run.call_args_list[1].args[0]
        assert listing[:2] == ["flux", "jobs"] and listing[-3:] == ["ƒA", "ƒB", "ƒC"]

    def test_array_falls_back_to_print_order(self) -> None:
        with patch(
            RUN, side_effect=[_out("f1\nf2"), ExecutorError(_out(returncode=1))]
        ):
            assert _flux().submit(_array("0-1"), ["echo"]) == "f1,f2"

    def test_decimal_and_ascii_ids_are_accepted(self) -> None:
        names = _out("123 wf-convert.0\nf3x wf-convert.1")
        with patch(RUN, side_effect=[_out("123\nf3x"), names]):
            assert _flux().submit(_array("0-1"), ["echo"]) == "123,f3x"


class TestElementJobIds:
    def test_maps_ids_to_sorted_indices(self) -> None:
        got = _flux().element_job_ids("ƒa,ƒb,ƒc", [7, 3, 5])
        assert got == {3: "ƒa", 5: "ƒb", 7: "ƒc"}

    def test_single_element(self) -> None:
        assert _flux().element_job_ids("ƒa", [4]) == {4: "ƒa"}

    def test_count_mismatch_keeps_the_group(self) -> None:
        assert _flux().element_job_ids("ƒa,ƒb", [0, 1, 2]) is None


class TestCancel:
    def test_cancel_expands_groups_in_one_call(self) -> None:
        with patch(RUN, return_value=_out()) as run:
            _flux().cancel_many(["ƒa,ƒb", "ƒc", "DRYRUN"])
        run.assert_called_once_with(["flux", "cancel", "ƒa", "ƒb", "ƒc"])

    def test_cancel_batches_large_sets(self) -> None:
        ids = [f"f{i}" for i in range(450)]
        with patch(RUN, return_value=_out()) as run:
            _flux().cancel(",".join(ids))
        assert [len(c.args[0]) - 2 for c in run.call_args_list] == [200, 200, 50]

    def test_cancel_failure_is_logged_not_raised(self) -> None:
        with patch(RUN, side_effect=ExecutorError(_out(returncode=1))):
            _flux().cancel("ƒa")


class TestJobStates:
    def test_group_states_keyed_by_index(self) -> None:
        listing = _out(
            "ƒa INACTIVE COMPLETED wf-convert.0\n"
            "ƒb RUN wf-convert.1\n"
            "ƒc INACTIVE TIMEOUT wf-convert.2\n"
            "ƒd SCHED wf-convert.3"
        )
        with patch(RUN, return_value=listing):
            states = _flux().job_states("ƒa,ƒb,ƒc,ƒd")
        assert states.get(0).status is JobStatus.ENDED
        assert states.get(1).status is JobStatus.RUNNING
        assert (states.get(2).status, states.get(2).reason) == (
            JobStatus.ENDED,
            "TIMEOUT",
        )
        assert states.get(3).status is JobStatus.QUEUED

    def test_single_job_covers_any_index(self) -> None:
        with patch(RUN, return_value=_out("ƒa INACTIVE FAILED wf-convert.4")):
            states = _flux().job_states("ƒa")
        assert states.get(4).reason == "FAILED"

    def test_query_failure_is_unknown(self) -> None:
        with patch(RUN, side_effect=OSError("no flux")):
            assert _flux().job_states("ƒa").get(0).status is JobStatus.UNKNOWN
        with patch(RUN, return_value=_out("", returncode=1, stderr="boom")):
            assert _flux().job_states("ƒa").get(0).status is JobStatus.UNKNOWN

    def test_dry_run_id_is_not_queried(self) -> None:
        with patch(RUN) as run:
            _flux().job_states("DRYRUN")
        run.assert_not_called()
