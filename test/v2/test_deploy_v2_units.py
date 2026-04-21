"""Unit-style port of every scenario in ``test/test_deploy.py``.

The legacy tests drove the real ``pbt deploy`` CLI against HelloWorld, which
took ~15-20s apiece because the CLI attempted a full Databricks deploy (and
only failed out once HTTP requests started). Here we:

- Use a tiny synthetic project (2 jobs + up to 4 pipelines) written to
  ``tmp_path`` — no HelloWorld parsing.
- Patch ``ProjectDeployment.deploy`` with the ``fake_databricks_deploy``
  recorder from the v2 conftest, so flag/filter plumbing is visible as
  recorder state.
- For the three legacy-only validation errors (``Can't combine filters``,
  ``Can't skip builds for job_id filter``, ``No Job IDs matches``) we invoke
  the legacy ``deploy`` CLI directly against the same synthetic project,
  since those checks do not exist in ``deploy_v2``. They still fail fast
  because ``ProphecyBuildTool.deploy`` raises before any external call.

Each test runs in well under a second.
"""

from __future__ import annotations

from click.testing import CliRunner

import pytest

from src.pbt import deploy as legacy_deploy, deploy_v2
from src.pbt.utils.project_config import DeploymentMode

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _hello_world_shape(synthetic_project):
    """Build a synthetic project with HelloWorld's shape: 2 jobs + 4 pipelines.

    ``test-job`` depends on 3 pipelines; ``job-another`` depends on all 4.
    Two distinct fabrics (``647`` and ``648``) are represented, mirroring
    the legacy fixture.
    """

    pipelines = [
        ("pipelines/customers_orders", "customers_orders"),
        ("pipelines/report_top_customers", "report_top_customers"),
        ("pipelines/join_agg_sort", "join_agg_sort"),
        ("pipelines/farmers-markets-irs", "farmers-markets-irs"),
    ]
    jobs = [
        {
            "id": "jobs/test-job",
            "name": "test-job",
            "fabric": "647",
            "scheduler": "Databricks",
            "pipeline_ids": [pid for pid, _ in pipelines[:3]],
        },
        {
            "id": "jobs/job-another",
            "name": "job-another",
            "fabric": "648",
            "scheduler": "Databricks",
            "pipeline_ids": [pid for pid, _ in pipelines],
        },
    ]
    return synthetic_project(
        name="HelloWorld",
        language="python",
        pipelines=pipelines,
        jobs=jobs,
        with_databricks_json=True,
    )


def _run_deploy_v2(runner: CliRunner, project_path, *extra):
    result = runner.invoke(deploy_v2, ["--path", str(project_path), *extra])
    assert result.exit_code == 0, (
        f"deploy_v2 failed unexpectedly (exit={result.exit_code}): " f"{result.output}\n{result.exception!r}"
    )
    return result


# ---------------------------------------------------------------------------
# Happy-path & flag plumbing (ports legacy happy-path + filter tests)
# ---------------------------------------------------------------------------


def test_default_invokes_deploy_once_full_project_mode(
    cli_runner: CliRunner, synthetic_project, fake_databricks_deploy
) -> None:
    """Replaces legacy ``test_deploy_path_default`` / ``_new_project`` happy paths."""

    project = _hello_world_shape(synthetic_project)
    _run_deploy_v2(cli_runner, project)

    assert len(fake_databricks_deploy.calls) == 1
    call = fake_databricks_deploy.calls[0]
    assert call.mode == DeploymentMode.FullProject
    assert call.skip_builds is False
    assert call.job_ids == []
    # Both fabrics present when no --fabric-ids is passed.
    assert set(call.fabric_ids) == {"647", "648"}


def test_skip_builds_flag_plumbed_to_deploy(cli_runner: CliRunner, synthetic_project, fake_databricks_deploy) -> None:
    """Replaces legacy ``test_deploy_path_default_skip_builds``."""

    project = _hello_world_shape(synthetic_project)
    _run_deploy_v2(cli_runner, project, "--skip-builds")

    assert len(fake_databricks_deploy.calls) == 1
    assert fake_databricks_deploy.calls[0].skip_builds is True


def test_fabric_ids_filter_restricts_fabrics_in_deploy(
    cli_runner: CliRunner, synthetic_project, fake_databricks_deploy
) -> None:
    """Replaces legacy ``test_deploy_path_fabric_id_filter``.

    Only the selected fabric's FabricInfo reaches the deploy layer; jobs for
    unselected fabrics will be skipped at deploy time.
    """

    project = _hello_world_shape(synthetic_project)
    _run_deploy_v2(cli_runner, project, "--fabric-ids", "647")

    call = fake_databricks_deploy.calls[0]
    assert call.fabric_ids == ["647"]


def test_invalid_fabric_id_propagates_unchanged(
    cli_runner: CliRunner, synthetic_project, fake_databricks_deploy
) -> None:
    """Replaces legacy ``test_deploy_path_pipeline_invalid_fabric_id``.

    When there is no ``fabrics.yml`` config folder, v2 synthesises a
    ``FabricInfo`` for every id in ``--fabric-ids`` (real or not). The
    deploy layer is then responsible for skipping jobs whose fabric id isn't
    known. We simply assert the filter plumbed through to deploy.
    """

    project = _hello_world_shape(synthetic_project)
    _run_deploy_v2(cli_runner, project, "--fabric-ids", "999")

    call = fake_databricks_deploy.calls[0]
    assert call.fabric_ids == ["999"]


def test_job_ids_filter_switches_to_selective_mode(
    cli_runner: CliRunner, synthetic_project, fake_databricks_deploy
) -> None:
    """Replaces legacy ``test_deploy_path_pipeline_with_job_id_filter``."""

    project = _hello_world_shape(synthetic_project)
    _run_deploy_v2(cli_runner, project, "--job-ids", "test-job")

    call = fake_databricks_deploy.calls[0]
    assert call.mode == DeploymentMode.SelectiveJob
    assert call.jobs_and_fabric == [("jobs/test-job", "")]


def test_multiple_job_ids_filter_kept_in_jobs_and_fabric(
    cli_runner: CliRunner, synthetic_project, fake_databricks_deploy
) -> None:
    """Replaces legacy ``test_deploy_path_pipeline_with_multiple_job_id_filter``."""

    project = _hello_world_shape(synthetic_project)
    _run_deploy_v2(cli_runner, project, "--job-ids", "test-job,job-another")

    call = fake_databricks_deploy.calls[0]
    assert call.mode == DeploymentMode.SelectiveJob
    assert sorted(call.jobs_and_fabric) == [
        ("jobs/job-another", ""),
        ("jobs/test-job", ""),
    ]


def test_partial_invalid_job_ids_keeps_both_in_selective_mode(
    cli_runner: CliRunner, synthetic_project, fake_databricks_deploy
) -> None:
    """Replaces legacy ``test_deploy_path_pipeline_with_one_invalid_job_id_filter``.

    v2 doesn't bail out on unknown job ids the way legacy does. The unknown
    id goes through as a ``JobAndFabric`` entry; downstream deploy will
    simply find no matching job to act on. We assert the selective-mode
    plumbing rather than the legacy stdout substrings.
    """

    project = _hello_world_shape(synthetic_project)
    _run_deploy_v2(cli_runner, project, "--job-ids", "invalid1,test-job")

    call = fake_databricks_deploy.calls[0]
    assert call.mode == DeploymentMode.SelectiveJob
    assert sorted(call.jobs_and_fabric) == [
        ("jobs/invalid1", ""),
        ("jobs/test-job", ""),
    ]


def test_all_invalid_job_ids_still_invokes_selective_mode_in_v2(
    cli_runner: CliRunner, synthetic_project, fake_databricks_deploy
) -> None:
    """v2 counterpart to legacy ``test_deploy_path_pipeline_with_all_invalid_job_ids_filter``.

    The legacy CLI refused entirely ('No Job IDs matches ...'); the v2
    pipeline hands the (phantom) list to deploy unchanged, where zero jobs
    match and nothing happens. The legacy-specific error case is covered in
    ``test_legacy_all_invalid_job_ids_errors`` below.
    """

    project = _hello_world_shape(synthetic_project)
    _run_deploy_v2(cli_runner, project, "--job-ids", "invalid1,invalid2")

    call = fake_databricks_deploy.calls[0]
    assert call.mode == DeploymentMode.SelectiveJob
    assert sorted(call.jobs_and_fabric) == [
        ("jobs/invalid1", ""),
        ("jobs/invalid2", ""),
    ]


# ---------------------------------------------------------------------------
# Legacy-only validation error paths (still fast with a synthetic project).
# ---------------------------------------------------------------------------
#
# These checks live in ``ProphecyBuildTool.deploy`` and are not mirrored in
# ``deploy_v2``. The assertions themselves are behavioral (CLI-surface
# contract), not substring-heavy, so we accept a one-line stdout probe.


def test_legacy_combine_fabric_and_job_filter_errors(cli_runner: CliRunner, synthetic_project) -> None:
    """Legacy: fabric_ids + job_ids is explicitly rejected."""

    project = _hello_world_shape(synthetic_project)
    result = cli_runner.invoke(
        legacy_deploy,
        ["--path", str(project), "--fabric-ids", "647", "--job-ids", "test-job"],
    )
    assert result.exit_code == 1
    assert "Can't combine filters" in result.output


def test_legacy_job_ids_plus_skip_builds_errors(cli_runner: CliRunner, synthetic_project) -> None:
    """Legacy: job_ids + skip_builds is explicitly rejected."""

    project = _hello_world_shape(synthetic_project)
    result = cli_runner.invoke(legacy_deploy, ["--path", str(project), "--job-ids", "test-job", "--skip-builds"])
    assert result.exit_code == 1
    assert "Can't skip builds for job_id filter" in result.output


def test_legacy_all_invalid_job_ids_errors(cli_runner: CliRunner, synthetic_project) -> None:
    """Legacy: all-invalid job_ids raises before build/deploy."""

    project = _hello_world_shape(synthetic_project)
    result = cli_runner.invoke(legacy_deploy, ["--path", str(project), "--job-ids", "invalid1,invalid2"])
    assert result.exit_code == 1
    assert "No Job IDs matches with passed --job_id filter" in result.output
