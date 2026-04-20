"""Tests for ``deploy_v2`` option plumbing through :class:`ProjectConfig`.

Rather than running ``deploy_v2`` end-to-end (which spins up Maven, Spark,
and Databricks clients), we exercise the cheap part of the pipeline: the
options from ``deploy_v2`` flow into ``PBTCli.from_conf_folder`` which builds
a ``ProjectConfig``. Every interesting flag (``--skip-builds``,
``--skip-pipeline-deploy``, ``--migrate``, ``--artifactory``, ``--fabric-ids``,
``--job-ids``) leaves a visible fingerprint on that config object, so we can
assert there without any external calls.

These tests are sub-second and run under xdist.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.pbt.pbt_cli import PBTCli
from src.pbt.utils.project_config import DeploymentMode

pytestmark = [pytest.mark.unit, pytest.mark.v2]


def _project_with_fabric(tmp_path: Path, fabric_ids: list[str]) -> Path:
    project = tmp_path / "deploy-test"
    project.mkdir()
    jobs_block = ""
    for idx, fid in enumerate(fabric_ids):
        jobs_block += (
            f"  jobs/job{idx}:\n"
            f"    name: job{idx}\n"
            f"    fabricUID: '{fid}'\n"
            f"    scheduler:\n      Databricks: {{}}\n"
            f"    pipelines: []\n"
        )
    (project / "pbt_project.yml").write_text(
        "name: deploy-test\n" "language: python\n" "version: 0.0.1\n" "pipelines: {}\n" "jobs:\n" f"{jobs_block}"
    )
    return project


def test_default_flags_produce_full_project_mode(tmp_path: Path) -> None:
    project = _project_with_fabric(tmp_path, ["10", "20"])
    pbt = PBTCli.from_conf_folder(str(project))

    config = pbt.project.project_config
    assert config.skip_builds is False
    assert config.skip_pipeline_deploy is False
    assert config.migrate is False
    assert config.artifactory == ""
    assert config.skip_artifactory_upload is False
    assert config.configs_override.mode == DeploymentMode.FullProject


def test_skip_builds_and_pipeline_deploy_plumbed(tmp_path: Path) -> None:
    project = _project_with_fabric(tmp_path, ["10"])
    pbt = PBTCli.from_conf_folder(str(project), skip_builds=True, skip_pipeline_deploy=True)

    assert pbt.project.project_config.skip_builds is True
    assert pbt.project.project_config.skip_pipeline_deploy is True


def test_migrate_and_artifactory_plumbed(tmp_path: Path) -> None:
    project = _project_with_fabric(tmp_path, ["10"])
    pbt = PBTCli.from_conf_folder(
        str(project),
        migrate=True,
        artifactory="https://artifactory.example.com/repo",
        skip_artifactory_upload=True,
    )

    assert pbt.project.project_config.migrate is True
    assert pbt.project.project_config.artifactory == "https://artifactory.example.com/repo"
    assert pbt.project.project_config.skip_artifactory_upload is True


def test_fabric_ids_filter_restricts_fabrics(tmp_path: Path) -> None:
    project = _project_with_fabric(tmp_path, ["10", "20", "30"])
    pbt = PBTCli.from_conf_folder(str(project), fabric_ids="10,30")

    remaining = {f.id for f in pbt.project.project_config.fabric_config.fabrics}
    assert remaining == {"10", "30"}


def test_job_ids_filter_switches_mode_to_selective(tmp_path: Path) -> None:
    project = _project_with_fabric(tmp_path, ["10", "20"])
    pbt = PBTCli.from_conf_folder(str(project), job_ids="job0")

    config = pbt.project.project_config
    assert config.configs_override.mode == DeploymentMode.SelectiveJob
    assert any(j.job_id == "jobs/job0" for j in config.configs_override.jobs_and_fabric)
