"""Tests for ``validate_v2``.

The v2 validator runs a quick JSON-based check of each pipeline's diagnostics
and exits 0 or 1. It does not touch Maven, Spark, or the network, so these
tests can exercise it directly against tiny synthetic projects and never
go through the CLI for the interesting assertions.

The single CLI smoke test at the bottom keeps Click wiring honest.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from src.pbt import validate_v2
from src.pbt.pbt_cli import PBTCli

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


def _make_pipeline(project: Path, pipeline_id: str, diagnostics: list | None) -> None:
    # The product reads pipelines from ``<pipeline_id>/code/`` and returns a
    # dict keyed on paths relative to that folder.
    pipeline_dir = project / pipeline_id / "code" / ".prophecy"
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    workflow: dict = {"metainfo": {"name": pipeline_id.split("/")[-1]}}
    if diagnostics is not None:
        workflow["diagnostics"] = diagnostics
    (pipeline_dir / "workflow.latest.json").write_text(json.dumps(workflow))


def _make_project_with_job(tmp_path: Path, pipeline_ids: list[str]) -> Path:
    """Create a project with a single job that references ``pipeline_ids``."""

    project = tmp_path / "tiny"
    project.mkdir()
    pipelines_block = "\n".join(
        f"  {pid}:\n    name: {pid.split('/')[-1]}\n    language: python\n" for pid in pipeline_ids
    )
    pipeline_refs = "\n".join(f"    - {pid}" for pid in pipeline_ids)
    (project / "pbt_project.yml").write_text(
        "name: tiny\n"
        "language: python\n"
        "version: 0.0.1\n"
        f"pipelines:\n{pipelines_block}\n"
        "jobs:\n"
        "  jobs/smoke:\n"
        "    name: smoke\n"
        "    fabricUID: '1'\n"
        "    scheduler:\n"
        "      Databricks: {}\n"
        "    pipelines:\n"
        f"{pipeline_refs}\n"
    )
    return project


def test_validate_v2_succeeds_when_no_errors(tmp_path: Path) -> None:
    project = _make_project_with_job(tmp_path, ["pipelines/p1"])
    _make_pipeline(project, "pipelines/p1", diagnostics=[])

    pbt = PBTCli.from_conf_folder(str(project))
    with pytest.raises(SystemExit) as exc:
        pbt.validate(treat_warnings_as_errors=False)
    assert exc.value.code == 0


def test_validate_v2_fails_on_error_diagnostic(tmp_path: Path) -> None:
    project = _make_project_with_job(tmp_path, ["pipelines/p1"])
    _make_pipeline(project, "pipelines/p1", diagnostics=[{"severity": 1, "message": "bad"}])

    pbt = PBTCli.from_conf_folder(str(project))
    with pytest.raises(SystemExit) as exc:
        pbt.validate(treat_warnings_as_errors=False)
    assert exc.value.code == 1


def test_validate_v2_warnings_ignored_by_default(tmp_path: Path) -> None:
    project = _make_project_with_job(tmp_path, ["pipelines/p1"])
    _make_pipeline(project, "pipelines/p1", diagnostics=[{"severity": 2, "message": "heads up"}])

    pbt = PBTCli.from_conf_folder(str(project))
    with pytest.raises(SystemExit) as exc:
        pbt.validate(treat_warnings_as_errors=False)
    assert exc.value.code == 0


def test_validate_v2_warnings_as_errors_flag_fails(tmp_path: Path) -> None:
    project = _make_project_with_job(tmp_path, ["pipelines/p1"])
    _make_pipeline(project, "pipelines/p1", diagnostics=[{"severity": 2, "message": "heads up"}])

    pbt = PBTCli.from_conf_folder(str(project))
    with pytest.raises(SystemExit) as exc:
        pbt.validate(treat_warnings_as_errors=True)
    assert exc.value.code == 1


def test_validate_v2_cli_smoke(tmp_path: Path, cli_runner: CliRunner) -> None:
    """One CLI-level test ensures Click wiring threads ``--treat-warnings-as-errors``.

    Everything else tests ``PBTCli.validate`` directly for speed and to avoid
    asserting on Rich-rendered stdout.
    """

    project = _make_project_with_job(tmp_path, ["pipelines/p1"])
    _make_pipeline(project, "pipelines/p1", diagnostics=[{"severity": 2, "message": "heads up"}])

    ok = cli_runner.invoke(validate_v2, ["--path", str(project)])
    assert ok.exit_code == 0

    warn_as_err = cli_runner.invoke(validate_v2, ["--path", str(project), "--treat-warnings-as-errors"])
    assert warn_as_err.exit_code == 1
