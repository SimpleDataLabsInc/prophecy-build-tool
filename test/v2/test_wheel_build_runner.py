"""``PackageBuilderAndUploader._build`` routes external commands through ``CommandRunner``."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from fakes import FakeRunner
from src.pbt.deployment.pipeline import PackageBuilderAndUploader
from src.pbt.pbt_cli import PBTCli
from src.pbt.runner import CommandResult

pytestmark = [pytest.mark.unit, pytest.mark.v2]


@pytest.fixture
def wheel_project(tmp_path: Path) -> Path:
    project = tmp_path / "wheel-project"
    project.mkdir()
    (project / "pbt_project.yml").write_text(
        "name: wheel-project\n" "language: python\n" "version: 0.0.1\n" "pipelines: {}\n" "jobs: {}\n"
    )
    (project / "fake-pipeline" / "code").mkdir(parents=True)
    return project


def _builder(wheel_project: Path, fake: FakeRunner) -> PackageBuilderAndUploader:
    pbt = PBTCli.from_conf_folder(str(wheel_project), runner=fake)
    # __init__ does a python probe via the runner; front-load a success so the
    # rest of ``fake.script`` applies to the behavior under test.
    fake.script.insert(0, CommandResult(args=[], returncode=0, stdout="Python 3.11"))
    builder = PackageBuilderAndUploader(
        pbt.project.project,
        pipeline_id="fake-pipeline",
        pipeline_name="fake-pipeline",
        project_config=pbt.project.project_config,
        runner=fake,
    )
    # Reset recorded calls so assertions target only the subsequent _build call.
    fake.calls.clear()
    return builder


def test_build_routes_through_runner_and_sets_fabric_default(
    wheel_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeRunner(
        script=[
            CommandResult(
                args=[],
                returncode=0,
                stdout="line1\nProgress (1): something\nok\n",
                stderr="errline\n",
            )
        ]
    )
    builder = _builder(wheel_project, fake)
    monkeypatch.delenv("FABRIC_NAME", raising=False)

    rc = builder._build([sys.executable, "-c", "print(1)"])
    assert rc == 0
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call.args[:2] == [sys.executable, "-c"]
    assert call.cwd == builder._base_path
    assert call.env is not None
    assert call.env.get("FABRIC_NAME") == "default"
    assert "MAVEN_OPTS" in call.env


def test_build_failure_raises_project_build_failed(wheel_project: Path) -> None:
    from src.pbt.utils.exceptions import ProjectBuildFailedException

    fake = FakeRunner(script=[CommandResult(args=[], returncode=1, stdout="", stderr="nope")])
    builder = _builder(wheel_project, fake)

    with pytest.raises(ProjectBuildFailedException, match="exit code 1"):
        builder._build([sys.executable, "-c", "import sys; sys.exit(1)"])


def test_build_respects_ignore_build_errors(wheel_project: Path) -> None:
    fake = FakeRunner(script=[CommandResult(args=[], returncode=7, stdout="", stderr="")])
    builder = _builder(wheel_project, fake)

    rc = builder._build([sys.executable, "-c", "import sys; sys.exit(7)"], ignore_build_errors=True)
    assert rc == 7
