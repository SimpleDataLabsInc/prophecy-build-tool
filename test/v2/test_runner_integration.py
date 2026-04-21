"""End-to-end seam test: a ``FakeRunner`` injected at ``PBTCli`` is carried
down to ``PackageBuilderAndUploader`` and intercepts the twine upload.

This guards the wiring added in the v2 refactor: if someone adds a new
constructor downstream and forgets to thread ``runner`` through, the call
falls back to ``default_runner`` and these tests fail fast.
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pytest

from fakes import FakeRunner
from src.pbt.deployment.pipeline import PackageBuilderAndUploader
from src.pbt.pbt_cli import PBTCli
from src.pbt.runner import CommandResult

pytestmark = [pytest.mark.integration, pytest.mark.v2, pytest.mark.fast]


@pytest.fixture
def wheel_project(tmp_path: Path) -> Path:
    project = tmp_path / "wheel-project"
    project.mkdir()
    (project / "pbt_project.yml").write_text(
        "name: wheel-project\n" "language: python\n" "version: 0.0.1\n" "pipelines: {}\n" "jobs: {}\n"
    )
    return project


def _make_builder(
    wheel_project: Path,
    fake: FakeRunner,
    artifactory_url: str = "https://artifactory.example.com/repo",
) -> Tuple[PackageBuilderAndUploader, Path]:
    pbt = PBTCli.from_conf_folder(str(wheel_project), runner=fake)
    project_config = pbt.project.project_config
    project_config.artifactory = artifactory_url
    project_config.skip_artifactory_upload = False

    # PackageBuilderAndUploader probes for python/pip via the runner during
    # __init__. Front-load a scripted success so subsequent scripted entries
    # apply to the behavior under test.
    fake.script.insert(0, CommandResult(args=[], returncode=0, stdout="Python 3.11"))

    builder = PackageBuilderAndUploader(
        pbt.project.project,
        pipeline_id="fake-pipeline",
        pipeline_name="fake-pipeline",
        project_config=project_config,
        runner=fake,
    )

    fake_wheel = wheel_project / "fake-1.0-py3-none-any.whl"
    fake_wheel.write_text("")
    return builder, fake_wheel


def test_fake_runner_intercepts_twine_upload(wheel_project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ARTIFACTORY_USERNAME", "user")
    monkeypatch.setenv("ARTIFACTORY_PASSWORD", "pass")

    fake = FakeRunner(script=[CommandResult(args=[], returncode=0)])
    builder, fake_wheel = _make_builder(wheel_project, fake)

    result = builder._uploading_to_artifactory(str(fake_wheel))
    assert result.is_right

    assert fake.executed("twine"), "twine should have been invoked via the injected runner"
    twine_call = fake.executed("twine")[0]
    assert twine_call.args[:2] == ["twine", "upload"]
    assert str(fake_wheel) in twine_call.args
    assert "https://artifactory.example.com/repo" in twine_call.args


def test_fake_runner_surfaces_twine_failure(wheel_project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ARTIFACTORY_USERNAME", "user")
    monkeypatch.setenv("ARTIFACTORY_PASSWORD", "pass")

    fake = FakeRunner(script=[CommandResult(args=[], returncode=1, stderr="bad credentials")])
    builder, fake_wheel = _make_builder(wheel_project, fake)

    with pytest.raises(Exception, match="Twine upload failed"):
        builder._uploading_to_artifactory(str(fake_wheel))


def test_process_sequential_accepts_injected_runner() -> None:
    """``Process.process_sequential`` now also accepts an optional runner."""

    from src.pbt.process import Process

    fake = FakeRunner(script=[CommandResult(args=[], returncode=0, stdout="ok")])
    rc = Process.process_sequential(
        [Process(process_args=["echo", "hi"], current_working_directory=None)],
        time_between_each_cmd=0,
        runner=fake,
    )

    assert rc == 0
    assert fake.executed("echo"), "process_sequential should route through the runner"
