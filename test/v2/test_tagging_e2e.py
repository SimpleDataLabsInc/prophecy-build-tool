"""Port of ``test/test_tagging.py`` onto a vendored git bundle.

Legacy version cloned ``https://github.com/prophecy-samples/HelloProphecy.git``
on every test invocation, which:

- Hit the network on every run.
- Assumed the remote repo layout was stable forever.
- Wrote into ``/tmp`` under a uuid that was never cleaned up.

We now clone ``test/resources/tagging.bundle`` (see
``test/resources/bundles/build_tagging_bundle.sh``) into ``tmp_path`` and
exercise the ``pbt tag`` CLI against it. All tests use ``--no-push`` so no
network is involved at any point.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner
from git import Repo

from src.pbt import tag

pytestmark = [pytest.mark.e2e, pytest.mark.v2]


def _read_pbt_version(project: Path) -> str:
    for line in (project / "pbt_project.yml").read_text().splitlines():
        if line.startswith("version: "):
            return line.split(":", 1)[1].strip()
    raise AssertionError("pbt_project.yml missing version field")


@pytest.fixture
def tagging_repo(git_bundle_repo):
    """Yield ``(repo_path, python_project_path, scala_project_path)``."""

    repo_path = git_bundle_repo("tagging.bundle")
    python_project_path = repo_path / "prophecy"
    scala_project_path = repo_path / "prophecy_scala"
    return repo_path, python_project_path, scala_project_path


def test_tagging_custom(cli_runner: CliRunner, tagging_repo) -> None:
    repo_path, python_project, _ = tagging_repo
    custom = "CUSTOM_TAG_UNITTEST"

    result = cli_runner.invoke(
        tag,
        [
            "--path",
            str(python_project),
            "--repo-path",
            str(repo_path),
            "--no-push",
            "--custom",
            custom,
        ],
    )
    assert result.exit_code == 0, result.output
    assert custom in [str(t) for t in Repo(str(repo_path)).tags]


@pytest.mark.parametrize("language", ["python", "scala"])
def test_tagging_default_uses_branch_slash_version(
    cli_runner: CliRunner, tagging_repo, language: str
) -> None:
    repo_path, python_project, scala_project = tagging_repo
    project = python_project if language == "python" else scala_project

    branch_name = f"custom_branch_unittest_{language}"
    subprocess.run(
        ["git", "-C", str(repo_path), "checkout", "-b", branch_name],
        check=True,
        capture_output=True,
    )

    result = cli_runner.invoke(
        tag, ["--path", str(project), "--repo-path", str(repo_path), "--no-push"]
    )
    assert result.exit_code == 0, result.output

    pbt_version = _read_pbt_version(project)
    expected = f"{branch_name}/{pbt_version}"
    assert expected in [str(t) for t in Repo(str(repo_path)).tags]


def test_tagging_omit_branchname(cli_runner: CliRunner, tagging_repo) -> None:
    repo_path, python_project, _ = tagging_repo
    pbt_version = _read_pbt_version(python_project)

    result = cli_runner.invoke(
        tag,
        [
            "--path",
            str(python_project),
            "--repo-path",
            str(repo_path),
            "--no-push",
            "--branch",
            "",
        ],
    )
    assert result.exit_code == 0, result.output
    assert pbt_version in [str(t) for t in Repo(str(repo_path)).tags]


def test_tagging_custom_branchname(cli_runner: CliRunner, tagging_repo) -> None:
    repo_path, python_project, _ = tagging_repo
    pbt_version = _read_pbt_version(python_project)
    custom_branch = "nonexistant_branch"

    result = cli_runner.invoke(
        tag,
        [
            "--path",
            str(python_project),
            "--repo-path",
            str(repo_path),
            "--no-push",
            "--branch",
            custom_branch,
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"{custom_branch}/{pbt_version}" in [str(t) for t in Repo(str(repo_path)).tags]
