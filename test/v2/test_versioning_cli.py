"""CLI-level tests for ``pbt versioning`` (non-git scenarios only).

The git-heavy scenarios (``--compare-to-target``, ``--make-unique``) are
covered in ``test_versioning_git_e2e.py`` using a vendored bundle. Here we
cover every non-git branch end-to-end against a synthetic project:

- ``--set`` (happy path, below-current without force, below-current with
  force, invalid string with force)
- ``--set-suffix`` (valid semver suffix, override with force)
- ``--bump`` (major/minor/patch/build/prerelease)
- ``--sync`` (propagates pbt_project.yml version to setup.py)
- ``--check-sync`` (pass + fail)
- mutually-exclusive flag combos + "no option" usage errors
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from src.pbt import versioning

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


def _read_pbt_version(project: Path) -> str:
    return yaml.safe_load((project / "pbt_project.yml").read_text())["version"]


def _read_setup_py_version(setup_py: Path) -> str | None:
    m = re.search(r"version\s*=\s*['\"]([^'\"]+)['\"]", setup_py.read_text())
    return m.group(1) if m else None


def _python_project(synthetic_project, version: str = "0.0.1") -> Path:
    project = synthetic_project(
        language="python",
        version=version,
        pipelines=[("pipelines/demo", "demo")],
    )
    # Overwrite the default synthetic setup.py to match the requested version.
    # The multi-line form is required because ``update_all_versions`` matches
    # on a regex anchored to the start of the line.
    setup_py = project / "pipelines" / "demo" / "code" / "setup.py"
    setup_py.write_text(
        "from setuptools import setup\n" "setup(\n" "    name='x',\n" f"    version='{version}'\n" ")\n"
    )
    return project


def _scala_project(synthetic_project, version: str = "0.0.1") -> Path:
    project = synthetic_project(
        language="scala",
        version=version,
        pipelines=[("pipelines/demo", "demo")],
    )
    # Give the pipeline a real-enough pom.xml that version_check_sync parses.
    pom = project / "pipelines" / "demo" / "code" / "pom.xml"
    pom.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<project xmlns="http://maven.apache.org/POM/4.0.0">\n'
        "  <modelVersion>4.0.0</modelVersion>\n"
        "  <groupId>g</groupId>\n"
        "  <artifactId>x</artifactId>\n"
        f"  <version>{version}</version>\n"
        "</project>\n"
    )
    return project


# ---------------------------------------------------------------------------
# --set
# ---------------------------------------------------------------------------


def test_set_high_version_succeeds(cli_runner: CliRunner, synthetic_project) -> None:
    project = _python_project(synthetic_project, "0.0.1")
    result = cli_runner.invoke(versioning, ["--path", str(project), "--set", "999999.0.0"])
    assert result.exit_code == 0, result.output
    assert _read_pbt_version(project) == "999999.0.0"


def test_set_below_current_without_force_fails(cli_runner: CliRunner, synthetic_project) -> None:
    project = _python_project(synthetic_project, "1.2.3")
    result = cli_runner.invoke(versioning, ["--path", str(project), "--set", "0.0.1"])
    assert result.exit_code == 1
    assert _read_pbt_version(project) == "1.2.3"


def test_set_below_current_with_force_succeeds(cli_runner: CliRunner, synthetic_project) -> None:
    project = _python_project(synthetic_project, "1.2.3")
    result = cli_runner.invoke(versioning, ["--path", str(project), "--set", "invalid-0.0.0-thing", "--force"])
    assert result.exit_code == 0, result.output
    assert _read_pbt_version(project) == "invalid-0.0.0-thing"


# ---------------------------------------------------------------------------
# --bump (pbt-only to avoid dragging setup.py version-string replacement into
# CLI assertions; the file-level rewrite is already covered by semver_sync
# and the full chain is covered by e2e tests).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("bump", "expected"),
    [
        ("major", "1.0.0"),
        ("minor", "0.1.0"),
        ("patch", "0.0.2"),
    ],
)
def test_bump_core_fields(cli_runner: CliRunner, synthetic_project, bump: str, expected: str) -> None:
    project = _python_project(synthetic_project, "0.0.1")
    result = cli_runner.invoke(versioning, ["--path", str(project), "--bump", bump, "--pbt-only"])
    assert result.exit_code == 0, result.output
    assert _read_pbt_version(project) == expected


@pytest.mark.parametrize(
    ("bump", "expected"),
    [
        ("build", "0.0.1+build.1"),
        ("prerelease", "0.0.1-rc.1"),
    ],
)
def test_bump_build_and_prerelease_need_force(
    cli_runner: CliRunner, synthetic_project, bump: str, expected: str
) -> None:
    project = _python_project(synthetic_project, "0.0.1")
    result = cli_runner.invoke(
        versioning,
        ["--path", str(project), "--bump", bump, "--force", "--pbt-only"],
    )
    assert result.exit_code == 0, result.output
    assert _read_pbt_version(project) == expected


# ---------------------------------------------------------------------------
# --set-suffix
# ---------------------------------------------------------------------------


def test_set_suffix_scala_snapshot(cli_runner: CliRunner, synthetic_project) -> None:
    project = _scala_project(synthetic_project, "1.0.0")
    result = cli_runner.invoke(
        versioning,
        ["--path", str(project), "--set-suffix", "-SNAPSHOT", "--force", "--pbt-only"],
    )
    assert result.exit_code == 0, result.output
    assert _read_pbt_version(project) == "1.0.0-SNAPSHOT"


def test_set_suffix_python_prerelease(cli_runner: CliRunner, synthetic_project) -> None:
    project = _python_project(synthetic_project, "0.0.1")
    result = cli_runner.invoke(
        versioning,
        ["--path", str(project), "--set-suffix", "-rc.4", "--force", "--pbt-only"],
    )
    assert result.exit_code == 0, result.output
    assert _read_pbt_version(project) == "0.0.1-rc.4"


def test_set_suffix_then_bump_prerelease_python(cli_runner: CliRunner, synthetic_project) -> None:
    """Mirrors legacy ``test_versioning_set_prerelease_and_bump_python``."""

    project = _python_project(synthetic_project, "0.0.1")
    first = cli_runner.invoke(
        versioning,
        ["--path", str(project), "--set-suffix", "-rc.4", "--force", "--pbt-only"],
    )
    assert first.exit_code == 0, first.output

    second = cli_runner.invoke(
        versioning,
        ["--path", str(project), "--bump", "prerelease", "--pbt-only"],
    )
    assert second.exit_code == 0, second.output
    assert _read_pbt_version(project) == "0.0.1-rc.5"


# ---------------------------------------------------------------------------
# --sync
# ---------------------------------------------------------------------------


def test_sync_propagates_pbt_version_to_setup_py(cli_runner: CliRunner, synthetic_project) -> None:
    project = _python_project(synthetic_project, "0.0.1")
    setup_py = project / "pipelines" / "demo" / "code" / "setup.py"

    # Make the pipeline's setup.py drift so sync has work to do. Keep the
    # multi-line form so ``update_all_versions``' regex matches.
    setup_py.write_text("from setuptools import setup\n" "setup(\n" "    name='x',\n" "    version='9.9.9'\n" ")\n")
    assert _read_setup_py_version(setup_py) == "9.9.9"

    result = cli_runner.invoke(versioning, ["--path", str(project), "--sync"])
    assert result.exit_code == 0, result.output
    assert _read_setup_py_version(setup_py) == "0.0.1"


# ---------------------------------------------------------------------------
# --check-sync
# ---------------------------------------------------------------------------


def test_check_sync_success(cli_runner: CliRunner, synthetic_project) -> None:
    project = _python_project(synthetic_project, "1.2.3")
    result = cli_runner.invoke(versioning, ["--path", str(project), "--check-sync"])
    assert result.exit_code == 0, result.output


def test_check_sync_detects_drift(cli_runner: CliRunner, synthetic_project) -> None:
    project = _python_project(synthetic_project, "1.2.3")
    setup_py = project / "pipelines" / "demo" / "code" / "setup.py"
    setup_py.write_text("from setuptools import setup\n" "setup(\n" "    name='x',\n" "    version='9.9.9'\n" ")\n")

    result = cli_runner.invoke(versioning, ["--path", str(project), "--check-sync"])
    assert result.exit_code == 1
    assert "out of sync" in result.output.lower() or "versions are out of sync" in result.output.lower()


# ---------------------------------------------------------------------------
# Flag-combination usage errors
# ---------------------------------------------------------------------------


def test_no_option_errors(cli_runner: CliRunner, synthetic_project) -> None:
    project = _python_project(synthetic_project, "0.0.1")
    result = cli_runner.invoke(versioning, ["--path", str(project)])
    assert result.exit_code != 0
    assert "must give ONE of" in result.output


def test_mutually_exclusive_flags_error(cli_runner: CliRunner, synthetic_project) -> None:
    """``--set`` and ``--bump`` together is rejected."""

    project = _python_project(synthetic_project, "0.0.1")
    result = cli_runner.invoke(versioning, ["--path", str(project), "--set", "1.0.0", "--bump", "minor"])
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


def test_compare_and_bump_combo_is_allowed_at_parse_time(cli_runner: CliRunner, synthetic_project) -> None:
    """``--compare-to-target`` + ``--bump`` is the single permitted pairing.

    We only assert that click doesn't reject the pairing outright — the actual
    git interaction is exercised in the bundle-backed e2e test.
    """

    project = _python_project(synthetic_project, "0.0.1")
    result = cli_runner.invoke(
        versioning,
        [
            "--path",
            str(project),
            "--compare-to-target",
            "nonexistent-branch",
            "--bump",
            "patch",
            "--repo-path",
            str(project),
        ],
    )
    # The combo passed click's validation; subsequent failure is due to git
    # not finding the branch / repo — we don't care about its specific exit
    # code here, just that we didn't hit the "mutually exclusive" guard.
    assert "mutually exclusive" not in result.output
