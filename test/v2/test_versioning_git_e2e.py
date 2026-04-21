"""Git-heavy versioning scenarios, replacing three legacy tests.

These used to fetch real origin branches in the prophecy-build-tool
repository and checked out ``pytest/test_big_version`` /
``pytest/test_small_version`` / ``pytest/test_bad_version`` /
``pytest/static_branch_name`` to drive comparisons. That design had two
problems:

1. Needed network + org-owned branches that can be force-pushed or deleted.
2. Mutated the developer's working tree (stash/pop) during tests.

We instead ship a pre-baked ``test/resources/versioning.bundle`` with the
same topology (see ``test/resources/bundles/build_versioning_bundle.sh``)
and clone it into ``tmp_path`` for each test — zero network, zero shared
state.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from src.pbt import versioning

# These tests clone a bundle on every case — cheap but not pure-unit. They
# carry the ``e2e`` marker so they only run on the v2-e2e lane, not the
# fast PR lane.
pytestmark = [pytest.mark.e2e, pytest.mark.v2]


def _read_pbt_version(project: Path) -> str:
    return yaml.safe_load((project / "pbt_project.yml").read_text())["version"]


# ---------------------------------------------------------------------------
# --compare-to-target (pure comparison, no bump)
# ---------------------------------------------------------------------------


def test_compare_to_bigger_target_returns_failure(
    cli_runner: CliRunner, git_bundle_repo
) -> None:
    repo = git_bundle_repo("versioning.bundle")
    # main is 0.0.1, target branch is 9999.0.0 → current is not greater.
    result = cli_runner.invoke(
        versioning,
        ["--path", str(repo), "--repo-path", str(repo), "--compare", "pytest/test_big_version"],
    )
    assert result.exit_code == 1, result.output


def test_compare_to_smaller_target_returns_success(
    cli_runner: CliRunner, git_bundle_repo
) -> None:
    repo = git_bundle_repo("versioning.bundle")
    # main is 0.0.1, target branch is 0.0.0 → current is strictly greater.
    result = cli_runner.invoke(
        versioning,
        ["--path", str(repo), "--repo-path", str(repo), "--compare", "pytest/test_small_version"],
    )
    assert result.exit_code == 0, result.output


def test_compare_to_malformed_target_returns_failure(
    cli_runner: CliRunner, git_bundle_repo
) -> None:
    repo = git_bundle_repo("versioning.bundle")
    result = cli_runner.invoke(
        versioning,
        ["--path", str(repo), "--repo-path", str(repo), "--compare", "pytest/test_bad_version"],
    )
    assert result.exit_code == 1, result.output


# ---------------------------------------------------------------------------
# --compare + --bump (the one permitted combo)
# ---------------------------------------------------------------------------


def test_compare_and_bump_against_bigger_branch_bumps_patch(
    cli_runner: CliRunner, git_bundle_repo
) -> None:
    repo = git_bundle_repo("versioning.bundle")
    # main is 0.0.1, target is 9999.0.0 — since current < target, the bump
    # strategy is applied on top of the target version, yielding 9999.0.1.
    result = cli_runner.invoke(
        versioning,
        [
            "--path",
            str(repo),
            "--repo-path",
            str(repo),
            "--compare",
            "pytest/test_big_version",
            "--bump",
            "patch",
        ],
    )
    assert result.exit_code == 0, result.output
    assert _read_pbt_version(repo) == "9999.0.1"


def test_compare_and_bump_against_smaller_branch_is_noop(
    cli_runner: CliRunner, git_bundle_repo
) -> None:
    """When current > target, ``--compare --bump`` leaves the version alone."""

    repo = git_bundle_repo("versioning.bundle")
    before = _read_pbt_version(repo)
    result = cli_runner.invoke(
        versioning,
        [
            "--path",
            str(repo),
            "--repo-path",
            str(repo),
            "--compare",
            "pytest/test_small_version",
            "--bump",
            "patch",
        ],
    )
    assert result.exit_code == 0, result.output
    assert _read_pbt_version(repo) == before


# ---------------------------------------------------------------------------
# --make-unique (deterministic sha-based suffix per branch name)
# ---------------------------------------------------------------------------


def test_make_unique_on_static_branch_produces_deterministic_version(
    cli_runner: CliRunner, git_bundle_repo
) -> None:
    import subprocess

    repo = git_bundle_repo("versioning.bundle")
    # Switch to the branch the legacy test pinned so the sha prefix is stable.
    subprocess.run(
        ["git", "-C", str(repo), "checkout", "pytest/static_branch_name"],
        check=True,
        capture_output=True,
    )

    result = cli_runner.invoke(
        versioning,
        ["--path", str(repo), "--repo-path", str(repo), "--make-unique"],
    )
    assert result.exit_code == 0, result.output

    new_version = _read_pbt_version(repo)
    # The legacy test asserted the exact value 0.0.1dev0+sha.062f87eb which is
    # the PEP440-ish python form: ``<base>dev0+sha.<8-char branch hash>``.
    # We replicate that assertion here for byte-for-byte parity.
    assert new_version == "0.0.1dev0+sha.062f87eb"
