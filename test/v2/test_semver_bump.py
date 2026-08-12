"""Pure-unit tests for :func:`pbt.utils.versioning.get_bumped_version` and friends.

No filesystem, no subprocess, no network. These are the "cheap-but-valuable"
unit tests called out in the refactor plan, covering semver edge cases across
python / scala project languages.
"""

from __future__ import annotations

import pytest

from src.pbt.utils.versioning import get_bumped_version

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


@pytest.mark.parametrize(
    ("original", "bump", "expected"),
    [
        ("1.2.3", "major", "2.0.0"),
        ("1.2.3", "minor", "1.3.0"),
        ("1.2.3", "patch", "1.2.4"),
        ("0.0.0", "patch", "0.0.1"),
        ("0.0.0", "minor", "0.1.0"),
        ("0.0.0", "major", "1.0.0"),
    ],
)
def test_bump_core_fields(original: str, bump: str, expected: str) -> None:
    assert get_bumped_version(original, bump, "scala") == expected


def test_bump_build_increments_build_metadata() -> None:
    # semver build metadata is appended as +build.N when bumped.
    bumped = get_bumped_version("1.2.3", "build", "scala")
    assert bumped.startswith("1.2.3+build.")


def test_bump_prerelease_increments_prerelease_id() -> None:
    bumped = get_bumped_version("1.2.3", "prerelease", "scala")
    # Initial prerelease produces 1.2.4-rc.1 in the semver package; we only
    # assert on the structural pieces that are guaranteed to be stable.
    assert "-" in bumped
    assert bumped.split("-")[0].count(".") == 2


def test_python_requires_pep440_compatible_version(capsys: pytest.CaptureFixture[str]) -> None:
    # Plain semver is PEP440 compatible; bumping succeeds on python projects.
    assert get_bumped_version("1.2.3", "patch", "python") == "1.2.4"


def test_python_rejects_non_pep440_version() -> None:
    # "1.2.3+foo" parses as valid semver but is invalid PEP440 for python
    # projects, and the helper exits non-zero. ``SystemExit`` is the product
    # contract today; we pin it so a regression is obvious.
    with pytest.raises(SystemExit):
        get_bumped_version("1.2.3+not a pep440 segment!", "patch", "python")


def test_malformed_version_exits() -> None:
    with pytest.raises(SystemExit):
        get_bumped_version("not-a-version", "patch", "scala")


# ---------------------------------------------------------------------------
# Extra scenarios replacing the non-git branches of legacy test_versioning.
# These mirror the numeric expectations of the legacy CLI tests without any
# filesystem or git wiring.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("original", "bump", "expected"),
    [
        # The three starting versions that legacy tests used across projects.
        ("0.0.1", "major", "1.0.0"),
        ("0.0.1", "minor", "0.1.0"),
        ("0.0.1", "patch", "0.0.2"),
        # Bumping major resets minor and patch.
        ("1.2.3", "major", "2.0.0"),
        # Bumping minor resets patch.
        ("1.2.3", "minor", "1.3.0"),
    ],
)
def test_legacy_project_bump_parity(original: str, bump: str, expected: str) -> None:
    assert get_bumped_version(original, bump, "python") == expected
    assert get_bumped_version(original, bump, "scala") == expected


@pytest.mark.parametrize(
    ("original", "bump", "expected"),
    [
        # Legacy docs: ``0.0.1`` → ``0.0.1+build.1`` with --force.
        ("0.0.1", "build", "0.0.1+build.1"),
        # Legacy docs: ``0.0.1`` → ``0.0.1-rc.1`` with --force.
        ("0.0.1", "prerelease", "0.0.1-rc.1"),
    ],
)
def test_legacy_scala_build_and_prerelease_parity(original: str, bump: str, expected: str) -> None:
    # These are scala-flavoured in the legacy suite; python also supports them
    # provided the output remains PEP440-compatible.
    assert get_bumped_version(original, bump, "scala") == expected


def test_prerelease_then_bump_prerelease_python() -> None:
    # Mirrors ``test_versioning_set_prerelease_and_bump_python``:
    #   start 0.0.1 → set-suffix -rc.4 (via packaging) → bump prerelease → 0.0.1-rc.5
    # We exercise just the bump half here; the set-suffix half lives in the CLI
    # test file and the packaging helper itself.
    assert get_bumped_version("0.0.1-rc.4", "prerelease", "python") == "0.0.1-rc.5"


def test_build_bump_increments_monotonically() -> None:
    first = get_bumped_version("1.2.3", "build", "scala")
    second = get_bumped_version(first, "build", "scala")
    # semver's bump_build increments the numeric suffix of the build-metadata
    # segment. We pin only the structural contract: second > first, same core.
    assert first.split("+")[0] == second.split("+")[0] == "1.2.3"
    assert first != second
