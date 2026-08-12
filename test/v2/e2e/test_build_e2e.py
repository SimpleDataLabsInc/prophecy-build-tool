"""Consolidated end-to-end ``pbt build_v2`` tests for HelloProphecy.

Replaces the split assertions in legacy ``test/test_build.py``:

- ``test_build_v2_path_default``
- ``test_build_v2_path_default_build_errors``
- ``test_build_v2_path_default_build_errors_ignore_errors``

and the broader legacy ``test_build_path_*`` family (pipeline filtering),
which are owned by :mod:`test.v2.test_build_v2_units` (fake builder).

The legacy ``test_build_v2_binary_check`` (no python on PATH → exit 1)
is **not** ported here — covered by :mod:`test.v2.test_python_probe` at
the unit lane through the ``CommandRunner`` seam in milliseconds.

Python ↔ Scala parity
---------------------

The happy path is parametrized over both languages so the python and
scala variants live side by side in one place. The only thing that
differs is the artefact location:

- python: ``setup.py bdist_wheel`` → ``<pipeline>/code/dist/*.whl``
- scala:  ``mvn package``          → ``<pipeline>/code/target/*.jar``

The scala variant carries ``pytest.mark.legacy`` (scala is deprecated)
so it shows up in both the legacy CI lane and the slow e2e lane, but
never in the v2 fast PR lane — see ``test/conftest.py`` for the marker
policy.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from click.testing import CliRunner

from src.pbt import build_v2

pytestmark = [pytest.mark.e2e, pytest.mark.v2, pytest.mark.maven]


_HERE = Path(__file__).resolve().parents[2]
HELLO_WORLD_BUILD_ERROR = _HERE / "resources" / "HelloWorldBuildError"


# Parametrize axis shared with ``test_test_e2e`` and ``test_deploy_e2e``.
# ``pytest.param(..., marks=…)`` applies the marker to *just* that param,
# so the scala instance carries ``legacy`` while python doesn't.
LANGUAGES = [
    pytest.param("python", id="python"),
    pytest.param("scala", id="scala", marks=pytest.mark.legacy),
]


def _require_fixture(path: Path) -> None:
    if not path.exists():
        pytest.skip(f"Fixture missing: {path}")


def _assert_found_some_pipelines(output: str) -> None:
    match = re.search(r"Found (\d+) pipelines", output)
    assert match, f"Expected 'Found N pipelines' in output, got:\n{output}"
    assert int(match.group(1)) > 0, f"Found 0 pipelines in:\n{output}"


def _list_built_artefacts(project: Path, language: str) -> tuple[list[str], list[Path]]:
    """Return ``(missing_pipelines, found_artefacts)``.

    A pipeline is considered missing if its ``code/dist`` (python) or
    ``code/target`` (scala) directory has no usable artefact. For scala
    we ignore ``original-*.jar`` (the unshaded copy the maven-shade
    plugin keeps as a sibling).
    """

    pipelines_root = project / "pipelines"
    pipeline_dirs = sorted(p for p in pipelines_root.iterdir() if p.is_dir())
    assert pipeline_dirs, f"No pipelines under {pipelines_root}"

    missing: list[str] = []
    found: list[Path] = []
    for pipeline in pipeline_dirs:
        if language == "python":
            artefact_dir = pipeline / "code" / "dist"
            artefacts = list(artefact_dir.glob("*.whl")) if artefact_dir.exists() else []
        else:  # scala
            artefact_dir = pipeline / "code" / "target"
            artefacts = (
                [j for j in artefact_dir.glob("*.jar") if not j.name.startswith("original-")]
                if artefact_dir.exists()
                else []
            )
        if artefacts:
            found.extend(artefacts)
        else:
            missing.append(str(pipeline.relative_to(project)))
    return missing, found


@pytest.mark.parametrize("language", LANGUAGES)
def test_build_v2_produces_artefact_per_pipeline(
    cli_runner: CliRunner,
    helloprophecy_repo: tuple[Path, Path, Path],
    language: str,
) -> None:
    """``build_v2`` must drop a wheel (python) or jar (scala) per pipeline.

    Without this artefact assertion the e2e would happily pass even if
    ``setup.py bdist_wheel`` / ``mvn package`` silently skipped — we
    have seen exactly that failure mode in PR #109.

    Per-pipeline filter checks are owned by
    :mod:`test.v2.test_build_v2_units` (fake builder, milliseconds).
    """

    _, python_project, scala_project = helloprophecy_repo
    project = python_project if language == "python" else scala_project

    result = cli_runner.invoke(build_v2, ["--path", str(project), "--use-uv"])
    assert result.exit_code == 0, result.output
    _assert_found_some_pipelines(result.output)

    missing, found = _list_built_artefacts(project, language)
    artefact_label = "wheel" if language == "python" else "jar"
    assert not missing, (
        f"build_v2 reported success but produced no .{artefact_label[0:3]} "
        f"artefact for these {language} pipelines:\n  - "
        + "\n  - ".join(missing)
        + f"\n\n--- build_v2 output ---\n{result.output}"
    )
    assert found, f"No {artefact_label} artefacts found across any pipeline"


def test_build_v2_reports_errors_by_default(cli_runner: CliRunner) -> None:
    """A failing python build should exit non-zero by default.

    Not parametrized over scala: the scala fixture has no pre-broken
    sibling and the error-handling lives above the language layer (in
    the orchestrator), so re-running this against a deliberately broken
    scala project would only re-test the same code path more slowly.
    """

    _require_fixture(HELLO_WORLD_BUILD_ERROR)
    result = cli_runner.invoke(build_v2, ["--path", str(HELLO_WORLD_BUILD_ERROR)])
    assert result.exit_code == 1, result.output


def test_build_v2_ignore_errors_flag_swallows_failures(
    cli_runner: CliRunner,
) -> None:
    _require_fixture(HELLO_WORLD_BUILD_ERROR)
    result = cli_runner.invoke(build_v2, ["--path", str(HELLO_WORLD_BUILD_ERROR), "--ignore-build-errors"])
    assert result.exit_code == 0, result.output
