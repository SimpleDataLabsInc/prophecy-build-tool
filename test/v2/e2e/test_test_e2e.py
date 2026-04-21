"""End-to-end ``pbt test_v2`` tests against the HelloProphecy projects.

The unit / integration lane already covers everything except the
"does the whole pipeline run end-to-end on a realistic project" check:

- Driver-library-path resolution           — :mod:`test.v2.test_test_v2_driver_path`
- Pipeline-filter plumbing (legacy CLI)    — :mod:`test.v2.test_test_v2_units`
- Coverage / report artefact generation    — :mod:`test.v2.test_test_v2_coverage`
  (asserts ``.coveragerc``, ``coverage.xml`` and ``report.xml`` are
  written, and that ``coverage.xml`` actually records the module under
  test). Cheap: ~1 second on a synthetic 1-pipeline project.
- ``--path`` resolution (absolute / relative / cwd) is plain Click
  plumbing — exercised by every other CLI invocation in the suite, no
  dedicated e2e needed.

This file's only job is to prove the whole ``test_v2`` pipeline runs
end-to-end against a realistic project (``test/resources/HelloProphecy``,
the vendored copy of ``prophecy-samples/HelloProphecy``). One run per
language, no filters, minimal assertions — the goal is signal that the
project's pipelines tested cleanly, not pattern-match on every log line.

Python ↔ Scala parity
---------------------

Run-the-whole-thing is identical for both languages, so the body is
parametrized. The scala variant carries ``pytest.mark.legacy`` (scala
is deprecated) so it shows up in both the legacy CI lane and the slow
e2e lane, but never in the v2 fast PR lane — see ``test/conftest.py``
for the marker policy.

Legacy files this consolidates:

- ``test_test_v2_path_default``
- ``test_test_v2_path_relative``      (dropped — pure plumbing)
- ``test_test_path_default``          (legacy ``pbt test`` — still in the
                                       legacy lane via ``pytest.mark.legacy``)
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from click.testing import CliRunner

from src.pbt import test_v2 as _test_v2_cli  # aliased so pytest ignores it

pytestmark = [pytest.mark.e2e, pytest.mark.v2, pytest.mark.spark]


# Same parametrize axis as ``test_build_e2e`` and ``test_deploy_e2e``.
LANGUAGES = [
    pytest.param("python", id="python"),
    pytest.param("scala", id="scala", marks=pytest.mark.legacy),
]


def _assert_found_some_pipelines(output: str) -> None:
    match = re.search(r"Found (\d+) pipelines", output)
    assert match, f"Expected 'Found N pipelines' in output, got:\n{output}"
    assert int(match.group(1)) > 0, f"Found 0 pipelines in:\n{output}"


@pytest.mark.parametrize("language", LANGUAGES)
def test_test_v2_helloprophecy_runs_all_pipelines(
    cli_runner: CliRunner,
    helloprophecy_repo: tuple[Path, Path, Path],
    language: str,
) -> None:
    """One ``pbt test_v2`` run against HelloProphecy — should exit 0.

    The coverage-artefact half of this assertion lives in
    :mod:`test.v2.test_test_v2_coverage` (faster, runs in the PR fast
    lane) — this e2e only needs to confirm that the CLI ran the whole
    project to completion in both languages.
    """

    _, python_project, scala_project = helloprophecy_repo
    project = python_project if language == "python" else scala_project

    result = cli_runner.invoke(_test_v2_cli, ["--path", str(project)])
    assert result.exit_code == 0, result.output
    _assert_found_some_pipelines(result.output)
    assert "Testing pipelines" in result.output
