"""Unit-style port of the pipeline-filter and not-found cases in ``test_testing.py``.

The filter option is only on the legacy ``test`` CLI (``test_v2`` takes no
``--pipelines``), so these tests drive the legacy command — but against a
tiny synthetic project with ``ProphecyBuildTool.test_python`` patched to a
no-op. That makes the entire run sub-second and portable, instead of the
minute-plus legacy tests that spin up Spark against HelloWorld.

Scenarios covered (ports ``test_test_with_pipeline_filter``,
``_one_notfound_pipeline``, ``_all_notfound_pipelines``):

- Valid filter of 2 → both pipelines are tested, unfiltered pipelines are not.
- 1 valid + 1 notfound → exits 1 with 'doesn't match' message.
- All notfound → exits 1 with 'doesn't match' message.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from src.pbt import test as legacy_test
from src.pbt import prophecy_build_tool as pbt_module

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


def _make_testable_project(synthetic_project, pipeline_names: list[str]) -> Path:
    """Synthetic project whose pipelines each have a ``test/TestSuite.py`` stub.

    The legacy ``test`` runner only invokes ``test_python`` if this file
    exists, so we create it so the filter path exercises normally.
    """

    pipelines = [(f"pipelines/{name}", name) for name in pipeline_names]
    project = synthetic_project(
        name="testproj",
        language="python",
        pipelines=pipelines,
        jobs=[
            {
                "id": "jobs/smoke",
                "name": "smoke",
                "fabric": "1",
                "scheduler": "Databricks",
                "pipeline_ids": [pid for pid, _ in pipelines],
            }
        ],
        with_databricks_json=True,
    )
    for pid, _ in pipelines:
        test_dir = project / pid / "code" / "test"
        test_dir.mkdir(parents=True, exist_ok=True)
        (test_dir / "TestSuite.py").write_text("")
    return project


@pytest.fixture
def fake_test_python(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Patch ``ProphecyBuildTool.test_python`` to record path and return 0."""

    recorded: list[str] = []

    def _fake(self, path_absolute, path_pipeline):  # noqa: ANN001
        recorded.append(path_pipeline)
        return 0

    monkeypatch.setattr(pbt_module.ProphecyBuildTool, "test_python", _fake)
    return recorded


def test_valid_filter_tests_only_matching_pipelines(cli_runner: CliRunner, synthetic_project, fake_test_python) -> None:
    """Ports legacy ``test_test_with_pipeline_filter``."""

    project = _make_testable_project(
        synthetic_project, ["customers_orders", "report_top_customers", "join_agg_sort", "farmers-markets-irs"]
    )
    result = cli_runner.invoke(
        legacy_test,
        ["--path", str(project), "--pipelines", "report_top_customers,join_agg_sort"],
    )
    assert result.exit_code == 0, result.output
    assert sorted(fake_test_python) == ["pipelines/join_agg_sort", "pipelines/report_top_customers"]


def test_partial_notfound_filter_exits_nonzero(cli_runner: CliRunner, synthetic_project, fake_test_python) -> None:
    """Ports legacy ``test_test_with_pipeline_filter_one_notfound_pipeline``."""

    project = _make_testable_project(synthetic_project, ["customers_orders", "report_top_customers"])
    result = cli_runner.invoke(legacy_test, ["--path", str(project), "--pipelines", "report_top_customers,notfound"])
    assert result.exit_code == 1
    assert "Filtered pipelines doesn't match with passed filter" in result.output
    # Filter exits before any pipeline is tested.
    assert fake_test_python == []


def test_all_notfound_filter_exits_nonzero(cli_runner: CliRunner, synthetic_project, fake_test_python) -> None:
    """Ports legacy ``test_test_with_pipeline_filter_all_notfound_pipelines``."""

    project = _make_testable_project(synthetic_project, ["customers_orders"])
    result = cli_runner.invoke(legacy_test, ["--path", str(project), "--pipelines", "nope1,nope2,nope3"])
    assert result.exit_code == 1
    assert "Filtered pipelines doesn't match with passed filter" in result.output
    assert fake_test_python == []
