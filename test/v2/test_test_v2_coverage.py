"""Integration test covering ``test_v2``'s coverage + report artefact generation.

Replaces both legacy coverage tests:

- ``test_test_coverage_and_test_report_generation`` (legacy ``pbt test``)
- ``test_test_v2_coverage_and_test_report_generation`` (``pbt test_v2``)

Strategy:

1. Build a synthetic Python project with one pipeline whose
   ``test/TestSuite.py`` contains a single trivially-passing test.
2. Patch out the slow prelude — ``get_python_dependencies`` /
   ``get_maven_dependencies_python`` — so the test doesn't need to shell out
   to ``pip install`` / ``mvn`` just to run one assert.
3. Invoke ``PBTCli.test`` directly (equivalent to ``pbt test_v2``).
4. Assert the three expected artefacts exist and are well-formed:
   ``.coveragerc``, ``coverage.xml``, ``report.xml``. (Unlike the legacy
   ``pbt test`` CLI, ``test_v2`` does not pass ``--html`` so there is no
   ``report.html`` to assert on.)

With the prelude patched out this takes ~1s — one real pytest subprocess,
which is the minimum we can do while still exercising the product code
path that writes these files.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.pbt.deployment import pipeline as pipeline_module
from src.pbt.pbt_cli import PBTCli

pytestmark = [pytest.mark.integration, pytest.mark.v2, pytest.mark.fast]


_TESTSUITE_STUB = "def test_always_passes():\n    assert 1 == 1\n"
_SETUP_PY_STUB = "from setuptools import setup\n" "setup(name='pbt_cov_probe', version='0.0.1', packages=['job'])\n"
_MODULE_INIT = ""
_MODULE_BODY = "def add(a, b):\n    return a + b\n"


def _write_pipeline(project: Path, pid: str) -> Path:
    code = project / pid / "code"
    code.mkdir(parents=True, exist_ok=True)
    (code / "setup.py").write_text(_SETUP_PY_STUB)
    # Give coverage something real to measure so coverage.xml isn't empty.
    module = code / "job"
    module.mkdir(exist_ok=True)
    (module / "__init__.py").write_text(_MODULE_INIT)
    (module / "utils.py").write_text(_MODULE_BODY)
    # pytest TestSuite.py imports the module under test so coverage records.
    test_dir = code / "test"
    test_dir.mkdir(exist_ok=True)
    (test_dir / "__init__.py").write_text("")
    (test_dir / "TestSuite.py").write_text(
        "from job.utils import add\n\n" "def test_add_ok():\n    assert add(1, 2) == 3\n"
    )
    return code


def test_test_v2_writes_coverage_and_reports(
    tmp_path: Path, synthetic_project, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = synthetic_project(
        name="covproj",
        language="python",
        pipelines=[("pipelines/probe", "probe")],
        jobs=[
            {
                "id": "jobs/smoke",
                "name": "smoke",
                "fabric": "1",
                "scheduler": "Databricks",
                "pipeline_ids": ["pipelines/probe"],
            }
        ],
        with_databricks_json=True,
    )
    code = _write_pipeline(project, "pipelines/probe")

    # Skip the expensive dep install that wheel_test would otherwise run.
    monkeypatch.setattr(
        pipeline_module.PackageBuilderAndUploader,
        "get_python_dependencies",
        lambda self: None,
    )
    monkeypatch.setattr(
        pipeline_module.PackageBuilderAndUploader,
        "get_maven_dependencies_python",
        lambda self: None,
    )

    pbt = PBTCli.from_conf_folder(str(project))
    pbt.test("")

    # Three artefacts must exist at the pipeline's code directory. Note
    # ``test_v2`` does not pass ``--html``; that only happens on legacy
    # ``pbt test``.
    assert (code / ".coveragerc").is_file()
    assert (code / "coverage.xml").is_file()
    assert (code / "report.xml").is_file()

    coverage_content = (code / "coverage.xml").read_text()
    # ``.coveragerc`` sets relative_files=True; ensure source stanza is present.
    assert "<sources>" in coverage_content or "<source>" in coverage_content
    # ``job`` is the only module we wrote; it should be covered.
    assert 'name="job' in coverage_content
    # setup.py is excluded via .coveragerc's ``omit=`` rule.
    assert 'name="setup.py"' not in coverage_content
