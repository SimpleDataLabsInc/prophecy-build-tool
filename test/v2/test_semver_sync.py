"""Unit tests for :func:`pbt.utils.versioning.version_check_sync`.

Exercises the python / scala / sql branches against minimal synthetic projects
in ``tmp_path`` — no git, no real subprocess, sub-second.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.pbt.utils.versioning import version_check_sync

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


def _write_pbt_project(path: Path, version: str, language: str) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    (path / "pbt_project.yml").write_text(f"name: demo\nlanguage: {language}\nversion: {version}\n")
    return path


def test_python_in_sync(tmp_path: Path) -> None:
    project = _write_pbt_project(tmp_path, "1.2.3", "python")
    (project / "pipelines").mkdir()
    (project / "pipelines" / "setup.py").write_text("setup(\n    name='demo',\n    version='1.2.3'\n)\n")

    # Should not raise / exit.
    version_check_sync(str(project), "python", "1.2.3")


def test_python_mismatch_exits(tmp_path: Path) -> None:
    project = _write_pbt_project(tmp_path, "1.2.3", "python")
    (project / "pipelines").mkdir()
    (project / "pipelines" / "setup.py").write_text("setup(\n    name='demo',\n    version='9.9.9'\n)\n")

    with pytest.raises(SystemExit):
        version_check_sync(str(project), "python", "1.2.3")


def test_bad_language_raises(tmp_path: Path) -> None:
    project = _write_pbt_project(tmp_path, "1.2.3", "python")
    with pytest.raises(ValueError):
        version_check_sync(str(project), "cobol", "1.2.3")


# ---------------------------------------------------------------------------
# Extra coverage: scala pom.xml and sql dbt_project.yml code paths.
# ---------------------------------------------------------------------------


_POM_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.demo</groupId>
  <artifactId>demo</artifactId>
  <version>{version}</version>
</project>
"""


def test_scala_in_sync(tmp_path: Path) -> None:
    project = _write_pbt_project(tmp_path, "1.2.3", "scala")
    (project / "pipelines").mkdir()
    (project / "pipelines" / "pom.xml").write_text(_POM_TEMPLATE.format(version="1.2.3"))

    version_check_sync(str(project), "scala", "1.2.3")


def test_scala_mismatch_exits(tmp_path: Path) -> None:
    project = _write_pbt_project(tmp_path, "1.2.3", "scala")
    (project / "pipelines").mkdir()
    (project / "pipelines" / "pom.xml").write_text(_POM_TEMPLATE.format(version="9.9.9"))

    with pytest.raises(SystemExit):
        version_check_sync(str(project), "scala", "1.2.3")


def test_sql_in_sync(tmp_path: Path) -> None:
    project = _write_pbt_project(tmp_path, "1.2.3", "sql")
    (project / "pipelines").mkdir()
    (project / "pipelines" / "dbt_project.yml").write_text("name: demo\nversion: '1.2.3'\n")

    version_check_sync(str(project), "sql", "1.2.3")


def test_sql_mismatch_exits(tmp_path: Path) -> None:
    project = _write_pbt_project(tmp_path, "1.2.3", "sql")
    (project / "pipelines").mkdir()
    (project / "pipelines" / "dbt_project.yml").write_text("name: demo\nversion: '9.9.9'\n")

    with pytest.raises(SystemExit):
        version_check_sync(str(project), "sql", "1.2.3")


def test_python_setup_missing_version_string_raises(tmp_path: Path) -> None:
    """``setup.py`` that doesn't contain a ``version=`` line raises ValueError."""

    project = _write_pbt_project(tmp_path, "1.2.3", "python")
    (project / "pipelines").mkdir()
    (project / "pipelines" / "setup.py").write_text("setup(\n    name='demo'\n)\n")

    with pytest.raises(ValueError, match="could not find version"):
        version_check_sync(str(project), "python", "1.2.3")


def test_multiple_files_all_in_sync(tmp_path: Path) -> None:
    """A project with multiple pipelines all at the same version passes."""

    project = _write_pbt_project(tmp_path, "1.2.3", "python")
    for pipeline_id in ("pipelineA", "pipelineB", "pipelineC"):
        pipeline_dir = project / "pipelines" / pipeline_id / "code"
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "setup.py").write_text("setup(\n    name='x',\n    version='1.2.3'\n)\n")

    version_check_sync(str(project), "python", "1.2.3")


def test_one_of_many_out_of_sync_exits(tmp_path: Path) -> None:
    """If *any* pipeline is out of sync, version_check_sync exits non-zero."""

    project = _write_pbt_project(tmp_path, "1.2.3", "python")
    for pipeline_id in ("pipelineA", "pipelineB"):
        d = project / "pipelines" / pipeline_id / "code"
        d.mkdir(parents=True)
        (d / "setup.py").write_text("setup(\n    name='x',\n    version='1.2.3'\n)\n")
    bad = project / "pipelines" / "pipelineC" / "code"
    bad.mkdir(parents=True)
    (bad / "setup.py").write_text("setup(\n    name='x',\n    version='0.0.1'\n)\n")

    with pytest.raises(SystemExit):
        version_check_sync(str(project), "python", "1.2.3")
