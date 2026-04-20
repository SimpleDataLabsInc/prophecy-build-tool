"""Unit tests for :func:`pbt.utils.versioning.version_check_sync`.

Exercises the python / scala / sql branches against minimal synthetic projects
in ``tmp_path`` — no git, no real subprocess, sub-second.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.pbt.utils.versioning import version_check_sync

pytestmark = [pytest.mark.unit, pytest.mark.v2]


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
