"""Unit tests for ``PBTCli.test``'s driver-library-path handling.

The interesting v2 behavior is that ``driver_library_path`` is translated
into ``SPARK_JARS_CONFIG`` before the underlying test runner is invoked.
We patch ``ProjectDeployment.test`` so no Spark or pytest is executed.

Covers:
- a directory argument → comma-joined .jar files in SPARK_JARS_CONFIG
- a single file argument → absolute path in SPARK_JARS_CONFIG
- a comma-separated file list → absolute paths in SPARK_JARS_CONFIG
- a bad path in a comma-separated list → ``ValueError``
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.pbt.pbt_cli import PBTCli
from src.pbt.deployment import project as project_module

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


@pytest.fixture
def python_project(tmp_path: Path) -> Path:
    project = tmp_path / "driver-path-test"
    project.mkdir()
    (project / "pbt_project.yml").write_text(
        "name: driver-path-test\n" "language: python\n" "version: 0.0.1\n" "pipelines: {}\n" "jobs: {}\n"
    )
    return project


@pytest.fixture(autouse=True)
def _stub_deployment_test(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent ``PBTCli.test`` from actually trying to run Spark / pytest."""

    monkeypatch.setattr(project_module.ProjectDeployment, "test", lambda self: None)


def test_directory_path_sets_jar_list(python_project: Path, tmp_path: Path) -> None:
    jars_dir = tmp_path / "jars"
    jars_dir.mkdir()
    (jars_dir / "a.jar").touch()
    (jars_dir / "b.jar").touch()
    (jars_dir / "not_a_jar.txt").touch()

    pbt = PBTCli.from_conf_folder(str(python_project))
    pbt.test(driver_library_path=str(jars_dir))

    import os as _os

    entries = _os.environ["SPARK_JARS_CONFIG"].split(",")
    assert sorted(Path(p).name for p in entries) == ["a.jar", "b.jar"]
    assert all(Path(p).is_absolute() for p in entries)


def test_single_file_path_sets_absolute_path(python_project: Path, tmp_path: Path) -> None:
    jar = tmp_path / "one.jar"
    jar.touch()

    pbt = PBTCli.from_conf_folder(str(python_project))
    pbt.test(driver_library_path=str(jar))

    import os as _os

    assert _os.environ["SPARK_JARS_CONFIG"] == str(jar.resolve())


def test_comma_separated_files_sets_abs_paths(python_project: Path, tmp_path: Path) -> None:
    a = tmp_path / "a.jar"
    b = tmp_path / "b.jar"
    a.touch()
    b.touch()

    pbt = PBTCli.from_conf_folder(str(python_project))
    pbt.test(driver_library_path=f"{a},{b}")

    import os as _os

    entries = _os.environ["SPARK_JARS_CONFIG"].split(",")
    assert [Path(p).name for p in entries] == ["a.jar", "b.jar"]
    assert all(Path(p).is_absolute() for p in entries)


def test_comma_separated_list_with_missing_file_raises(python_project: Path, tmp_path: Path) -> None:
    real = tmp_path / "real.jar"
    real.touch()
    missing = tmp_path / "not_here.jar"

    pbt = PBTCli.from_conf_folder(str(python_project))
    with pytest.raises(ValueError):
        pbt.test(driver_library_path=f"{real},{missing}")


def test_empty_driver_path_leaves_env_empty(python_project: Path, clean_env) -> None:
    # ``clean_env`` strips SPARK_JARS_CONFIG so we can observe the default branch.
    pbt = PBTCli.from_conf_folder(str(python_project))
    pbt.test(driver_library_path=None)

    import os as _os

    assert _os.environ["SPARK_JARS_CONFIG"] == ""
