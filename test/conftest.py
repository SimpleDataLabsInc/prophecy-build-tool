"""Shared pytest fixtures and configuration for PBT tests.

Responsibilities:

- Register the project ``test/resources/ivysettings.xml`` into ``SPARK_CONFIG_JSON``
  so Spark sessions spawned by concurrent tests share ``~/.ivy2/cache`` safely
  (see PR #109).
- Provide per-test env isolation via an autouse fixture so tests that mutate
  environment variables (directly or through product code) cannot leak to
  other tests in the same xdist worker.
- Offer reusable fixtures: ``cli_runner``, ``sample_project``, ``sample_repo``.

The legacy tests kept for the refactor (``test_build.py``, ``test_deploy.py``,
``test_testing.py``, ``test_versioning.py``, ``test_tagging.py``,
``test_pipeline_sync.py``, ``test_utils.py``) remain untouched except for a
``pytestmark = pytest.mark.legacy`` marker applied from the bottom of this file.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Iterator

import pytest
from click.testing import CliRunner

_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent
_RESOURCES = _HERE / "resources"

# Match existing tests which import via ``from src.pbt import ...`` — the
# repo-root layout puts ``src`` on sys.path when pytest is run from the top
# level, so no explicit sys.path mutation is needed.
#
# We do add ``test/`` to sys.path so new v2 tests can ``from fakes import FakeRunner``
# without needing ``test/__init__.py`` (which the legacy tests don't use).
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


def pytest_configure(config: pytest.Config) -> None:
    """Register ivysettings and force deterministic CLI output."""

    ivysettings_file = _RESOURCES / "ivysettings.xml"
    if ivysettings_file.exists():
        spark_config = json.loads(os.environ.get("SPARK_CONFIG_JSON", "{}"))
        spark_config.setdefault("spark.jars.ivySettings", str(ivysettings_file))
        os.environ["SPARK_CONFIG_JSON"] = json.dumps(spark_config)

    # Stable CLI rendering for any remaining stdout assertions. Note we must
    # NOT set COLUMNS here: legacy tests assert on Rich's 80-col word wrap
    # (see ``_isolate_env`` which actively clears COLUMNS per test).
    os.environ.setdefault("NO_COLOR", "1")


# ---------------------------------------------------------------------------
# Env-var isolation
# ---------------------------------------------------------------------------

_TRACKED_ENV_VARS = (
    "SPARK_JARS_CONFIG",
    "SPARK_CONFIG_JSON",
    "SCALA_VERSION",
    "FABRIC_NAME",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "ARTIFACTORY_USERNAME",
    "ARTIFACTORY_PASSWORD",
)


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Snapshot and restore env vars that PBT product code reads from ``os.environ``.

    Using ``monkeypatch`` guarantees automatic restoration at teardown. This
    fixture is safe under ``pytest-xdist`` because workers are separate
    processes; tests within a worker run sequentially, so no cross-test leak
    is possible once we stop using module-level ``os.environ[...] = ...``.
    """

    # Default Databricks creds so deploy code paths do not explode when tests
    # exercise them without real secrets. Individual tests may override.
    monkeypatch.setenv("DATABRICKS_HOST", os.environ.get("DATABRICKS_HOST", "test"))
    monkeypatch.setenv("DATABRICKS_TOKEN", os.environ.get("DATABRICKS_TOKEN", "test"))

    # Pin Rich to its 80-column default regardless of the developer's terminal
    # or IDE. Legacy tests assert on substrings containing Rich's 80-col word
    # wrap (e.g. ``... (python),\njoin_agg_sort ...``); without this, those
    # assertions fail on any terminal wider than ~100 chars.
    monkeypatch.delenv("COLUMNS", raising=False)
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    yield


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Explicitly clear tracked env vars for tests that need a pristine environment."""

    for name in _TRACKED_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


# ---------------------------------------------------------------------------
# CLI fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_runner() -> CliRunner:
    """Click ``CliRunner`` for invoking commands in-process."""

    return CliRunner()


# ---------------------------------------------------------------------------
# Sample project fixtures (hybrid: vendored + isolated copy)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def resources_dir() -> Path:
    return _RESOURCES


@pytest.fixture
def hello_world_path(resources_dir: Path) -> str:
    """Path to the vendored HelloWorld sample project (read-only).

    Prefer :func:`sample_project` when a test mutates the project tree; this
    fixture is for tests that only read.
    """

    return str(resources_dir / "HelloWorld")


@pytest.fixture
def sample_project(tmp_path: Path, resources_dir: Path) -> Path:
    """Return a ``tmp_path``-scoped copy of ``HelloWorld`` for mutation-safe tests."""

    dest = tmp_path / "HelloWorld"
    shutil.copytree(resources_dir / "HelloWorld", dest)
    return dest


@pytest.fixture
def empty_project(tmp_path: Path) -> Path:
    """Tiny synthetic project used for validator/unit tests that only need a pbt_project.yml."""

    project = tmp_path / "tiny_project"
    project.mkdir()
    (project / "pbt_project.yml").write_text(
        "name: tiny_project\n" "language: python\n" "version: 0.0.1\n" "pipelines: {}\n" "jobs: {}\n"
    )
    return project


# ---------------------------------------------------------------------------
# Per-worker Maven / Ivy cache hints
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _per_worker_maven_repo(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """Give each xdist worker its own ``-Dmaven.repo.local`` path.

    This belts-and-braces the Ivy lock fix for xdist runs in CI where multiple
    Maven invocations may race for the same local repository.
    """

    worker = os.environ.get("PYTEST_XDIST_WORKER")  # "gw0", "gw1", ... or None for master
    if worker:
        repo = tmp_path_factory.mktemp(f"m2-{worker}")
        existing = os.environ.get("MAVEN_OPTS", "")
        if "maven.repo.local" not in existing:
            os.environ["MAVEN_OPTS"] = f"{existing} -Dmaven.repo.local={repo}".strip()
    yield
