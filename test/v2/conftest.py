"""Fixtures shared across v2 tests.

This is a local ``conftest.py`` for the ``test/v2/`` tree so individual files
don't have to re-declare the same synthetic-project / deploy-recorder helpers.

Fixtures:

- ``synthetic_project`` — factory that writes a minimal ``pbt_project.yml``
  with configurable pipelines + jobs into ``tmp_path``.
- ``fake_databricks_deploy`` — patches ``ProjectDeployment.deploy`` with a
  recorder so unit tests can assert the CLI would have deployed without
  actually talking to Databricks.
- ``git_bundle_repo`` — clones a vendored ``test/resources/*.bundle`` into
  ``tmp_path`` so versioning/tagging tests have a reproducible git history
  without hitting the network.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple

import pytest

from src.pbt.deployment.project import ProjectDeployment


# ---------------------------------------------------------------------------
# Synthetic project factory
# ---------------------------------------------------------------------------


@dataclass
class _SyntheticProjectSpec:
    """Minimal structural spec for building a synthetic pbt project tree."""

    name: str = "synthetic"
    language: str = "python"
    version: str = "0.0.1"
    # ``pipelines`` is a list of ``(pipeline_id, pipeline_name)`` tuples.
    pipelines: List[Tuple[str, str]] = field(default_factory=list)
    # ``jobs`` is a list of dicts: ``{"id": "jobs/job0", "name": "job0",
    # "fabric": "1", "pipeline_ids": [...], "scheduler": "Databricks"}``.
    jobs: List[dict] = field(default_factory=list)


@pytest.fixture
def synthetic_project(tmp_path: Path) -> Callable[..., Path]:
    """Factory fixture: call to produce a minimal pbt project tree.

    Usage::

        project = synthetic_project(
            pipelines=[("pipelines/alpha", "alpha"), ("pipelines/beta", "beta")],
            jobs=[{"id": "jobs/test-job", "name": "test-job", "fabric": "1",
                   "pipeline_ids": ["pipelines/alpha"], "scheduler": "Databricks"}],
        )
    """

    def _make(
        *,
        name: str = "synthetic",
        language: str = "python",
        version: str = "0.0.1",
        pipelines: Optional[List[Tuple[str, str]]] = None,
        jobs: Optional[List[dict]] = None,
        subdir: Optional[str] = None,
        with_databricks_json: bool = False,
    ) -> Path:
        spec = _SyntheticProjectSpec(
            name=name,
            language=language,
            version=version,
            pipelines=list(pipelines or []),
            jobs=list(jobs or []),
        )
        root = tmp_path / (subdir or name)
        root.mkdir(parents=True, exist_ok=True)

        pipelines_block = "".join(
            f"  {pid}:\n    name: {pname}\n    language: {spec.language}\n" for pid, pname in spec.pipelines
        )
        jobs_block = ""
        for job in spec.jobs:
            job_id = job["id"]
            job_name = job["name"]
            fabric = str(job.get("fabric", "1"))
            scheduler = job.get("scheduler", "Databricks")
            pipe_ids = job.get("pipeline_ids", [])
            pipe_refs = "".join(f"    - {pid}\n" for pid in pipe_ids)
            jobs_block += (
                f"  {job_id}:\n"
                f"    name: {job_name}\n"
                f"    fabricUID: '{fabric}'\n"
                f"    scheduler:\n      {scheduler}: {{}}\n"
                f"    pipelines:\n{pipe_refs}"
                if pipe_refs
                else (
                    f"  {job_id}:\n"
                    f"    name: {job_name}\n"
                    f"    fabricUID: '{fabric}'\n"
                    f"    scheduler:\n      {scheduler}: {{}}\n"
                    f"    pipelines: []\n"
                )
            )

        pipelines_yaml = f"pipelines:\n{pipelines_block}" if pipelines_block else "pipelines: {}\n"
        jobs_yaml = f"jobs:\n{jobs_block}" if jobs_block else "jobs: {}\n"
        (root / "pbt_project.yml").write_text(
            f"name: {spec.name}\n"
            f"language: {spec.language}\n"
            f"version: {spec.version}\n"
            f"{pipelines_yaml}"
            f"{jobs_yaml}"
        )

        # Create minimal pipeline code folders so project loaders don't trip.
        for pid, _ in spec.pipelines:
            code = root / pid / "code"
            code.mkdir(parents=True, exist_ok=True)
            if spec.language == "python":
                (code / "setup.py").write_text("from setuptools import setup\nsetup(name='x', version='0.0.1')\n")
            else:
                (code / "pom.xml").write_text(
                    "<project><modelVersion>4.0.0</modelVersion>"
                    "<groupId>g</groupId><artifactId>x</artifactId><version>1</version></project>"
                )

        # Create minimal job JSON stubs where requested.
        for job in spec.jobs:
            job_id = job["id"]
            code = root / job_id / "code"
            code.mkdir(parents=True, exist_ok=True)
            if with_databricks_json:
                (code / "databricks-job.json").write_text(
                    json.dumps(
                        {
                            "fabric_id": str(job.get("fabric", "1")),
                            "components": [],
                            "request": {"name": job["name"], "tasks": []},
                        }
                    )
                )
                (code / "prophecy-job.json").write_text(json.dumps({"processes": {}}))

        return root

    return _make


# ---------------------------------------------------------------------------
# Fake Databricks deploy recorder
# ---------------------------------------------------------------------------


@dataclass
class DeployCall:
    """One recorded invocation of ``ProjectDeployment.deploy``."""

    job_ids: Any
    # Snapshot of the deploy orchestrator at invocation time. Only the fields
    # unit tests care about are copied — full deepcopies of ProjectConfig are
    # expensive and not needed.
    mode: Any
    jobs_and_fabric: Any
    skip_builds: bool
    skip_pipeline_deploy: bool
    fabric_ids: List[str]


@dataclass
class FakeDeploy:
    calls: List[DeployCall] = field(default_factory=list)

    def reset(self) -> None:
        self.calls.clear()


@pytest.fixture
def fake_databricks_deploy(monkeypatch: pytest.MonkeyPatch) -> FakeDeploy:
    """Patch ``ProjectDeployment.deploy`` with a recorder.

    The recorder captures enough state from the ``ProjectDeployment`` instance
    to let tests assert on flag plumbing, filter application, and call count
    — without touching the Databricks HTTP layer.
    """

    tracker = FakeDeploy()

    def _fake_deploy(self: ProjectDeployment, job_ids):  # noqa: ANN001 - match real signature
        cfg = self.project_config
        co = cfg.configs_override
        tracker.calls.append(
            DeployCall(
                job_ids=job_ids,
                mode=co.mode,
                jobs_and_fabric=([(j.job_id, j.fabric_id) for j in co.jobs_and_fabric] if co.jobs_and_fabric else None),
                skip_builds=cfg.skip_builds,
                skip_pipeline_deploy=cfg.skip_pipeline_deploy,
                fabric_ids=[f.id for f in cfg.fabric_config.fabrics],
            )
        )
        return []

    monkeypatch.setattr(ProjectDeployment, "deploy", _fake_deploy, raising=True)
    return tracker


# ---------------------------------------------------------------------------
# Git bundle clone helper
# ---------------------------------------------------------------------------


_HERE = Path(__file__).resolve().parent
_BUNDLES_DIR = _HERE.parent / "resources"


@pytest.fixture
def git_bundle_repo(tmp_path: Path) -> Callable[[str], Path]:
    """Factory: clone ``test/resources/<name>.bundle`` into a temp dir.

    Returns the path to the freshly cloned working tree. The test owns the
    clone so mutations (commits, tags) won't leak between tests.
    """

    def _clone(bundle_name: str) -> Path:
        bundle_path = _BUNDLES_DIR / bundle_name
        if not bundle_path.exists():
            pytest.skip(f"Git bundle not found: {bundle_path}")
        dest = tmp_path / bundle_path.stem
        # ``git clone`` only checks out HEAD and creates remote-tracking refs
        # for the rest. Tests need to reference branches by bare name (e.g.
        # ``git show pytest/test_big_version:pbt_project.yml``), so we promote
        # every remote branch to a local branch after cloning.
        subprocess.run(
            ["git", "clone", str(bundle_path), str(dest)],
            check=True,
            capture_output=True,
        )
        # List every remote branch and create a matching local branch.
        result = subprocess.run(
            ["git", "-C", str(dest), "for-each-ref", "--format=%(refname:short)", "refs/remotes/origin"],
            check=True,
            capture_output=True,
            text=True,
        )
        for ref in result.stdout.splitlines():
            ref = ref.strip()
            if not ref or ref.endswith("/HEAD"):
                continue
            # ``origin/foo/bar`` → local ``foo/bar``.
            local = ref[len("origin/") :] if ref.startswith("origin/") else ref
            # Skip if the local branch already exists (e.g. the checked-out one).
            exists = subprocess.run(["git", "-C", str(dest), "show-ref", "--verify", "--quiet", f"refs/heads/{local}"])
            if exists.returncode == 0:
                continue
            subprocess.run(
                ["git", "-C", str(dest), "branch", local, ref],
                check=True,
                capture_output=True,
            )
        # Configure a deterministic user for any commits the test may make.
        subprocess.run(["git", "-C", str(dest), "config", "user.email", "pbt-test@example.com"], check=True)
        subprocess.run(["git", "-C", str(dest), "config", "user.name", "PBT Test"], check=True)
        return dest

    return _clone
