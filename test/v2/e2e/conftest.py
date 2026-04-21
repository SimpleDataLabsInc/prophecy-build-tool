"""Shared fixtures + helpers for the v2 e2e lane.

Currently scopes the *real-Databricks* deploy plumbing used by both
:mod:`test.v2.e2e.test_deploy_e2e` (python) and
:mod:`test.v2.e2e.test_scala_e2e` (scala).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Callable, List

import pytest
import yaml

from src.pbt.utils.databricks_creds import (
    DatabricksCredentials,
    get_databricks_credentials,
)


# ---------------------------------------------------------------------------
# Real-Databricks opt-in
# ---------------------------------------------------------------------------


@pytest.fixture
def real_databricks_creds() -> DatabricksCredentials:
    """Skip unless real Databricks creds AND explicit opt-in are present.

    Two independent gates so a developer running ``pytest`` locally with a
    populated ``~/.databrickscfg`` doesn't accidentally have e2e tests
    create jobs in their workspace:

    1. ``PBT_E2E_DATABRICKS_OK=1`` — explicit acknowledgement.
    2. Working creds resolvable by
       :func:`src.pbt.utils.databricks_creds.get_databricks_credentials`
       (env vars, then ``~/.databrickscfg``).
    """

    if os.environ.get("PBT_E2E_DATABRICKS_OK") != "1":
        pytest.skip(
            "Real-Databricks deploy e2e is opt-in. Set PBT_E2E_DATABRICKS_OK=1 "
            "to allow this test to create (and clean up) Databricks jobs in "
            "the workspace your creds point at."
        )
    creds = get_databricks_credentials()
    if creds is None:
        pytest.skip(
            "No Databricks credentials configured. Set DATABRICKS_HOST + "
            "DATABRICKS_TOKEN or configure a ~/.databrickscfg profile."
        )
    return creds


# ---------------------------------------------------------------------------
# Job-name mutation helper (exposed as a fixture so the e2e files can
# pick it up via pytest's auto-discovery — ``test/v2/e2e`` isn't a
# package, so a plain ``from .conftest import …`` doesn't work.)
# ---------------------------------------------------------------------------


@pytest.fixture
def suffix_databricks_job_names() -> Callable[[Path, str], List[str]]:
    """Factory: ``(project_root, suffix) → list[new_name]``.

    Appends ``suffix`` to every Databricks job's display name in the
    project. Mutates ``pbt_project.yml`` (drives ``find_job``) and each
    Databricks job's ``code/databricks-job.json`` (drives the actual job
    name on the Databricks side via ``create_job``). Returns the list of
    new job names so callers can sanity-check them after deploy.
    """

    def _suffix(project_root: Path, suffix: str) -> List[str]:
        pbt_path = project_root / "pbt_project.yml"
        pbt = yaml.safe_load(pbt_path.read_text())

        new_names: list[str] = []
        for job_id, job_def in (pbt.get("jobs") or {}).items():
            scheduler = job_def.get("scheduler") or {}
            if "Databricks" not in scheduler:
                continue

            old = job_def.get("name") or job_id.split("/")[-1]
            new = f"{old}{suffix}"
            job_def["name"] = new
            new_names.append(new)

            json_path = project_root / job_id / "code" / "databricks-job.json"
            if json_path.is_file():
                data = json.loads(json_path.read_text())
                if "name" in data:
                    data["name"] = f"{data['name']}{suffix}"
                json_path.write_text(json.dumps(data, indent=2))

        pbt_path.write_text(yaml.safe_dump(pbt, sort_keys=False))
        return new_names

    return _suffix
