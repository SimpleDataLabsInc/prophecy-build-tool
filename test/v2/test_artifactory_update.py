"""Port of ``test/test_utils.py`` for the artifactory-whl rewrite helper.

Legacy version compared the entire output JSON (~280 lines) against an
expected JSON blob. That meant:

- Any unrelated change to the fixture required re-pasting 280 lines.
- A single off-by-one error gave you no signal about *what* changed.

Here we externalise the input into ``test/v2/fixtures/artifactory/input.json``
and assert directly on the fields that
``modify_databricks_json_for_private_artifactory`` is documented to mutate —
``request.tasks[*].libraries`` — while confirming that unrelated sections are
passed through unchanged.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from src.pbt.deployment.jobs.utils import modify_databricks_json_for_private_artifactory

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


_FIXTURE = Path(__file__).parent / "fixtures" / "artifactory" / "input.json"


@pytest.fixture
def input_db_json() -> dict:
    """Return a fresh copy of the input JSON on every test so mutations
    inside the helper don't leak across parametrisations."""

    return json.loads(_FIXTURE.read_text())


def _libraries_by_task(job: dict) -> dict[str, list[dict]]:
    return {t["task_key"]: t.get("libraries", []) for t in job["request"]["tasks"]}


def test_default_replaces_whl_with_pypi_package(input_db_json: dict) -> None:
    original = copy.deepcopy(input_db_json)
    out = modify_databricks_json_for_private_artifactory(input_db_json)

    libs = _libraries_by_task(out)

    # For each task, the whl entry is replaced by a ``pypi`` entry carrying the
    # package name + version derived from the whl filename.
    assert libs["customer_orders"][-1] == {"pypi": {"package": "customers_orders==1.0"}}
    assert libs["join_agg"][-1] == {"pypi": {"package": "join_agg_sort==1.0"}}

    # No whl survives in either task.
    for task_key, entries in libs.items():
        assert not any("whl" in e for e in entries), f"whl leaked in task {task_key}"

    # Non-whl libraries (maven, pypi for prophecy-libs) pass through unchanged.
    assert libs["customer_orders"][0]["maven"]["coordinates"].startswith("io.prophecy:")
    assert libs["customer_orders"][1] == {"pypi": {"package": "prophecy-libs==1.9.16"}}

    # Everything outside request.tasks is untouched.
    for key in ("fabric_id", "components", "cluster_mode", "secret_scope", "sorted_processes"):
        assert out[key] == original[key]


def test_custom_artifactory_adds_repo_simple_url(input_db_json: dict) -> None:
    artifactory = "https://prophecyio.jfrog.io/artifactory/api/pypi/pypi-local"
    out = modify_databricks_json_for_private_artifactory(input_db_json, artifactory=artifactory)

    libs = _libraries_by_task(out)

    assert libs["customer_orders"][-1] == {
        "pypi": {
            "package": "customers_orders==1.0",
            "repo": f"{artifactory}/simple",
        }
    }
    assert libs["join_agg"][-1] == {
        "pypi": {
            "package": "join_agg_sort==1.0",
            "repo": f"{artifactory}/simple",
        }
    }


def test_trailing_slash_on_artifactory_is_normalised(input_db_json: dict) -> None:
    artifactory = "https://prophecyio.jfrog.io/artifactory/api/pypi/pypi-local/"
    out = modify_databricks_json_for_private_artifactory(input_db_json, artifactory=artifactory)

    libs = _libraries_by_task(out)
    expected_repo = "https://prophecyio.jfrog.io/artifactory/api/pypi/pypi-local/simple"
    for task_key in ("customer_orders", "join_agg"):
        assert (
            libs[task_key][-1]["pypi"]["repo"] == expected_repo
        ), f"Expected trailing-slash to be stripped for {task_key}"


def test_unknown_whl_shape_is_passed_through_unchanged(input_db_json: dict) -> None:
    # Replace the whl entries with a malformed whl value so extraction returns
    # (None, None) — the helper should leave them in place.
    for task in input_db_json["request"]["tasks"]:
        task["libraries"] = [{"whl": "dbfs:/not-a-wheel-path.txt"}]

    out = modify_databricks_json_for_private_artifactory(input_db_json)

    for task in out["request"]["tasks"]:
        assert task["libraries"] == [{"whl": "dbfs:/not-a-wheel-path.txt"}]
