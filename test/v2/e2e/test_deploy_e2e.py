"""End-to-end ``pbt deploy_v2`` test against a real Databricks workspace.

This is the **one** required full-stack deploy test, parametrized over
both supported languages so the python and scala paths sit side by
side. Everything from the CLI parse → project load → real wheel/jar
build → real deploy orchestrator → ``DatabricksJobsDeployment.deploy``
runs unmodified, and the outbound HTTP layer talks to the real
Databricks workspace identified by ``DATABRICKS_HOST`` /
``DATABRICKS_TOKEN`` (or the configured ``~/.databrickscfg`` profile).

Filter plumbing, per-flag behaviour, error messages, and orchestration
permutations are owned by the much faster
:mod:`test.v2.test_deploy_v2_units` lane (which mocks the deploy seam
entirely). This e2e only exists so we can prove the integration with
Databricks actually works — i.e., that the job_id ``deploy_v2`` reports
is a real job id that Databricks itself acknowledges.

Python ↔ Scala parity
---------------------

The test body is parametrized over ``("python", "scala")``. The scala
variant carries ``pytest.mark.legacy`` (scala is deprecated) so it
shows up in both the legacy CI lane and the slow e2e lane, but never
in the v2 fast PR lane — see ``test/conftest.py`` for the marker policy.

The two variants do exactly the same thing, just against different
HelloProphecy sub-projects (one builds wheels via pip, the other jars
via maven).

Safety / opt-in
---------------

Creating real jobs against an arbitrary Databricks workspace is
expensive and side-effectful, so this test is gated behind two
independent opt-ins:

1. ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` (or a usable
   ``~/.databrickscfg`` profile). Without working creds, skip.
2. ``PBT_E2E_DATABRICKS_OK=1`` — explicit acknowledgement that the
   caller is OK with this test creating Databricks jobs in the workspace
   the creds point at. Without this var, skip even when creds are
   present, so a developer just running ``pytest`` locally with a
   ``.databrickscfg`` profile doesn't accidentally spam their workspace.

To make every run independent (no collisions if the previous run failed
to clean up), the project's job names are mutated with a per-run suffix
``-pbt-e2e-<lang>-<8-hex>``. After the run we use a fresh
``DatabricksClient`` to ``get_job`` each created id (proving Databricks
knows about it) and then ``delete_job`` to clean up. DBFS uploads are
*not* explicitly cleaned up — they live under the project's release
path and are small.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner

from src.pbt import deploy_v2
from src.pbt.client.databricks import DatabricksClient
from src.pbt.deployment.jobs.airflow import AirflowJobDeployment
from src.pbt.utils.databricks_creds import DatabricksCredentials

# ``real_databricks_creds`` and ``suffix_databricks_job_names`` are fixtures
# defined in :mod:`test.v2.e2e.conftest`; pytest auto-discovers them.

pytestmark = [pytest.mark.e2e, pytest.mark.v2, pytest.mark.maven]


# Same parametrize axis as ``test_build_e2e`` and ``test_test_e2e``.
LANGUAGES = [
    pytest.param("python", id="python"),
    pytest.param("scala", id="scala", marks=pytest.mark.legacy),
]


@pytest.mark.parametrize("language", LANGUAGES)
def test_deploy_v2_helloprophecy_creates_real_job_in_databricks(
    cli_runner: CliRunner,
    helloprophecy_repo: tuple[Path, Path, Path],
    real_databricks_creds: DatabricksCredentials,
    suffix_databricks_job_names,
    monkeypatch: pytest.MonkeyPatch,
    language: str,
) -> None:
    """Full ``pbt deploy_v2`` run: real build, real deploy, real Databricks API.

    Steps:

    1. Suffix every Databricks job name with a per-run
       ``-pbt-e2e-<lang>-<id>`` so we always exercise the
       *create-new-job* path (and never collide with leftover jobs from
       earlier runs).
    2. Wrap ``DatabricksClient.create_job`` to capture the ``job_id``
       Databricks returns to ``deploy_v2``.
    3. Run ``deploy_v2`` against the real workspace.
    4. With a *fresh* ``DatabricksClient`` (no shared state with the one
       under test), call ``get_job`` for each captured id and assert
       Databricks owns the job and reports the suffixed name back.
    5. Delete each created job in a finalizer — runs even if the
       assertions fail.

    Airflow jobs (e.g. ``HelloProphecy/prophecy/jobs/AirflowEndToEndJob``)
    are short-circuited because we have no opt-in for an Airflow target.
    """

    _, python_project, scala_project = helloprophecy_repo
    project = python_project if language == "python" else scala_project

    suffix = f"-pbt-e2e-{language[:3]}-{uuid.uuid4().hex[:8]}"
    expected_names = suffix_databricks_job_names(project, suffix)
    assert expected_names, (
        f"HelloProphecy {language} project ships at least one Databricks "
        "job — the suffix helper found none, which means the fixture "
        "changed shape."
    )

    created_job_ids: list[int] = []
    real_create = DatabricksClient.create_job

    def _capture_create_job(self, content):  # type: ignore[no-untyped-def]
        response = real_create(self, content)
        try:
            created_job_ids.append(int(response["job_id"]))
        except (KeyError, TypeError, ValueError):
            pass
        return response

    monkeypatch.setattr(DatabricksClient, "create_job", _capture_create_job)

    # We do not have an Airflow target wired up for this e2e — short-circuit
    # the whole airflow lane so the deploy doesn't try to reach a non-
    # existent Airflow instance for ``AirflowEndToEndJob``.
    monkeypatch.setattr(AirflowJobDeployment, "deploy", lambda self: [])

    # Make sure the credential resolver inside deploy_v2 sees the same
    # creds we're going to use for the verifier client.
    monkeypatch.setenv("DATABRICKS_HOST", real_databricks_creds.host)
    monkeypatch.setenv("DATABRICKS_TOKEN", real_databricks_creds.token)

    verifier = DatabricksClient.from_host_and_token(
        real_databricks_creds.host,
        real_databricks_creds.token,
        f"pbt-e2e-verifier-{language}",
    )

    try:
        result = cli_runner.invoke(deploy_v2, ["--path", str(project)])
        assert result.exit_code == 0, result.output
        assert "Uploading pipeline" in result.output, result.output
        assert "Deployment completed successfully" in result.output, result.output

        assert created_job_ids, (
            "deploy_v2 ran to completion but DatabricksClient.create_job "
            "was never called — the orchestrator never reached the create-job "
            f"branch.\n--- output ---\n{result.output}"
        )

        # Verify each created id actually exists in Databricks and carries
        # one of the per-run suffixed names. Use a fresh DatabricksClient
        # so we can't be fooled by any in-process caching.
        seen_names: list[str] = []
        for job_id in created_job_ids:
            response = verifier.get_job(str(job_id))
            assert int(response["job_id"]) == job_id, (
                f"Databricks returned a different job_id for {job_id}: {response}"
            )
            name = (response.get("settings") or {}).get("name") or response.get("name")
            assert name and name.endswith(suffix), (
                f"Job {job_id} exists but its name {name!r} does not carry "
                f"this run's suffix {suffix!r}; something else owns this id."
            )
            seen_names.append(name)

        # Sanity: every name we suffixed should have produced a job.
        for expected in expected_names:
            assert expected in seen_names, (
                f"Expected to see job named {expected!r} in Databricks but "
                f"only saw {seen_names!r} for created ids {created_job_ids!r}"
            )

    finally:
        for job_id in created_job_ids:
            try:
                verifier.delete_job(str(job_id))
            except Exception as cleanup_err:  # noqa: BLE001
                # Best-effort cleanup — print so a failed cleanup is at
                # least visible to whoever runs the test, but don't mask
                # the real assertion failure (if any).
                print(
                    f"WARNING: failed to delete leaked Databricks job "
                    f"{job_id} ({suffix}): {cleanup_err!r}"
                )
