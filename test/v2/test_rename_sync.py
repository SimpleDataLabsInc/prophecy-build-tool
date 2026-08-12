"""Port of ``test/test_pipeline_sync.py`` to native pytest.

The legacy file used ``unittest.TestCase`` with ``setUp`` / ``tearDown`` that
built a tmp project via ``tempfile.mkdtemp``. We swap that for a pytest
fixture that builds the same shape in ``tmp_path``. Every assertion and
scenario from the legacy file is preserved — these tests are already pure
Python / filesystem, so no mocking is required and they run sub-second in
aggregate.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import pytest
import yaml
from click.testing import CliRunner

from src.pbt import cli
from src.pbt.utils.pipeline_rename import (
    PipelineNotFoundError,
    ValidationError,
    find_pipeline_by_id_or_name,
    find_pipeline_in_project_config,
    sync_pipeline,
    update_job_python_file,
    update_json_file,
    validate_sync,
)

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


# ---------------------------------------------------------------------------
# Fixture: synthetic rename-sync project (was setUp in the legacy TestCase).
# ---------------------------------------------------------------------------


def _write_pipeline_files(pipelines_dir: Path, pipeline_name: str) -> None:
    """Write the ``workflow.latest.json`` + ``pipeline.py`` structure."""

    code_dir = pipelines_dir / pipeline_name / "code"
    prophecy_dir = code_dir / ".prophecy"
    prophecy_dir.mkdir(parents=True, exist_ok=True)

    workflow = {
        "metainfo": {
            "uri": f"pipelines/{pipeline_name}",
            "topLevelPackage": f"io.prophecy.pipe.{pipeline_name}",
            "configTopLevelPackage": f"io.prophecy.config.{pipeline_name}",
            "pipelineSettingsInfo": {"applicationName": f"io.prophecy.{pipeline_name}App"},
        }
    }
    (prophecy_dir / "workflow.latest.json").write_text(json.dumps(workflow, indent=2))

    (code_dir / "pipeline.py").write_text(f'pipelineId = "pipelines/{pipeline_name}"\n' f'appName("{pipeline_name}")\n')


@pytest.fixture
def rename_sync_project(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Return ``(project_path, pipelines_dir, pbt_project_file)``."""

    project_path = tmp_path / "test_project"
    pipelines_dir = project_path / "pipelines"
    pipelines_dir.mkdir(parents=True)

    pbt_project_file = project_path / "pbt_project.yml"
    project_config = {
        "pipelines": {
            "pipelines/test_pipeline": {"name": "test_pipeline", "version": "1.0.0"},
            "pipelines/another_pipeline": {"name": "another_pipeline", "version": "1.0.0"},
        },
        "jobs": {},
    }
    pbt_project_file.write_text(yaml.dump(project_config))

    _write_pipeline_files(pipelines_dir, "test_pipeline")
    _write_pipeline_files(pipelines_dir, "another_pipeline")

    return project_path, pipelines_dir, pbt_project_file


@pytest.fixture
def rename_sync_helpers(rename_sync_project) -> Callable:
    """Helpers that need the fixture data."""

    project_path, pipelines_dir, pbt_project_file = rename_sync_project

    def _set_name(pipeline_id: str, new_name: str) -> None:
        config = yaml.safe_load(pbt_project_file.read_text())
        config["pipelines"][pipeline_id]["name"] = new_name
        pbt_project_file.write_text(yaml.dump(config))

    def _add_pipeline(pipeline_id: str, name: str, dir_name: str) -> None:
        config = yaml.safe_load(pbt_project_file.read_text())
        config["pipelines"][pipeline_id] = {"name": name, "version": "1.0.0"}
        pbt_project_file.write_text(yaml.dump(config))
        (pipelines_dir / dir_name).mkdir(exist_ok=True)
        _write_pipeline_files(pipelines_dir, dir_name)

    def _read_config() -> dict:
        return yaml.safe_load(pbt_project_file.read_text())

    return {"set_name": _set_name, "add_pipeline": _add_pipeline, "read_config": _read_config}


# ---------------------------------------------------------------------------
# find_pipeline_in_project_config / find_pipeline_by_id_or_name
# ---------------------------------------------------------------------------


def test_find_pipeline_by_id(rename_sync_project, rename_sync_helpers) -> None:
    config = rename_sync_helpers["read_config"]()
    assert find_pipeline_in_project_config(config, "test_pipeline") == "pipelines/test_pipeline"


def test_find_pipeline_by_name() -> None:
    config = {"pipelines": {"pipelines/different_id": {"name": "test_pipeline", "version": "1.0.0"}}}
    assert find_pipeline_in_project_config(config, "test_pipeline") == "pipelines/different_id"


def test_find_pipeline_not_found(rename_sync_helpers) -> None:
    assert find_pipeline_in_project_config(rename_sync_helpers["read_config"](), "nonexistent") is None


def test_find_pipeline_by_id_or_name_with_id(rename_sync_helpers) -> None:
    pid, pname, pdata = find_pipeline_by_id_or_name(rename_sync_helpers["read_config"](), pipeline_id="test_pipeline")
    assert pid == "pipelines/test_pipeline"
    assert pname == "test_pipeline"
    assert pdata is not None


def test_find_pipeline_by_id_or_name_with_name(rename_sync_helpers) -> None:
    pid, pname, pdata = find_pipeline_by_id_or_name(rename_sync_helpers["read_config"](), pipeline_name="test_pipeline")
    assert pid == "pipelines/test_pipeline"
    assert pname == "test_pipeline"
    assert pdata is not None


def test_find_pipeline_by_id_or_name_not_found(rename_sync_helpers) -> None:
    pid, pname, pdata = find_pipeline_by_id_or_name(rename_sync_helpers["read_config"](), pipeline_id="nonexistent")
    assert pid is None and pname is None and pdata is None


# ---------------------------------------------------------------------------
# validate_sync
# ---------------------------------------------------------------------------


def test_validate_sync_by_id_success(rename_sync_project, rename_sync_helpers) -> None:
    project_path, _, _ = rename_sync_project
    rename_sync_helpers["set_name"]("pipelines/test_pipeline", "renamed_pipeline")

    current_id, current_dir, target_name = validate_sync(str(project_path), pipeline_id="test_pipeline")
    assert current_id == "pipelines/test_pipeline"
    assert current_dir == "test_pipeline"
    assert target_name == "renamed_pipeline"


def test_validate_sync_by_name_success(rename_sync_project, rename_sync_helpers) -> None:
    project_path, _, _ = rename_sync_project
    rename_sync_helpers["set_name"]("pipelines/test_pipeline", "renamed_pipeline")

    current_id, current_dir, target_name = validate_sync(str(project_path), pipeline_name="renamed_pipeline")
    assert current_id == "pipelines/test_pipeline"
    assert current_dir == "test_pipeline"
    assert target_name == "renamed_pipeline"


def test_validate_sync_no_args(rename_sync_project) -> None:
    project_path, _, _ = rename_sync_project
    with pytest.raises(ValidationError) as excinfo:
        validate_sync(str(project_path))
    assert "Either --pipeline-id or --pipeline-name" in str(excinfo.value)
    assert "ACTION" in str(excinfo.value)


def test_validate_sync_both_args(rename_sync_project) -> None:
    project_path, _, _ = rename_sync_project
    with pytest.raises(ValidationError) as excinfo:
        validate_sync(str(project_path), pipeline_id="test", pipeline_name="test")
    assert "Cannot specify both" in str(excinfo.value)
    assert "ACTION" in str(excinfo.value)


def test_validate_sync_pipeline_not_found_by_id(rename_sync_project) -> None:
    project_path, _, _ = rename_sync_project
    with pytest.raises(PipelineNotFoundError) as excinfo:
        validate_sync(str(project_path), pipeline_id="nonexistent")
    assert "not found" in str(excinfo.value)
    assert "ACTION" in str(excinfo.value)


def test_validate_sync_pipeline_not_found_by_name(rename_sync_project) -> None:
    project_path, _, _ = rename_sync_project
    with pytest.raises(PipelineNotFoundError) as excinfo:
        validate_sync(str(project_path), pipeline_name="nonexistent")
    assert "not found" in str(excinfo.value)
    assert "ACTION" in str(excinfo.value)


def test_validate_sync_already_synced(rename_sync_project) -> None:
    project_path, _, _ = rename_sync_project
    # ``test_pipeline`` directory already matches the name field → no-op.
    with pytest.raises(ValidationError) as excinfo:
        validate_sync(str(project_path), pipeline_id="test_pipeline")
    assert "already synced" in str(excinfo.value)


# ---------------------------------------------------------------------------
# sync_pipeline (safe and unsafe)
# ---------------------------------------------------------------------------


def test_sync_pipeline_by_id_safe_mode(rename_sync_project, rename_sync_helpers) -> None:
    project_path, pipelines_dir, pbt_project_file = rename_sync_project
    rename_sync_helpers["set_name"]("pipelines/test_pipeline", "renamed_pipeline")

    sync_pipeline(str(project_path), pipeline_id="test_pipeline", unsafe=False)

    assert not (pipelines_dir / "test_pipeline").exists()
    assert (pipelines_dir / "renamed_pipeline").exists()

    config = yaml.safe_load(pbt_project_file.read_text())
    assert "pipelines/test_pipeline" not in config["pipelines"]
    assert "pipelines/renamed_pipeline" in config["pipelines"]
    assert config["pipelines"]["pipelines/renamed_pipeline"]["name"] == "renamed_pipeline"


def test_sync_pipeline_by_name_safe_mode(rename_sync_project, rename_sync_helpers) -> None:
    project_path, pipelines_dir, _ = rename_sync_project
    rename_sync_helpers["set_name"]("pipelines/test_pipeline", "renamed_pipeline")

    sync_pipeline(str(project_path), pipeline_name="renamed_pipeline", unsafe=False)

    assert not (pipelines_dir / "test_pipeline").exists()
    assert (pipelines_dir / "renamed_pipeline").exists()


def test_sync_pipeline_unsafe_mode(rename_sync_project, rename_sync_helpers) -> None:
    project_path, pipelines_dir, _ = rename_sync_project
    rename_sync_helpers["set_name"]("pipelines/test_pipeline", "renamed_pipeline")

    sync_pipeline(str(project_path), pipeline_id="test_pipeline", unsafe=True)

    assert not (pipelines_dir / "test_pipeline").exists()
    assert (pipelines_dir / "renamed_pipeline").exists()

    workflow = (pipelines_dir / "renamed_pipeline" / "code" / ".prophecy" / "workflow.latest.json").read_text()
    assert '"uri": "pipelines/renamed_pipeline"' in workflow
    assert "renamed_pipeline" in workflow
    assert "pipelines/test_pipeline" not in workflow


def test_sync_pipeline_not_found_error(rename_sync_project) -> None:
    project_path, _, _ = rename_sync_project
    with pytest.raises(PipelineNotFoundError) as excinfo:
        sync_pipeline(str(project_path), pipeline_id="nonexistent", unsafe=False)
    assert "not found" in str(excinfo.value)
    assert "ACTION" in str(excinfo.value)


# ---------------------------------------------------------------------------
# CLI smoke tests
# ---------------------------------------------------------------------------


def test_cli_command_success_by_id(cli_runner: CliRunner, rename_sync_project, rename_sync_helpers) -> None:
    project_path, _, _ = rename_sync_project
    rename_sync_helpers["set_name"]("pipelines/test_pipeline", "cli_renamed")

    result = cli_runner.invoke(cli, ["rename-sync", "--path", str(project_path), "--pipeline-id", "test_pipeline"])
    assert result.exit_code == 0
    assert "Successfully synced" in result.output


def test_cli_command_success_by_name(cli_runner: CliRunner, rename_sync_project, rename_sync_helpers) -> None:
    project_path, _, _ = rename_sync_project
    rename_sync_helpers["set_name"]("pipelines/test_pipeline", "cli_renamed")

    result = cli_runner.invoke(cli, ["rename-sync", "--path", str(project_path), "--pipeline-name", "cli_renamed"])
    assert result.exit_code == 0
    assert "Successfully synced" in result.output


def test_cli_command_pipeline_not_found(cli_runner: CliRunner, rename_sync_project) -> None:
    project_path, _, _ = rename_sync_project
    result = cli_runner.invoke(cli, ["rename-sync", "--path", str(project_path), "--pipeline-id", "nonexistent"])
    assert result.exit_code == 1
    assert "Pipeline Not Found" in result.output
    assert "ACTION" in result.output


def test_cli_command_no_args(cli_runner: CliRunner, rename_sync_project) -> None:
    project_path, _, _ = rename_sync_project
    result = cli_runner.invoke(cli, ["rename-sync", "--path", str(project_path)])
    assert result.exit_code == 1
    assert "Validation Error" in result.output
    assert "Either --pipeline-id or --pipeline-name" in result.output


def test_cli_command_both_args(cli_runner: CliRunner, rename_sync_project) -> None:
    project_path, _, _ = rename_sync_project
    result = cli_runner.invoke(
        cli,
        [
            "rename-sync",
            "--path",
            str(project_path),
            "--pipeline-id",
            "test",
            "--pipeline-name",
            "test",
        ],
    )
    assert result.exit_code == 1
    assert "Validation Error" in result.output
    assert "Cannot specify both" in result.output


def test_cli_command_unsafe_mode(cli_runner: CliRunner, rename_sync_project, rename_sync_helpers) -> None:
    project_path, pipelines_dir, _ = rename_sync_project
    rename_sync_helpers["set_name"]("pipelines/test_pipeline", "unsafe_renamed")

    result = cli_runner.invoke(
        cli,
        [
            "rename-sync",
            "--path",
            str(project_path),
            "--pipeline-id",
            "test_pipeline",
            "--unsafe",
        ],
    )
    assert result.exit_code == 0
    assert "Successfully synced" in result.output

    workflow = (pipelines_dir / "unsafe_renamed" / "code" / ".prophecy" / "workflow.latest.json").read_text()
    assert "unsafe_renamed" in workflow
    assert '"topLevelPackage"' in workflow


# ---------------------------------------------------------------------------
# Error messages and disambiguation
# ---------------------------------------------------------------------------


def test_pbt_project_yml_search_by_id_and_name() -> None:
    config = {"pipelines": {"pipelines/custom_id": {"name": "custom_name", "version": "1.0.0"}}}
    assert find_pipeline_in_project_config(config, "custom_name") == "pipelines/custom_id"
    assert find_pipeline_in_project_config(config, "custom_id") == "pipelines/custom_id"


@pytest.mark.parametrize(
    "kwargs,expected_exception,expected_msg",
    [
        ({"pipeline_id": "nonexistent"}, PipelineNotFoundError, "not found"),
        ({"pipeline_name": "nonexistent"}, PipelineNotFoundError, "not found"),
    ],
)
def test_error_messages_contain_action(rename_sync_project, kwargs, expected_exception, expected_msg) -> None:
    project_path, _, _ = rename_sync_project
    with pytest.raises(expected_exception) as excinfo:
        validate_sync(str(project_path), **kwargs)
    assert "ACTION" in str(excinfo.value)
    assert expected_msg in str(excinfo.value)


def test_error_suggests_pipeline_id_when_name_matches_id(rename_sync_project, rename_sync_helpers) -> None:
    project_path, _, _ = rename_sync_project
    rename_sync_helpers["add_pipeline"]("pipelines/customId", "customName", "customId")

    with pytest.raises(PipelineNotFoundError) as excinfo:
        validate_sync(str(project_path), pipeline_name="customId")
    assert "Use --pipeline-id" in str(excinfo.value)
    assert "customId" in str(excinfo.value)


def test_error_suggests_pipeline_name_when_id_matches_name(rename_sync_project, rename_sync_helpers) -> None:
    project_path, _, _ = rename_sync_project
    rename_sync_helpers["add_pipeline"]("pipelines/customId", "customName", "customId")

    with pytest.raises(PipelineNotFoundError) as excinfo:
        validate_sync(str(project_path), pipeline_id="customName")
    assert "Use --pipeline-name" in str(excinfo.value)
    assert "customName" in str(excinfo.value)


# ---------------------------------------------------------------------------
# File-content update helpers
# ---------------------------------------------------------------------------


def test_job_json_comprehensive_update(tmp_path: Path) -> None:
    jobs_dir = tmp_path / "jobs" / "test_job" / "code"
    jobs_dir.mkdir(parents=True)

    test_json = {
        "processes": {
            "process1": {"properties": {"pipelineId": "pipelines/test_pipeline"}},
        },
        "components": [
            {
                "PipelineComponent": {
                    "pipelineId": "pipelines/test_pipeline",
                    "nodeName": "test_pipeline",
                    "path": "dbfs:/path/test_pipeline.whl",
                }
            }
        ],
        "request": {
            "CreateNewJobRequest": {
                "tasks": [
                    {
                        "task_key": "test_pipeline",
                        "libraries": [{"whl": "dbfs:/path/test_pipeline.whl"}],
                    }
                ]
            }
        },
        "other_field": "test_pipeline_something",
    }

    json_file = jobs_dir / "prophecy-job.json"
    json_file.write_text(json.dumps(test_json, indent=2))

    update_json_file(
        str(json_file),
        "pipelines/test_pipeline",
        "pipelines/renamed_pipeline",
        "test_pipeline",
        "renamed_pipeline",
    )

    json_content = json_file.read_text()
    assert '"pipelineId": "pipelines/renamed_pipeline"' in json_content
    assert '"nodeName": "renamed_pipeline"' in json_content
    assert '"task_key": "renamed_pipeline"' in json_content
    assert "renamed_pipeline" in json_content
    assert '"pipelineId": "pipelines/test_pipeline"' not in json_content
    # Substrings (non-exact matches) are not rewritten.
    assert '"other_field": "test_pipeline_something"' in json_content


def test_job_python_comprehensive_update(tmp_path: Path) -> None:
    jobs_dir = tmp_path / "jobs" / "test_job" / "code"
    jobs_dir.mkdir(parents=True)

    original = (
        "# This is a comment about test_pipeline\n"
        "pipeline_dict = {\n"
        '    "pipelines/test_pipeline": "some_value",\n'
        "    'pipelines/test_pipeline': 'another_value'\n"
        "}\n"
        'pipeline_id = "pipelines/test_pipeline"\n'
        'pipeline_name = "test_pipeline"\n'
        'some_path = "dbfs:/path/test_pipeline.whl"\n'
        "# test_pipeline is mentioned here\n"
        'variable_test_pipeline = "something"\n'
    )

    python_file = jobs_dir / "test.py"
    python_file.write_text(original)

    update_job_python_file(
        str(python_file),
        "pipelines/test_pipeline",
        "pipelines/renamed_pipeline",
        "test_pipeline",
        "renamed_pipeline",
    )

    updated = python_file.read_text()
    assert "renamed_pipeline" in updated
    assert '"pipelines/renamed_pipeline"' in updated
    assert 'pipeline_id = "pipelines/renamed_pipeline"' in updated
    assert 'pipeline_name = "renamed_pipeline"' in updated
    assert "dbfs:/path/renamed_pipeline.whl" in updated

    assert '"pipelines/test_pipeline"' not in updated
    assert 'pipeline_id = "pipelines/test_pipeline"' not in updated
    assert 'pipeline_name = "test_pipeline"' not in updated
