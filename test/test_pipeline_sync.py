import unittest
import os
import shutil
import tempfile
import json
import yaml
from click.testing import CliRunner
from src.pbt import cli
from src.pbt.utils.pipeline_rename import (
    sync_pipeline,
    validate_sync,
    find_pipeline_by_id_or_name,
    find_pipeline_in_project_config,
    validate_current_name_removed,
    PipelineNotFoundError,
    PipelineAlreadyExistsError,
    FileOperationError,
    ValidationError,
    PipelineRenameError,
)


class PipelineSyncTestCase(unittest.TestCase):
    def setUp(self):
        """Set up a temporary project structure for testing."""
        self.test_dir = tempfile.mkdtemp()
        self.project_path = os.path.join(self.test_dir, "test_project")
        os.makedirs(self.project_path)

        # Create pipelines directory
        self.pipelines_dir = os.path.join(self.project_path, "pipelines")
        os.makedirs(self.pipelines_dir)

        # Create pbt_project.yml
        self.pbt_project_file = os.path.join(self.project_path, "pbt_project.yml")
        self.project_config = {
            "pipelines": {
                "pipelines/test_pipeline": {"name": "test_pipeline", "version": "1.0.0"},
                "pipelines/another_pipeline": {"name": "another_pipeline", "version": "1.0.0"},
            },
            "jobs": {},
        }
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(self.project_config, f)

        # Create test pipeline structure
        self.create_test_pipeline("test_pipeline")
        self.create_test_pipeline("another_pipeline")

    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def create_test_pipeline(self, pipeline_name):
        """Create a test pipeline structure."""
        pipeline_dir = os.path.join(self.pipelines_dir, pipeline_name)
        code_dir = os.path.join(pipeline_dir, "code")
        prophecy_dir = os.path.join(code_dir, ".prophecy")

        os.makedirs(prophecy_dir)

        # Create workflow.latest.json
        workflow = {
            "metainfo": {
                "uri": f"pipelines/{pipeline_name}",
                "topLevelPackage": f"io.prophecy.pipe.{pipeline_name}",
                "configTopLevelPackage": f"io.prophecy.config.{pipeline_name}",
                "pipelineSettingsInfo": {"applicationName": f"io.prophecy.{pipeline_name}App"},
            }
        }
        workflow_file = os.path.join(prophecy_dir, "workflow.latest.json")
        with open(workflow_file, "w") as f:
            json.dump(workflow, f, indent=2)

        # Create a simple Python file
        python_file = os.path.join(code_dir, "pipeline.py")
        with open(python_file, "w") as f:
            f.write(f'pipelineId = "pipelines/{pipeline_name}"\n')
            f.write(f'appName("{pipeline_name}")\n')

    def test_find_pipeline_by_id(self):
        """Test finding pipeline by ID in pbt_project.yml."""
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)

        result = find_pipeline_in_project_config(config, "test_pipeline")
        self.assertEqual(result, "pipelines/test_pipeline")

    def test_find_pipeline_by_name(self):
        """Test finding pipeline by name field in pbt_project.yml."""
        # Modify config to have different ID but same name
        config = {"pipelines": {"pipelines/different_id": {"name": "test_pipeline", "version": "1.0.0"}}}

        result = find_pipeline_in_project_config(config, "test_pipeline")
        self.assertEqual(result, "pipelines/different_id")

    def test_find_pipeline_not_found(self):
        """Test finding non-existent pipeline."""
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)

        result = find_pipeline_in_project_config(config, "nonexistent")
        self.assertIsNone(result)

    def test_find_pipeline_by_id_or_name_with_id(self):
        """Test finding pipeline by ID using find_pipeline_by_id_or_name."""
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)

        pid, pname, pdata = find_pipeline_by_id_or_name(config, pipeline_id="test_pipeline")
        self.assertEqual(pid, "pipelines/test_pipeline")
        self.assertEqual(pname, "test_pipeline")
        self.assertIsNotNone(pdata)

    def test_find_pipeline_by_id_or_name_with_name(self):
        """Test finding pipeline by name using find_pipeline_by_id_or_name."""
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)

        pid, pname, pdata = find_pipeline_by_id_or_name(config, pipeline_name="test_pipeline")
        self.assertEqual(pid, "pipelines/test_pipeline")
        self.assertEqual(pname, "test_pipeline")
        self.assertIsNotNone(pdata)

    def test_find_pipeline_by_id_or_name_not_found(self):
        """Test finding non-existent pipeline using find_pipeline_by_id_or_name."""
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)

        pid, pname, pdata = find_pipeline_by_id_or_name(config, pipeline_id="nonexistent")
        self.assertIsNone(pid)
        self.assertIsNone(pname)
        self.assertIsNone(pdata)

    def test_validate_sync_by_id_success(self):
        """Test successful validation by ID."""
        # First, create a mismatch: directory name differs from pipeline name
        # Update pbt_project.yml to have different name
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)
        config["pipelines"]["pipelines/test_pipeline"]["name"] = "renamed_pipeline"
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(config, f)

        # Should not raise any exception
        try:
            current_id, current_dir, target_name = validate_sync(self.project_path, pipeline_id="test_pipeline")
            self.assertEqual(current_id, "pipelines/test_pipeline")
            self.assertEqual(current_dir, "test_pipeline")
            self.assertEqual(target_name, "renamed_pipeline")
        except Exception as e:
            self.fail(f"validate_sync raised {type(e).__name__} unexpectedly: {e}")

    def test_validate_sync_by_name_success(self):
        """Test successful validation by name."""
        # First, create a mismatch
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)
        config["pipelines"]["pipelines/test_pipeline"]["name"] = "renamed_pipeline"
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(config, f)

        # Should not raise any exception
        try:
            current_id, current_dir, target_name = validate_sync(self.project_path, pipeline_name="renamed_pipeline")
            self.assertEqual(current_id, "pipelines/test_pipeline")
            self.assertEqual(current_dir, "test_pipeline")
            self.assertEqual(target_name, "renamed_pipeline")
        except Exception as e:
            self.fail(f"validate_sync raised {type(e).__name__} unexpectedly: {e}")

    def test_validate_sync_no_args(self):
        """Test validation when neither ID nor name provided."""
        with self.assertRaises(ValidationError) as context:
            validate_sync(self.project_path)
        self.assertIn("Either --pipeline-id or --pipeline-name", str(context.exception))
        self.assertIn("ACTION", str(context.exception))

    def test_validate_sync_both_args(self):
        """Test validation when both ID and name provided."""
        with self.assertRaises(ValidationError) as context:
            validate_sync(self.project_path, pipeline_id="test", pipeline_name="test")
        self.assertIn("Cannot specify both", str(context.exception))
        self.assertIn("ACTION", str(context.exception))

    def test_validate_sync_pipeline_not_found_by_id(self):
        """Test validation when pipeline ID doesn't exist."""
        with self.assertRaises(PipelineNotFoundError) as context:
            validate_sync(self.project_path, pipeline_id="nonexistent")
        self.assertIn("not found", str(context.exception))
        self.assertIn("ACTION", str(context.exception))

    def test_validate_sync_pipeline_not_found_by_name(self):
        """Test validation when pipeline name doesn't exist."""
        with self.assertRaises(PipelineNotFoundError) as context:
            validate_sync(self.project_path, pipeline_name="nonexistent")
        self.assertIn("not found", str(context.exception))
        self.assertIn("ACTION", str(context.exception))

    def test_validate_sync_already_synced(self):
        """Test validation when pipeline is already synced."""
        # In our test setup, test_pipeline directory name matches the name field
        # So it should be already synced
        with self.assertRaises(ValidationError) as context:
            validate_sync(self.project_path, pipeline_id="test_pipeline")
        self.assertIn("already synced", str(context.exception))

    def test_sync_pipeline_by_id_safe_mode(self):
        """Test safe mode sync by ID."""
        # First, create a mismatch: directory name differs from pipeline name
        # Update pbt_project.yml to have different name
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)
        config["pipelines"]["pipelines/test_pipeline"]["name"] = "renamed_pipeline"
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(config, f)

        # Now sync
        sync_pipeline(self.project_path, pipeline_id="test_pipeline", unsafe=False)

        # Check directory was renamed
        self.assertFalse(os.path.exists(os.path.join(self.pipelines_dir, "test_pipeline")))
        self.assertTrue(os.path.exists(os.path.join(self.pipelines_dir, "renamed_pipeline")))

        # Check pbt_project.yml was updated
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)

        self.assertNotIn("pipelines/test_pipeline", config["pipelines"])
        self.assertIn("pipelines/renamed_pipeline", config["pipelines"])
        self.assertEqual(config["pipelines"]["pipelines/renamed_pipeline"]["name"], "renamed_pipeline")

    def test_sync_pipeline_by_name_safe_mode(self):
        """Test safe mode sync by name."""
        # First, create a mismatch: directory name differs from pipeline name
        # Update pbt_project.yml to have different name
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)
        config["pipelines"]["pipelines/test_pipeline"]["name"] = "renamed_pipeline"
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(config, f)

        # Now sync by name
        sync_pipeline(self.project_path, pipeline_name="renamed_pipeline", unsafe=False)

        # Check directory was renamed
        self.assertFalse(os.path.exists(os.path.join(self.pipelines_dir, "test_pipeline")))
        self.assertTrue(os.path.exists(os.path.join(self.pipelines_dir, "renamed_pipeline")))

    def test_sync_pipeline_unsafe_mode(self):
        """Test unsafe mode sync (full rename)."""
        # First, create a mismatch
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)
        config["pipelines"]["pipelines/test_pipeline"]["name"] = "renamed_pipeline"
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(config, f)

        sync_pipeline(self.project_path, pipeline_id="test_pipeline", unsafe=True)

        # Check directory was renamed
        self.assertFalse(os.path.exists(os.path.join(self.pipelines_dir, "test_pipeline")))
        self.assertTrue(os.path.exists(os.path.join(self.pipelines_dir, "renamed_pipeline")))

        # Check workflow.json was updated - use string matching to verify formatting
        workflow_file = os.path.join(
            self.pipelines_dir, "renamed_pipeline", "code", ".prophecy", "workflow.latest.json"
        )
        with open(workflow_file, "r") as f:
            workflow_content = f.read()

        # Verify content using string matching to ensure formatting is correct
        self.assertIn('"uri": "pipelines/renamed_pipeline"', workflow_content)
        self.assertIn("renamed_pipeline", workflow_content)
        # Verify no old pipeline name remains
        self.assertNotIn("pipelines/test_pipeline", workflow_content)

    def test_sync_pipeline_not_found_error(self):
        """Test error when pipeline doesn't exist."""
        with self.assertRaises(PipelineNotFoundError) as context:
            sync_pipeline(self.project_path, pipeline_id="nonexistent", unsafe=False)

        self.assertIn("not found", str(context.exception))
        self.assertIn("ACTION", str(context.exception))

    def test_cli_command_success_by_id(self):
        """Test CLI command success by ID."""
        # Create mismatch first
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)
        config["pipelines"]["pipelines/test_pipeline"]["name"] = "cli_renamed"
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(config, f)

        runner = CliRunner()
        result = runner.invoke(cli, ["rename-sync", "--path", self.project_path, "--pipeline-id", "test_pipeline"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Successfully synced", result.output)

    def test_cli_command_success_by_name(self):
        """Test CLI command success by name."""
        # Create mismatch first
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)
        config["pipelines"]["pipelines/test_pipeline"]["name"] = "cli_renamed"
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(config, f)

        runner = CliRunner()
        result = runner.invoke(cli, ["rename-sync", "--path", self.project_path, "--pipeline-name", "cli_renamed"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Successfully synced", result.output)

    def test_cli_command_pipeline_not_found(self):
        """Test CLI command with non-existent pipeline."""
        runner = CliRunner()
        result = runner.invoke(cli, ["rename-sync", "--path", self.project_path, "--pipeline-id", "nonexistent"])

        self.assertEqual(result.exit_code, 1)
        self.assertIn("Pipeline Not Found", result.output)
        self.assertIn("ACTION", result.output)

    def test_cli_command_no_args(self):
        """Test CLI command with no arguments."""
        runner = CliRunner()
        result = runner.invoke(cli, ["rename-sync", "--path", self.project_path])

        self.assertEqual(result.exit_code, 1)
        self.assertIn("Validation Error", result.output)
        self.assertIn("Either --pipeline-id or --pipeline-name", result.output)

    def test_cli_command_both_args(self):
        """Test CLI command with both ID and name."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["rename-sync", "--path", self.project_path, "--pipeline-id", "test", "--pipeline-name", "test"]
        )

        self.assertEqual(result.exit_code, 1)
        self.assertIn("Validation Error", result.output)
        self.assertIn("Cannot specify both", result.output)

    def test_cli_command_unsafe_mode(self):
        """Test CLI command with unsafe mode."""
        # Create mismatch first
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)
        config["pipelines"]["pipelines/test_pipeline"]["name"] = "unsafe_renamed"
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(config, f)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "rename-sync",
                "--path",
                self.project_path,
                "--pipeline-id",
                "test_pipeline",
                "--unsafe",
            ],
        )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Successfully synced", result.output)

        # Verify unsafe mode changes were made
        workflow_file = os.path.join(self.pipelines_dir, "unsafe_renamed", "code", ".prophecy", "workflow.latest.json")
        with open(workflow_file, "r") as f:
            workflow_content = f.read()

        # Verify content using string matching to ensure formatting is correct
        self.assertIn("unsafe_renamed", workflow_content)
        self.assertIn('"topLevelPackage"', workflow_content)

    def test_pbt_project_yml_search_by_id_and_name(self):
        """Test that pbt_project.yml search works with both ID and name."""
        # Create a config where ID doesn't match name
        config = {"pipelines": {"pipelines/custom_id": {"name": "custom_name", "version": "1.0.0"}}}

        # Should find by name
        result = find_pipeline_in_project_config(config, "custom_name")
        self.assertEqual(result, "pipelines/custom_id")

        # Should find by ID
        result = find_pipeline_in_project_config(config, "custom_id")
        self.assertEqual(result, "pipelines/custom_id")

    def test_error_messages_contain_action(self):
        """Test that all error messages contain actionable guidance."""
        # Test various error scenarios
        test_cases = [
            ({"pipeline_id": "nonexistent"}, PipelineNotFoundError, "not found"),
            ({"pipeline_name": "nonexistent"}, PipelineNotFoundError, "not found"),
        ]

        for kwargs, expected_exception, expected_error in test_cases:
            with self.assertRaises(expected_exception) as context:
                validate_sync(self.project_path, **kwargs)
            self.assertIn("ACTION", str(context.exception), f"Error message should contain ACTION: {context.exception}")
            self.assertIn(expected_error, str(context.exception))

    def test_error_suggests_pipeline_id_when_name_matches_id(self):
        """Test that error suggests --pipeline-id when provided name matches an ID."""
        # Create a pipeline where ID and name are different
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)

        # Add a pipeline with ID "pipelines/customId" but name "customName"
        config["pipelines"]["pipelines/customId"] = {"name": "customName", "version": "1.0.0"}
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(config, f)

        # Create the directory
        custom_dir = os.path.join(self.pipelines_dir, "customId")
        os.makedirs(custom_dir, exist_ok=True)
        self.create_test_pipeline("customId")

        # Now try to use --pipeline-name with "customId" (which is an ID, not a name)
        # This should fail because "customId" is not a name, but it exists as an ID
        with self.assertRaises(PipelineNotFoundError) as context:
            validate_sync(self.project_path, pipeline_name="customId")

        error_msg = str(context.exception)
        self.assertIn("Use --pipeline-id", error_msg, "Should suggest using --pipeline-id")
        self.assertIn("customId", error_msg, "Should mention the correct ID")

    def test_error_suggests_pipeline_name_when_id_matches_name(self):
        """Test that error suggests --pipeline-name when provided ID matches a name."""
        # Create a pipeline where ID and name are different
        with open(self.pbt_project_file, "r") as f:
            config = yaml.safe_load(f)

        # Add a pipeline with ID "pipelines/customId" but name "customName"
        config["pipelines"]["pipelines/customId"] = {"name": "customName", "version": "1.0.0"}
        with open(self.pbt_project_file, "w") as f:
            yaml.dump(config, f)

        # Create the directory
        custom_dir = os.path.join(self.pipelines_dir, "customId")
        os.makedirs(custom_dir, exist_ok=True)
        self.create_test_pipeline("customId")

        # Now try to use --pipeline-id with "customName" (which is a name, not an ID)
        # This should fail because "pipelines/customName" is not an ID, but "customName" exists as a name
        with self.assertRaises(PipelineNotFoundError) as context:
            validate_sync(self.project_path, pipeline_id="customName")

        error_msg = str(context.exception)
        self.assertIn("Use --pipeline-name", error_msg, "Should suggest using --pipeline-name")
        self.assertIn("customName", error_msg, "Should mention the correct name")

    def test_job_json_comprehensive_update(self):
        """Test that job JSON files are comprehensively updated with all occurrences."""
        from src.pbt.utils.pipeline_rename import update_json_file

        # Create a test JSON file with various patterns
        jobs_dir = os.path.join(self.project_path, "jobs", "test_job", "code")
        os.makedirs(jobs_dir, exist_ok=True)

        test_json = {
            "processes": {
                "process1": {
                    "properties": {"pipelineId": "pipelines/test_pipeline"},
                }
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
            "other_field": "test_pipeline_something",  # This won't be updated (substring, not exact match)
        }

        json_file = os.path.join(jobs_dir, "prophecy-job.json")
        with open(json_file, "w") as f:
            json.dump(test_json, f, indent=2)

        # Update the JSON file
        update_json_file(
            json_file, "pipelines/test_pipeline", "pipelines/renamed_pipeline", "test_pipeline", "renamed_pipeline"
        )

        # Verify all occurrences were updated - use string matching to verify formatting
        with open(json_file, "r") as f:
            json_content = f.read()

        # Verify content using string matching to ensure formatting is correct (no extra spaces)
        self.assertIn('"pipelineId": "pipelines/renamed_pipeline"', json_content)
        self.assertIn('"nodeName": "renamed_pipeline"', json_content)
        self.assertIn('"task_key": "renamed_pipeline"', json_content)
        self.assertIn("renamed_pipeline", json_content)
        # Verify pipelineId was updated
        self.assertNotIn('"pipelineId": "pipelines/test_pipeline"', json_content)
        # Verify other_field was NOT updated (substring, not exact match)
        self.assertIn('"other_field": "test_pipeline_something"', json_content)

    def test_job_python_comprehensive_update(self):
        """Test that job Python files are comprehensively updated with all occurrences."""
        from src.pbt.utils.pipeline_rename import update_job_python_file

        # Create a test Python file with various patterns
        jobs_dir = os.path.join(self.project_path, "jobs", "test_job", "code")
        os.makedirs(jobs_dir, exist_ok=True)

        test_python_content = """# This is a comment about test_pipeline
pipeline_dict = {
    "pipelines/test_pipeline": "some_value",
    'pipelines/test_pipeline': 'another_value'
}
pipeline_id = "pipelines/test_pipeline"
pipeline_name = "test_pipeline"
some_path = "dbfs:/path/test_pipeline.whl"
# test_pipeline is mentioned here
variable_test_pipeline = "something"
"""

        python_file = os.path.join(jobs_dir, "test.py")
        with open(python_file, "w") as f:
            f.write(test_python_content)

        # Update the Python file
        update_job_python_file(
            python_file, "pipelines/test_pipeline", "pipelines/renamed_pipeline", "test_pipeline", "renamed_pipeline"
        )

        # Verify all occurrences were updated
        with open(python_file, "r") as f:
            updated_content = f.read()

        # Check that pipeline references were updated
        self.assertIn("renamed_pipeline", updated_content)
        self.assertIn('"pipelines/renamed_pipeline"', updated_content)
        self.assertIn('pipeline_id = "pipelines/renamed_pipeline"', updated_content)
        self.assertIn('pipeline_name = "renamed_pipeline"', updated_content)
        self.assertIn("dbfs:/path/renamed_pipeline.whl", updated_content)

        # Check that pipeline ID and name strings were updated
        self.assertNotIn('"pipelines/test_pipeline"', updated_content)
        self.assertNotIn('pipeline_id = "pipelines/test_pipeline"', updated_content)
        self.assertNotIn('pipeline_name = "test_pipeline"', updated_content)

        # Note: variable names like variable_test_pipeline might still contain test_pipeline
        # as part of the variable name, which is acceptable


if __name__ == "__main__":
    unittest.main()
