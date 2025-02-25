import os
import pytest

from src.pbt import deploy, deploy_v2
from test.isolated_repo_test_case import IsolatedRepoTestCase
from test import get_command_name

if os.environ.get("DATABRICKS_HOST") is None:
    os.environ["DATABRICKS_HOST"] = "test"
if os.environ.get("DATABRICKS_TOKEN") is None:
    os.environ["DATABRICKS_TOKEN"] = "test"


class TestDeploy(IsolatedRepoTestCase):
    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy], ids=get_command_name)  # TODO deploy_v2 does not work here.
    def test_deploy_path_default_databricks_jobs(self, cli_runner, language, command):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        # runner = CliRunner()
        result = cli_runner.invoke(command, ["--path", project_path])
        print(result.output)

        if command is deploy:
            assert "Found 2 jobs:" in result.output
            assert "Found 5 pipelines" in result.output
            assert "Deploying 2 jobs" in result.output
        elif command is deploy_v2:
            assert "Deploying databricks jobs" in result.output

        # If running with Databricks creds on GitHub Actions
        if os.environ["DATABRICKS_HOST"] != "test":
            assert result.exit_code == 0
            if command is deploy:
                assert "dbfs:/FileStore/prophecy/artifacts/" in result.output
                assert "[DONE]: Deployment completed successfully!" in result.output
            elif command is deploy_v2:
                assert "Deployment completed successfully." in result.output
        else:
            assert result.exit_code == 1

    @pytest.mark.skip(reason="no way to currently test. no stdout output from airflow deploy")
    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy_v2], ids=get_command_name)
    def test_deploy_path_default_airflow_jobs(self, cli_runner, language, command):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        # runner = CliRunner()
        result = cli_runner.invoke(command, ["--path", project_path, "--job-ids", "AirflowEndToEndJob"])
        print(result.output)
        # assert result.exit_code == 0

        # TODO need some proper stdout output here or else no feedback
        # If running with Databricks creds on GitHub Actions
        assert False

        # If running with Databricks creds on GitHub Actions
        if os.environ["AIRFLOWHOST"] != "test":  # TODO make sure airflow creds are set too for airflow live test
            assert result.exit_code == 0
            if command is deploy:
                assert "[DONE]: Deployment completed successfully!" in result.output
            elif command is deploy_v2:
                assert "Deployment completed successfully." in result.output
        else:
            assert result.exit_code == 1

    # TODO deploy_v2 does not work here because airflow builds first and fails to upload
    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy], ids=get_command_name)
    def test_deploy_path_default_skip_builds(self, cli_runner, language, command):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        # runner = CliRunner()
        fabric_id = "16433"
        result = cli_runner.invoke(command, ["--path", project_path, "--skip-builds", "--fabric-ids", fabric_id])
        print(result.output)
        if command is deploy:
            assert "[SKIP]: Skipping builds for all pipelines as '--skip-builds' flag is passed." in result.output
        elif command is deploy_v2:
            assert "skip_builds is set to true" in result.output

        if os.environ["DATABRICKS_HOST"] != "test":
            # TODO should make this test run the build command first to check that it actually works
            # assert result.exit_code == 0
            pass
        else:
            assert result.exit_code == 1

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy, deploy_v2], ids=get_command_name)
    def test_deploy_path_fabric_id_filter(self, cli_runner, language, command):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        fabric_id = "16432"
        # runner = CliRunner()
        result = runner.invoke(command, ["--path", project_path, "--fabric-ids", fabric_id, "--skip-builds"])
        print(result.output)

        if command is deploy:
            assert "Found 2 jobs" in result.output
            assert f"Deploying jobs only for given Fabric IDs: ['{fabric_id}']" in result.output
        elif command is deploy_v2:
            assert "Found 3 jobs" in result.output
            assert "[SKIP] Job jobs/EndToEndJob skipped as it belongs to fabric-id" in result.output
            assert f"but allowed fabric-ids are ['{fabric_id}']" in result.output
            assert "[SKIP] Job jobs/DatabricksJob2 skipped as it belongs to fabric-id" in result.output
            assert "Deployment completed successfully." in result.output

        # TODO even though there is an airflow job in v2 it doesn't build and isn't deployed but we get exit code 0 and
        #  success message

    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_deploy_v1_path_pipeline_invalid_fabric_id(self, cli_runner, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        # runner = CliRunner()
        result = cli_runner.invoke(deploy, ["--path", project_path, "--fabric-ids", "999999"])
        print(result.output)
        assert "Found 2 jobs:" in result.output
        assert "Deploying jobs only for given Fabric IDs: ['999999']" in result.output
        assert "[SKIP]: Job " in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_deploy_v1_with_fabric_id_and_job_id_filter(self, cli_runner, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        # runner = CliRunner()
        result = cli_runner.invoke(deploy, ["--path", project_path, "--fabric-ids", "999", "--job-ids", "test-job"])
        print(result.output)
        assert result.exit_code == 1
        assert "[ERROR]: Can't combine filters, Please pass either --fabric_ids or --job_id" in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_deploy_v1_with_job_id_filter_and_skip_builds(self, cli_runner, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        # runner = CliRunner()
        result = cli_runner.invoke(deploy, ["--path", project_path, "--job-ids", "test-job", "--skip-builds"])
        print(result.output)
        assert result.exit_code == 1
        assert (
            "[ERROR]: Can't skip builds for job_id filter,\nas it only builds depending pipelines ,\nPlease pass "
            "either --skip-builds or --job_id filter" in result.output
        )

    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_deploy_v1_path_pipeline_with_job_id_filter(self, cli_runner, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        # runner = CliRunner()
        result = cli_runner.invoke(deploy, ["--path", project_path, "--job-ids", "EndToEndJob"])
        print(result.output)

        # tODO there is a bad error message, you can get it by passing "--job-ids jobs/EndToEndJob ":
        #       Deploying jobs only for given Job IDs: ['jobs/EndToEndJob']
        #       [ERROR]: No Job IDs matches with passed --job_id filter ['jobs/EndToEndJob']
        #       Available Job IDs are: dict_keys(['jobs/EndToEndJob'])
        #  the solution is to not prefix with "job/"

        assert "Found 2 jobs:" in result.output
        assert "Found 5 pipelines:" in result.output
        assert "Deploying jobs only for given Job IDs: ['EndToEndJob']" in result.output

        assert "[INFO]: Total Unique pipelines dependencies found: 5" in result.output
        assert "[INFO]: Building given custom pipelines" in result.output
        assert "[INFO]: Generating depending pipelines for all jobs" in result.output
        assert "Building 5 pipelines" in result.output
        assert "Deploying 1 jobs" in result.output
        assert "[START]:  Deploying job " in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_deploy_v1_path_pipeline_with_multiple_job_id_filter(self, cli_runner, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        # runner = CliRunner()
        result = cli_runner.invoke(deploy, ["--path", project_path, "--job-ids", "EndToEndJob,DatabricksJob2"])
        print(result.output)

        assert "Found 2 jobs: EndToEndJob, DatabricksJob2" in result.output
        assert "Deploying jobs only for given Job IDs: ['EndToEndJob', 'DatabricksJob2']" in result.output
        assert "[INFO]: Total Unique pipelines dependencies found: 5" in result.output
        assert "[INFO]: Building given custom pipelines" in result.output
        assert "Deploying 2 jobs" in result.output
        assert "[START]:  Deploying job jobs/EndToEndJob" in result.output
        assert "[START]:  Deploying job jobs/DatabricksJob2" in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_deploy_v1_path_pipeline_with_one_invalid_job_id_filter(self, cli_runner, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        # runner = CliRunner()
        result = cli_runner.invoke(deploy, ["--path", project_path, "--job-ids", "invalid1,EndToEndJob"])
        print(result.output)

        assert "Found 2 jobs: " in result.output
        assert "Deploying jobs only for given Job IDs: ['invalid1', 'EndToEndJob']" in result.output
        assert "[INFO]: Total Unique pipelines dependencies found: 5" in result.output
        assert "[INFO]: Building given custom pipelines" in result.output
        assert "Building 5 pipelines" in result.output
        assert "Deploying 1 jobs" in result.output
        assert "[START]:  Deploying job jobs/EndToEndJob" in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_deploy_v1_path_pipeline_with_all_invalid_job_ids_filter(self, cli_runner, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        # runner = CliRunner()
        result = cli_runner.invoke(deploy, ["--path", project_path, "--job-ids", "invalid1,invalid2"])
        print(result.output)

        assert result.exit_code == 1
        assert "Found 2 jobs: " in result.output
        assert "Deploying jobs only for given Job IDs: ['invalid1', 'invalid2']" in result.output
        assert (
            "[ERROR]: No Job IDs matches with passed --job_id filter ['invalid1', 'invalid2']\nAvailable Job IDs are: "
            in result.output
        )
