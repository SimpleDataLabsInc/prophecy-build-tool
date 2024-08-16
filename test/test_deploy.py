from click.testing import CliRunner
from src.pbt import deploy, deploy_v2
import os
import shutil
import uuid
from git import Repo
import pytest

SAMPLE_REPO = "https://github.com/prophecy-samples/HelloProphecy.git"

if os.environ.get("DATABRICKS_HOST") is None:
    os.environ["DATABRICKS_HOST"] = "test"
if os.environ.get("DATABRICKS_TOKEN") is None:
    os.environ["DATABRICKS_TOKEN"] = "test"


class TestDeploy:

    @staticmethod
    def _get_tmp_sample_repo(repo_url=SAMPLE_REPO):
        new_path = os.path.join("/tmp/", SAMPLE_REPO.split("/")[-1], f"{uuid.uuid4()}")
        repo = Repo.clone_from(repo_url, new_path)
        repo.git.fetch(tags=True)
        repo.git.checkout("pbt-reference-do-not-delete")
        return repo, new_path

    def setup_method(self):
        self.repo, self.repo_path = TestDeploy._get_tmp_sample_repo()
        self.python_project_path = os.path.join(self.repo_path, "prophecy")
        self.scala_project_path = os.path.join(self.repo_path, "prophecy_scala")

    def teardown_method(self):
        if self.repo_path:
            shutil.rmtree(self.repo_path, ignore_errors=True)


    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy, deploy_v2])  # TODO deploy_v2 does not work here.
    def test_deploy_path_default_databricks_jobs(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(command, ["--path", project_path, "--job-ids", "EndToEndJob"])

        # tODO this is a horrible error message, you can get it by passing "--job-ids jobs/EndToEndJob ":
        #       Deploying jobs only for given Job IDs: ['jobs/EndToEndJob']
        #       [ERROR]: No Job IDs matches with passed --job_id filter ['jobs/EndToEndJob']
        #       Available Job IDs are: dict_keys(['jobs/EndToEndJob'])

        if command is deploy:
            assert "Found 1 jobs:" in result.output
            assert "Deploying 1 jobs" in result.output
            assert "Found 5 pipelines" in result.output
        elif command is deploy_v2:
            assert "Deploying databricks jobs" in result.output

        # If running with Databricks creds on GitHub Actions
        if os.environ["DATABRICKS_HOST"] != "test":
            assert result.exit_code == 0
            if command is deploy:
                assert "dbfs:/FileStore/prophecy/artifacts/prophecy/" in result.output
                assert "[DONE]: Deployment completed successfully!" in result.output
            elif command is deploy_v2:
                assert "Deployment completed successfully." in result.output
        else:
            assert result.exit_code == 1


    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy_v2])  # airflow now supported by v1
    def test_deploy_path_default_airflow_jobs(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(command, ["--path", project_path, "--job-ids", "AirflowEndToEndJob"])
        print(result.output)
        #assert result.exit_code == 0

        # TODO get correct outputs. scala project requires airflow job so the two sides are mirrored.
        if command is deploy:
            assert "Found 1 jobs:" in result.output
            assert "Deploying 1 jobs" in result.output
            assert "Found 5 pipelines" in result.output
            assert "Deploying jobs for all Fabrics" in result.output
        elif command is deploy_v2:
            assert "Deploying databricks jobs" in result.output
            # If running with Databricks creds on GitHub Actions

        #
        # If running with Databricks creds on GitHub Actions
        if os.environ["DATABRICKS_HOST"] != "test":  #  TODO make sure airflow creds are set too for airflow live test
            assert result.exit_code == 0
            if command is deploy:
                assert "[DONE]: Deployment completed successfully!" in result.output
            elif command is deploy_v2:
                assert "Deployment completed successfully." in result.output
        else:
            assert result.exit_code == 1



    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy, deploy_v2])  # TODO deploy_v2 does not work here.
    def test_deploy_path_default_skip_builds(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(command, ["--path", project_path, "--skip-builds"])
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


 ################ TODO below this line ####################
#### make a second job in databricks and airflow with print statements.
    #####

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy_v2])
    def test_deploy_path_fabric_id_filter(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(command, ["--path", project_path, "--fabric-ids", "16432"])

        # assert "Found 2 jobs: test-job1234, job-another" in result.output
        # assert (
        #     "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), "
        #     "farmers-markets-irs (python)" in result.output
        # )
        # assert "Deploying jobs only for given Fabric IDs: ['647']" in result.output
        # assert "[START]:  Deploying job jobs/test-job" in result.output
        # assert "[DEPLOY]: Job being deployed for fabric id: 647" in result.output
        # assert "[START]:  Deploying job jobs/job-another" in result.output
        # assert "[SKIP]: Job skipped as it belongs to fabric id (not passed): 648" in result.output

        #TODO there are only 1 db job and 1 airflow job. can't test v1

        assert "skipped as it belongs to fabric-id" in result.output
        assert "Deployment completed successfully." in result.output


    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy_v2])
    def test_deploy_path_pipeline_invalid_fabric_id(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(deploy, ["--path", project_path, "--fabric-ids", "999999"])
        assert "Found 2 jobs:" in result.output
        assert (
            "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), "
            "farmers-markets-irs (python)" in result.output
        )
        assert "Deploying jobs only for given Fabric IDs: ['999']" in result.output
        assert "[START]:  Deploying job jobs/test-job" in result.output
        assert "[SKIP]: Job skipped as it belongs to fabric id (not passed): 647" in result.output
        assert "[START]:  Deploying job jobs/job-another" in result.output
        assert "[SKIP] Job " in result.output


    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy_v2])
    def test_deploy_with_fabric_id_and_job_id_filter(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(deploy, ["--path", project_path, "--fabric-ids", "999", "--job-ids", "test-job"])
        assert result.exit_code == 1
        assert "[ERROR]: Can't combine filters, Please pass either --fabric_ids or --job_id" in result.output


    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy_v2])
    def test_deploy_with_job_id_filter_and_skip_builds(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(deploy, ["--path", project_path, "--job-ids", "test-job", "--skip-builds"])
        assert result.exit_code == 1
        assert (
            "[ERROR]: Can't skip builds for job_id filter,\nas it only builds depending pipelines ,\nPlease pass "
            "either --skip-builds or --job_id filter" in result.output
        )


    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy_v2])
    def test_deploy_path_pipeline_with_job_id_filter(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(deploy, ["--path", project_path, "--job-ids", "test-job"])
    
        assert "Found 2 jobs: test-job1234, job-another" in result.output
        assert (
            "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), "
            "farmers-markets-irs (python)" in result.output
        )
        assert "Deploying jobs only for given Job IDs: ['test-job']" in result.output
        assert "[INFO]: Total Unique pipelines dependencies found: 3" in result.output
        assert "[INFO]: Building given custom pipelines" in result.output
        assert "[INFO]: Generating depending pipelines for all jobs" in result.output
        assert "Building 3 pipelines" in result.output
        assert "Building pipeline pipelines/customers_orders" in result.output
        assert "Building pipeline pipelines/report_top_customers" in result.output
        assert "Building pipeline pipelines/join_agg_sort" in result.output
        assert "Deploying 1 jobs" in result.output
        assert "[START]:  Deploying job jobs/test-job" in result.output
        assert "Deploying job jobs/job-another" not in result.output


    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy_v2])
    def test_deploy_path_pipeline_with_multiple_job_id_filter(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(deploy, ["--path", project_path, "--job-ids", "test-job,job-another"])
    
        assert "Found 2 jobs: test-job1234, job-another" in result.output
        assert (
            "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), "
            "farmers-markets-irs (python)" in result.output
        )
        assert "Deploying jobs only for given Job IDs: ['test-job', 'job-another']" in result.output
        assert "[INFO]: Total Unique pipelines dependencies found: 4" in result.output
        assert "[INFO]: Building given custom pipelines" in result.output
        assert "Building 4 pipelines" in result.output
        assert "Building pipeline pipelines/customers_orders" in result.output
        assert "Building pipeline pipelines/report_top_customers" in result.output
        assert "Building pipeline pipelines/join_agg_sort" in result.output
        assert "Building pipeline pipelines/farmers-markets-irs" in result.output
        assert "Deploying 2 jobs" in result.output
        assert "[START]:  Deploying job jobs/test-job" in result.output
        assert "[START]:  Deploying job jobs/job-another" in result.output


    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy_v2])
    def test_deploy_path_pipeline_with_one_invalid_job_id_filter(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(deploy, ["--path", project_path, "--job-ids", "invalid1,test-job"])
    
        assert "Found 2 jobs: test-job1234, job-another" in result.output
        assert (
            "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), "
            "farmers-markets-irs (python)" in result.output
        )
        assert "Deploying jobs only for given Job IDs: ['invalid1', 'test-job']" in result.output
        assert "[INFO]: Total Unique pipelines dependencies found: 3" in result.output
        assert "[INFO]: Building given custom pipelines" in result.output
        assert "Building 3 pipelines" in result.output
        assert "Building pipeline pipelines/customers_orders" in result.output
        assert "Building pipeline pipelines/report_top_customers" in result.output
        assert "Building pipeline pipelines/join_agg_sort" in result.output
        assert "Deploying 1 jobs" in result.output
        assert "[START]:  Deploying job jobs/test-job" in result.output


    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [deploy_v2])
    def test_deploy_path_pipeline_with_all_invalid_job_ids_filter(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(deploy, ["--path", project_path, "--job-ids", "invalid1,invalid2"])
    
        assert result.exit_code == 1
        assert "Found 2 jobs: test-job1234, job-another" in result.output
        assert (
            "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), "
            "farmers-markets-irs (python)" in result.output
        )
        assert "Deploying jobs only for given Job IDs: ['invalid1', 'invalid2']" in result.output
        assert (
            "[ERROR]: No Job IDs matches with passed --job_id filter ['invalid1', 'invalid2']\nAvailable Job IDs are: "
            "dict_keys(['jobs/test-job', 'jobs/job-another']" in result.output
        )
