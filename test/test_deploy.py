from click.testing import CliRunner
from src.pbt import deploy
import os

PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorld"
os.environ["DATABRICKS_HOST"] = "test"
os.environ["DATABRICKS_TOKEN"] = "test"


def test_deploy_path_default():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH])
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
    assert "Deploying 2 jobs" in result.output
    assert "Deploying jobs for all Fabrics" in result.output
    assert "[START]:  Deploying job jobs/test-job" in result.output
    assert "[START]:  Deploying job jobs/job-another" in result.output

def test_deploy_path_default_skip_builds():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--skip-builds"])
    print(result.output)
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "[SKIP]: Skipping builds for all pipelines as '--skip-builds' flag is passed." in result.output


def test_deploy_path_fabric_id_filter():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--fabric-ids", "647"])
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
    assert "Deploying jobs only for given Fabric IDs: ['647']" in result.output
    assert "[START]:  Deploying job jobs/test-job" in result.output
    assert "[DEPLOY]: Job being deployed for fabric id: 647" in result.output
    assert "[START]:  Deploying job jobs/job-another" in result.output
    assert "[SKIP]: Job skipped as it belongs to fabric id (not passed): 648" in result.output


def test_deploy_path_pipeline_invalid_fabric_id():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--fabric-ids", "999"])
    print(result.output)
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\n join_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
    assert "Deploying jobs only for given Fabric IDs: ['999']" in result.output
    assert "[START]:  Deploying job jobs/test-job" in result.output
    assert "[SKIP]: Job skipped as it belongs to fabric id (not passed): 647" in result.output
    assert "[START]:  Deploying job jobs/job-another" in result.output
    assert "[SKIP]: Job skipped as it belongs to fabric id (not passed): 648" in result.output

def test_deploy_with_fabric_id_and_job_id_filter():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--fabric-ids", "999", "--job-id", "test-job"])
    assert result.exit_code == 1
    assert "[ERROR]: Can't combine filters, Please pass either --fabric_ids or --job_id" in result.output


def test_deploy_with_job_id_filter_and_skip_builds():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--job-id", "test-job", "--skip-builds"])
    assert result.exit_code == 1
    assert "[ERROR]: Can't skip builds for job_id filter,\nas it only builds depending pipelines ,\nPlease pass " \
           "either --skip-builds or --job_id filter" in result.output

# ASHISH ==========> doing this
# finds all the pipelines needed -> add in tests
# only pass them fo builds -> add in assert
def test_deploy_path_pipeline_with_job_id_filter():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--job-id", "test-job"])

    print(result.output)
    assert result.exit_code == 0
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "[SKIP]: Skipping builds for all pipelines as '--skip-builds' flag is passed." in result.output
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
    assert "Filtering pipelines for job_id: test-job" in result.output
    assert "[START]:  Deploying job jobs/test-job" in result.output
    assert "[START]:  Deploying job jobs/job-another" not in result.output
