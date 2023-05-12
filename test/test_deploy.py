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
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
    assert "Deploying jobs only for given Fabric IDs: ['999']" in result.output
    assert "[START]:  Deploying job jobs/test-job" in result.output
    assert "[SKIP]: Job skipped as it belongs to fabric id (not passed): 647" in result.output
    assert "[START]:  Deploying job jobs/job-another" in result.output
    assert "[SKIP]: Job skipped as it belongs to fabric id (not passed): 648" in result.output


def test_deploy_with_fabric_id_and_job_id_filter():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--fabric-ids", "999", "--job-ids", "test-job"])
    assert result.exit_code == 1
    assert "[ERROR]: Can't combine filters, Please pass either --fabric_ids or --job_id" in result.output


def test_deploy_with_job_id_filter_and_skip_builds():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--job-ids", "test-job", "--skip-builds"])
    assert result.exit_code == 1
    assert "[ERROR]: Can't skip builds for job_id filter,\nas it only builds depending pipelines ,\nPlease pass " \
           "either --skip-builds or --job_id filter" in result.output


def test_deploy_path_pipeline_with_job_id_filter():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--job-ids", "test-job"])

    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
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


def test_deploy_path_pipeline_with_multiple_job_id_filter():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--job-ids", "test-job,job-another"])

    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
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


def test_deploy_path_pipeline_with_one_invalid_job_id_filter():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--job-ids", "invalid1,test-job"])

    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
    assert "Deploying jobs only for given Job IDs: ['invalid1', 'test-job']" in result.output
    assert "[INFO]: Total Unique pipelines dependencies found: 3" in result.output
    assert "[INFO]: Building given custom pipelines" in result.output
    assert "Building 3 pipelines" in result.output
    assert "Building pipeline pipelines/customers_orders" in result.output
    assert "Building pipeline pipelines/report_top_customers" in result.output
    assert "Building pipeline pipelines/join_agg_sort" in result.output
    assert "Deploying 1 jobs" in result.output
    assert "[START]:  Deploying job jobs/test-job" in result.output


def test_deploy_path_pipeline_with_all_invalid_job_ids_filter():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--job-ids", "invalid1,invalid2"])

    assert result.exit_code == 1
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
    assert "Deploying jobs only for given Job IDs: ['invalid1', 'invalid2']" in result.output
    assert "[ERROR]: No Job IDs matches with passed --job_id filter ['invalid1', 'invalid2']\nAvailable Job IDs are: " \
           "dict_keys(['jobs/test-job', 'jobs/job-another']" in result.output
