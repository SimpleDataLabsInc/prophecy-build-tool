from click.testing import CliRunner
from src.pbt import deploy
import os

PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorld"
os.environ["DATABRICKS_HOST"] = "test"
os.environ["DATABRICKS_TOKEN"] = "test"


def test_deploy_path_default():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH])
    print(result.output)
    # assert result.exit_code == 0 # this will be 1 as databricks token is invalid
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python), join_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
    assert "Deploying 2 jobs" in result.output
    assert "Deploying jobs for all Fabrics" in result.output
    assert "[START]:  Deploying job jobs/test-job" in result.output
    assert "[START]:  Deploying job jobs/job-another" in result.output


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


def test_build_path_pipeline_invalid_fabric_id():
    runner = CliRunner()
    result = runner.invoke(deploy, ["--path", PROJECT_PATH, "--fabric-ids", "999"])
    print(result.output)
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), " \
           "farmers-markets-irs (python)" in result.output
    assert "Deploying jobs only for given Fabric IDs: ['999']" in result.output
    assert "[START]:  Deploying job jobs/test-job" in result.output
    assert "[SKIP]: Job skipped as it belongs to fabric id (not passed): 647" in result.output
    assert "[START]:  Deploying job jobs/job-another" in result.output
    assert "[SKIP]: Job skipped as it belongs to fabric id (not passed): 648" in result.output


def test_build_path_pipeline_with_jobid_filter():
    runner = CliRunner()
    result = runner.invoke(
        build,
        [
            "--path",
            PROJECT_PATH,
            "--pipelines",
            "customers_orders,INVALID_PIPELINE_NAME",
        ],
    )
    print(result.output)
    assert result.exit_code == 0
    assert "Found 3 pipelines" in result.output
    assert "Building 1 pipelines" in result.output
    assert "Filtering pipelines: ['customers_orders', 'INVALID_PIPELINE_NAME']" in result.output
    assert "Building pipeline pipelines/customers_orders" in result.output
