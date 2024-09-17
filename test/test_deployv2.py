import os

from click.testing import CliRunner

from src.pbt import deploy_v2

PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorld"
PROJECT_PATH_NEW = str(os.getcwd()) + "/test/resources/ProjectCreatedOn160523"
if os.environ.get("DATABRICKS_HOST") is None:
    os.environ["DATABRICKS_HOST"] = "test"
if os.environ.get("DATABRICKS_TOKEN") is None:
    os.environ["DATABRICKS_TOKEN"] = "test"


def test_deploy_v2_path_default():
    runner = CliRunner()
    result = runner.invoke(deploy_v2, ["--path", PROJECT_PATH])
    print(result.output)
    # assert "Found 2 jobs: test-job1234, job-another" in result.output
    # assert (
    #     "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), "
    #     "farmers-markets-irs (python)" in result.output
    # )
    # assert "Deploying 2 jobs" in result.output
    # assert "Deploying jobs for all Fabrics" in result.output
    # assert "[START]:  Deploying job jobs/test-job" in result.output
    # assert "[START]:  Deploying job jobs/job-another" in result.output
