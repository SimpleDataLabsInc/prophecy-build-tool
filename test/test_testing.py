from click.testing import CliRunner
from src.pbt import test
import os

PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorld"
PROJECT_PATH_NEW = str(os.getcwd()) + "/test/resources/ProjectCreatedOn160523"
if os.environ.get("DATABRICKS_HOST") is None:
    os.environ["DATABRICKS_HOST"] = "test"
if os.environ.get("DATABRICKS_TOKEN") is None:
    os.environ["DATABRICKS_TOKEN"] = "test"


def test_test_path_default():
    runner = CliRunner()
    result = runner.invoke(test, ["--path", PROJECT_PATH])
    print(result.output)
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert (
            "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), "
            "farmers-markets-irs (python)" in result.output
    )
    assert "Testing All pipelines" in result.output
    assert "Unit Testing pipeline pipelines/customers_orders" in result.output
    assert "Unit test for pipeline: pipelines/customers_orders succeeded" in result.output

    assert "Unit Testing pipeline pipelines/report_top_customers" in result.output
    assert "Unit test for pipeline: pipelines/report_top_customers succeeded" in result.output

    assert "Unit Testing pipeline pipelines/join_agg_sort" in result.output
    assert "Unit test for pipeline: pipelines/join_agg_sort succeeded" in result.output

    assert "Unit Testing pipeline pipelines/farmers-markets-irs" in result.output
    assert "Unit test for pipeline: pipelines/farmers-markets-irs succeeded" in result.output


def test_test_with_pipeline_filter():
    runner = CliRunner()
    result = runner.invoke(test, ["--path", PROJECT_PATH, '--pipelines', 'report_top_customers,join_agg_sort'])
    print(result.output)
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert (
            "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), "
            "farmers-markets-irs (python)" in result.output
    )
    assert "Testing given pipelines: ['report_top_customers', 'join_agg_sort']" in result.output

    assert "Unit Testing pipeline pipelines/customers_orders" not in result.output

    assert "Unit Testing pipeline pipelines/report_top_customers" in result.output
    assert "Unit test for pipeline: pipelines/report_top_customers succeeded" in result.output

    assert "Unit Testing pipeline pipelines/join_agg_sort" in result.output
    assert "Unit test for pipeline: pipelines/join_agg_sort succeeded" in result.output

    assert "Unit Testing pipeline pipelines/farmers-markets-irs" not in result.output

def test_test_with_pipeline_filter_one_invalid_pipeline():
    runner = CliRunner()
    result = runner.invoke(test, ["--path", PROJECT_PATH, '--pipelines', 'report_top_customers,invalid'])
    print(result.output)
    assert "Testing given pipelines: ['report_top_customers', 'invalid']" in result.output

    assert "Unit Testing pipeline pipelines/report_top_customers" in result.output
    assert "Unit test for pipeline: pipelines/report_top_customers succeeded" in result.output
    assert "Pipelines found: 1" in result.output

def test_test_with_pipeline_filter_all_invalid_pipelines():
    runner = CliRunner()
    result = runner.invoke(test, ["--path", PROJECT_PATH, '--pipelines', 'invalid1,invalid2,invalid3'])
    print(result.output)
    assert "Testing given pipelines: ['invalid1', 'invalid2', 'invalid3']" in result.output

    assert "Unit Testing pipeline pipelines/report_top_customers" in result.output
    assert "Unit test for pipeline: pipelines/report_top_customers succeeded" in result.output
    assert "Pipelines found: 0" in result.output


