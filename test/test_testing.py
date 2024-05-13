from click.testing import CliRunner
from src.pbt import test
import os

PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorld"


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
    assert "Unit Testing pipeline pipelines/report_top_customers" in result.output
    assert "Unit Testing pipeline pipelines/join_agg_sort" in result.output
    assert "Unit Testing pipeline pipelines/farmers-markets-irs" in result.output


def test_test_with_pipeline_filter():
    runner = CliRunner()
    result = runner.invoke(test, ["--path", PROJECT_PATH, "--pipelines", "report_top_customers,join_agg_sort"])
    print(result.output)
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert (
        "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python),\njoin_agg_sort (python), "
        "farmers-markets-irs (python)" in result.output
    )
    assert "Pipeline Filters passed [2]: ['report_top_customers', 'join_agg_sort']" in result.output
    assert "Unit Testing pipeline pipelines/customers_orders" not in result.output
    assert "Unit Testing pipeline pipelines/report_top_customers" in result.output
    assert "Unit Testing pipeline pipelines/join_agg_sort" in result.output
    assert "Unit Testing pipeline pipelines/farmers-markets-irs" not in result.output


def test_test_with_pipeline_filter_one_notfound_pipeline():
    runner = CliRunner()
    result = runner.invoke(test, ["--path", PROJECT_PATH, "--pipelines", "report_top_customers,notfound"])
    print(result.output)
    assert "Pipeline Filters passed [2]: ['report_top_customers', 'notfound']" in result.output
    assert "Pipelines found [1]" in result.output
    assert "Filtered pipelines doesn't match with passed filter" in result.output


def test_test_with_pipeline_filter_all_notfound_pipelines():
    runner = CliRunner()
    result = runner.invoke(test, ["--path", PROJECT_PATH, "--pipelines", "notfound1,notfound2,notfound3"])
    print(result.output)
    assert "Pipeline Filters passed [3]: ['notfound1', 'notfound2', 'notfound3']" in result.output
    assert "Pipelines found [0]" in result.output
    assert "Filtered pipelines doesn't match with passed filter" in result.output
    assert "Coverage XML written to file coverage.xml" not in result.output


def test_test_coverage_report_generation():
    runner = CliRunner()
    result = runner.invoke(test, ["--path", PROJECT_PATH, "--pipelines", "report_top_customers"])
    print(result.output)
    assert "Unit Testing pipeline pipelines/report_top_customers" in result.output
    assert "Coverage XML written to file coverage.xml" in result.output
