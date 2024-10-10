from click.testing import CliRunner
from src.pbt import test, test_v2
import os

PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorld"


def test_test_v2_driver_paths1():
    runner = CliRunner()
    with open("./fake.jar", "w") as fd:
        fd.write("fake")
    with open("./fake2.jar", "w") as fd:
        fd.write("fake")

    result = runner.invoke(test_v2, ["--path", PROJECT_PATH, "--driver-library-path", "./"])
    print(result.output)
    assert "fake.jar" in result.output.replace("\n", "")
    assert "fake2.jar" in result.output.replace("\n", "")


def test_test_v2_driver_paths2():
    runner = CliRunner()
    with open("./fake.jar", "w") as fd:
        fd.write("fake")
    with open("./fake2.jar", "w") as fd:
        fd.write("fake")

    result = runner.invoke(test_v2, ["--path", PROJECT_PATH, "--driver-library-path", "./fake.jar,fake2.jar"])
    print(result.output)
    assert "fake.jar" in result.output.replace("\n", "")
    assert "fake2.jar" in result.output.replace("\n", "")


def test_test_v2_driver_paths3():
    runner = CliRunner()
    with open("./fake.jar", "w") as fd:
        fd.write("fake")
    with open("./fake2.jar", "w") as fd:
        fd.write("fake")

    result = runner.invoke(test_v2, ["--path", PROJECT_PATH, "--driver-library-path", os.getcwd()])
    print(result.output)
    assert "fake.jar" in result.output.replace("\n", "")
    assert "fake2.jar" in result.output.replace("\n", "")


def test_test_driver_paths():
    runner = CliRunner()
    with open("./fake.jar", "w") as fd:
        fd.write("fake")
    with open("./fake2.jar", "w") as fd:
        fd.write("fake")

    result = runner.invoke(test, ["--path", PROJECT_PATH, "--driver-library-path", "./"])
    print(result.output)
    assert "fake.jar" in result.output.replace("\n", "")
    assert "fake2.jar" in result.output.replace("\n", "")


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
    assert "Using default spark jars locations" in result.output


def test_test_v2_path_default():
    runner = CliRunner()
    result = runner.invoke(test_v2, ["--path", PROJECT_PATH])
    print(result.output)
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert (
        "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python), join_agg_sort (python), farmers-markets-irs (python)"
        in result.output
    )
    assert "Testing pipelines" in result.output
    assert "Pipeline test succeeded : `pipelines/customers_orders`" in result.output
    assert "Pipeline test succeeded : `pipelines/report_top_customers`" in result.output
    assert "Pipeline test succeeded : `pipelines/join_agg_sort`" in result.output
    assert "Pipeline test succeeded : `pipelines/farmers-markets-irs`" in result.output


def test_test_v2_path_relative():
    runner = CliRunner()
    result = runner.invoke(test_v2, ["--path", os.path.relpath(PROJECT_PATH, os.getcwd())])
    print(result.output)
    assert "Found 2 jobs: test-job1234, job-another" in result.output
    assert (
        "Found 4 pipelines: customers_orders1243 (python), report_top_customers (python), join_agg_sort (python), farmers-markets-irs (python)"
        in result.output
    )
    assert "Testing pipelines" in result.output
    assert "Pipeline test succeeded : `pipelines/customers_orders`" in result.output
    assert "Pipeline test succeeded : `pipelines/report_top_customers`" in result.output
    assert "Pipeline test succeeded : `pipelines/join_agg_sort`" in result.output
    assert "Pipeline test succeeded : `pipelines/farmers-markets-irs`" in result.output


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


def test_test_coverage_and_test_report_generation():
    coverage_path = os.path.join(PROJECT_PATH, "./pipelines/customers_orders/code/coverage.xml")
    if os.path.exists(coverage_path):
        os.remove(coverage_path)
    coveragerc_path = os.path.join(PROJECT_PATH, "./pipelines/customers_orders/code/.coveragerc")
    if os.path.exists(coveragerc_path):
        os.remove(coveragerc_path)
    runner = CliRunner()
    result = runner.invoke(test, ["--path", PROJECT_PATH, "--pipelines", "customers_orders"])
    print(result.output)
    assert "Unit Testing pipeline pipelines/customers_orders" in result.output
    assert os.path.exists(coverage_path)
    assert os.path.exists(os.path.join(PROJECT_PATH, "./pipelines/customers_orders/code/report.xml"))

    with open(coverage_path, "r") as fd:
        content = fd.read()
        # check to make sure that .coveragerc got picked up and made absolute paths:
        assert "<source>/" in content
        # verify that some coverage was written
        assert '<package name="job"' in content
        # check that setup.py is ignored
        assert '<class name="setup.py"' not in content
        # make sure we are not doing coverage for test directory
        assert '<package name="test"' not in content


def test_test_v2_coverage_and_test_report_generation():
    coverage_path = os.path.join(PROJECT_PATH, "./pipelines/customers_orders/code/coverage.xml")
    if os.path.exists(coverage_path):
        os.remove(coverage_path)
    coveragerc_path = os.path.join(PROJECT_PATH, "./pipelines/customers_orders/code/.coveragerc")
    if os.path.exists(coveragerc_path):
        os.remove(coveragerc_path)
    runner = CliRunner()
    result = runner.invoke(test_v2, ["--path", PROJECT_PATH])
    print(result.output)
    assert "Testing pipeline `pipelines/customers_orders`" in result.output
    assert "Coverage XML written to file coverage.xml" in result.output
    assert os.path.exists(coverage_path)
    assert os.path.exists(os.path.join(PROJECT_PATH, "./pipelines/customers_orders/code/report.xml"))

    assert os.path.exists(os.path.join(PROJECT_PATH, "./pipelines/customers_orders/code/report.html"))

    with open(coverage_path, "r") as fd:
        content = fd.read()
        # check to make sure that .coveragerc got picked up and made absolute paths:
        assert "<source>/" in content
        # verify that some coverage was written
        assert '<package name="job"' in content
        # check that setup.py is ignored
        assert '<class name="setup.py"' not in content
        # make sure we are not doing coverage for test directory
        assert '<package name="test"' not in content
