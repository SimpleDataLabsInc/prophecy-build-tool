from click.testing import CliRunner
from src.pbt import build, build_v2
import os

PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorld"
ERROR_PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorldBuildError"


def test_build_v2_binary_check(monkeypatch):
    monkeypatch.setenv("PATH", "")
    runner = CliRunner()
    result = runner.invoke(build_v2, ["--path", PROJECT_PATH])
    print(result.output)
    assert result.exit_code == 1
    assert "ERROR: no `python3` or `python` found" in result.output


def test_build_path_default():
    runner = CliRunner()
    result = runner.invoke(build, ["--path", PROJECT_PATH])
    assert result.exit_code == 0
    assert "Found 4 pipelines" in result.output
    assert "Building 4 pipelines" in result.output
    assert "Building pipeline pipelines/customers_orders" in result.output
    assert "Building pipeline pipelines/join_agg_sort" in result.output
    assert "Building pipeline pipelines/report_top_customers" in result.output
    assert "Building pipeline pipelines/farmers-markets-irs" in result.output


def test_build_v2_path_default():
    runner = CliRunner()
    result = runner.invoke(build_v2, ["--path", PROJECT_PATH])
    assert result.exit_code == 0
    assert "Found 4 pipelines" in result.output
    assert "Building pipelines 4" in result.output
    assert "Building pipeline `customers_orders" in result.output
    assert "Building pipeline `join_agg_sort" in result.output
    assert "Building pipeline `report_top_customers" in result.output
    assert "Building pipeline `farmers-markets-irs" in result.output


def test_build_v2_path_default_build_errors():
    runner = CliRunner()
    result = runner.invoke(build_v2, ["--path", ERROR_PROJECT_PATH])
    assert result.exit_code == 1


def test_build_v2_path_default_build_errors_ignore_errors():
    runner = CliRunner()
    result = runner.invoke(build_v2, ["--path", ERROR_PROJECT_PATH, "--ignore-build-errors"])
    assert result.exit_code == 0


def test_build_path_pipeline_filter():
    runner = CliRunner()
    result = runner.invoke(build, ["--path", PROJECT_PATH, "--pipelines", "customers_orders,join_agg_sort"])
    assert result.exit_code == 0
    assert "Found 4 pipelines" in result.output
    assert "Building 2 pipelines" in result.output
    assert "Filtering pipelines: ['customers_orders', 'join_agg_sort']" in result.output
    assert "Building pipeline pipelines/customers_orders" in result.output
    assert "Building pipeline pipelines/join_agg_sort" in result.output


def test_build_path_pipeline_with_invalid_filter():
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
    assert result.exit_code == 0
    assert "Found 4 pipelines" in result.output
    assert "Building 1 pipelines" in result.output
    assert "Filtering pipelines: ['customers_orders', 'INVALID_PIPELINE_NAME']" in result.output
    assert "Building pipeline pipelines/customers_orders" in result.output


def test_build_path_pipeline_invalid_filter_only():
    runner = CliRunner()
    result = runner.invoke(build, ["--path", PROJECT_PATH, "--pipelines", "INVALID_PIPELINE_NAME"])
    assert result.exit_code == 1
    assert "Found 4 pipelines" in result.output
    assert "No matching pipelines found for given pipelines names: ['INVALID_PIPELINE_NAME']" in result.output
