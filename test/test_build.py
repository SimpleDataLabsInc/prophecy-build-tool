from click.testing import CliRunner
from src.pbt import build

PROJECT_PATH = "./HelloWorld"


def test_build_path_default():
    runner = CliRunner()
    result = runner.invoke(build, ['--path', PROJECT_PATH])
    print(result.exit_code)
    print(result.output)
    assert result.exit_code == 0
    assert "Found 4 pipelines" in result.output
    assert "Building pipeline pipelines/customers_orders [1/4]" in result.output
    assert "Building pipeline pipelines/farmers-markets-irs [2/4]" in result.output
    assert "Building pipeline pipelines/join_agg_sort [3/4]" in result.output
    assert "Building pipeline pipelines/report_top_customers [4/4]" in result.output


def test_build_path_pipeline_filter():
    runner = CliRunner()
    result = runner.invoke(build, ['--path', PROJECT_PATH, '--pipelines', 'customers_orders,join_agg_sort'])
    print(result.exit_code)
    print(result.output)
    assert result.exit_code == 0
    assert "Found 2 pipelines" in result.output
    assert "Bulding 4 pipelines" in result.output
    assert "Building pipeline pipelines/customers_orders [1/4]" in result.output
    assert "Building pipeline pipelines/farmers-markets-irs [2/4]" in result.output
    assert "Building pipeline pipelines/join_agg_sort [3/4]" in result.output
    assert "Building pipeline pipelines/report_top_customers [4/4]" in result.output

