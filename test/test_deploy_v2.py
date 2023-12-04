import os

from click.testing import CliRunner

from src.pbt import deploy_v2, validate_v2, build_v2

PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorld"
PROJECT_PATH_NEW = str(os.getcwd()) + "/test/resources/ProjectCreatedOn160523"
PROJECT_PATH_SUB = str(os.getcwd()) + "/resources/SubProject"
PROJECT_PATH_Base = str(os.getcwd()) + "/resources/BaseDirectory"
PROJECT_PATH_New_Project = str(os.getcwd()) + "/resources/NewProject"
PROJECT_PATH_DELTA = str(os.getcwd()) + "/resources/Omega/delta/delta"
PROJECT_SCALA_WITH_AIRFLOW = str(os.getcwd()) + "/resources/scala_with_airflow"
PROJECT_SCALA_WITH_AIRFLOW2 = str(os.getcwd()) + "/resources/scala_with_airflow2"
PROJECT_SCALA_SANITY = str(os.getcwd()) + "/resources/Scala_sanity_project_01"

if os.environ.get("DATABRICKS_HOST") is None:
    os.environ["DATABRICKS_HOST"] = "https://dbc-ac0e9adb-13fb.cloud.databricks.com"
if os.environ.get("DATABRICKS_TOKEN") is None:
    os.environ["DATABRICKS_TOKEN"] = "dapi01634aff5c18139ecafecb9cc0e0e976"


def test_deploy_path_default():
    runner = CliRunner()
    result = runner.invoke(deploy_v2, ["--path", PROJECT_PATH])

    assert "Building Pipelines 4" in result.output
    assert "Building pipeline pipelines/customers_orders" in result.output
    assert "Build was successful with exit code 0" in result.output
    assert "Building pipeline pipelines/report_top_customers" in result.output
    assert "Building pipeline pipelines/join_agg_sort" in result.output
    assert "Building pipeline pipelines/farmers-markets-irs" in result.output

    if os.environ.get("DATABRICKS_HOST") != "test":
        assert "Refreshed job jobs/test-job in fabric  response " in result.output
        assert "Refreshed job jobs/job-another in fabric " in result.output


def test_build_new_project():
    runner = CliRunner()
    result = runner.invoke(build_v2, ["--path", PROJECT_PATH_New_Project, "--pipelines", "Pipeline01"])

    print(result.output)


def test_build_new_project_no_filter():
    runner = CliRunner()
    result = runner.invoke(build_v2, ["--path", PROJECT_PATH_New_Project])

    print(result.output)


def test_deploy_v2_new_project_skip_deploy_delta():
    runner = CliRunner()
    result = runner.invoke(deploy_v2, ["--path", PROJECT_PATH_DELTA, "--release-version", "1.0",
                                       "--project-id", "1", "--conf-dir",
                                       "/Users/pankaj/workplace/pbt/test/resources/Omega/delta/config"])

    print(result.output)


def test_deploy_v2_new_project_scala_with_airflow():
    runner = CliRunner()
    result = runner.invoke(deploy_v2, ["--path", PROJECT_SCALA_WITH_AIRFLOW, "--release-version", "1.0",
                                       "--project-id", "1"])

    print(result.output)


def test_deploy_v2_new_project_scala_with_airflow2():
    runner = CliRunner()
    result = runner.invoke(deploy_v2, ["--path", PROJECT_SCALA_WITH_AIRFLOW2, "--conf-dir", PROJECT_SCALA_WITH_AIRFLOW2,
                                       "--release-version", "1.0",
                                       "--project-id", "1"])

    print(result.output)


def test_deploy_v2_new_project_scala_with_airflow2_selective_deploy_for_fabric():
    runner = CliRunner()
    result = runner.invoke(deploy_v2, ["--path", PROJECT_SCALA_WITH_AIRFLOW2, "--conf-dir", PROJECT_SCALA_WITH_AIRFLOW2,
                                       "--fabric-ids", "735",
                                       "--release-version", "1.0",
                                       "--project-id", "1"])

    print(result.output)

def test_deploy_v2_new_project_scala_with_airflow2_selective_deploy_for_jobs():
    runner = CliRunner()
    result = runner.invoke(deploy_v2, ["--path", PROJECT_SCALA_WITH_AIRFLOW2, "--conf-dir", PROJECT_SCALA_WITH_AIRFLOW2,
                                       "--job-ids", "airflow_sca",
                                       "--release-version", "1.0",
                                       "--project-id", "1"])

    print(result.output)


def test_deploy_v2_new_project_skip_build():
    runner = CliRunner()
    result = runner.invoke(deploy_v2, ["--path", PROJECT_PATH_New_Project, "--skip-builds", "--release-version", "1.0",
                                       "--project-id", "1"])

    print(result.output)


def test_deploy_v2_new_project_filter_fabrics():
    runner = CliRunner()
    result = runner.invoke(deploy_v2,
                           ["--path", PROJECT_PATH_New_Project, "--skip-builds", "--fabric-ids", "234",
                            "--release-version", "1.0",
                            "--project-id", "1"])

    print(result.output)


def test_deploy_v2_new_project_filter_jobs():
    runner = CliRunner()
    result = runner.invoke(deploy_v2,
                           ["--path", PROJECT_PATH_New_Project, "--job-ids", "newJob01,newJob03", "--release-version",
                            "1.0",
                            "--project-id", "1"])

    print(result.output)


def test_deploy_v2_new_project_job_with_scripts():
    runner = CliRunner()
    result = runner.invoke(deploy_v2,
                           ["--path", PROJECT_PATH_New_Project, "--job-ids", "SecondJob01", "--release-version", "1.0",
                            "--project-id", "1"])

    print(result.output)


def test_build_v2_no_base_project_pipeline_build():
    runner = CliRunner()
    result = runner.invoke(build_v2,
                           ["--path", PROJECT_PATH_SUB])

    print(result.output)


def test_deploy_v2_no_base_project_pipeline_build():
    runner = CliRunner()
    result = runner.invoke(deploy_v2,
                           ["--path", PROJECT_PATH_SUB, "--release-version", "1.0", "--project-id", "1",
                            "--dependent-projects-path", PROJECT_PATH_Base])

    print(result.output)

def test_deploy_v2_scala_sanity_project():
    runner = CliRunner()
    result = runner.invoke(deploy_v2,
                           ["--path", PROJECT_SCALA_SANITY, "--release-version", "1.0", "--project-id", "1"])

    print(result.output)
# def test_deploy_path_default_new_project():
    #     runner = CliRunner()
    #     result = runner.invoke(deploy_v2, ["--path", PROJECT_PATH_NEW, "--release-version", "1.0", "--project-id", "1"])
    #
    #     print(result.output)
    #
    #     assert "Building Pipelines 2" in result.output
    #     assert "Building pipeline pipelines/AutomatedPBT-truescala" in result.output
    #     assert "Build was successful with exit code 0" in result.output
    #     assert "Building pipeline pipelines/AutomatedPBTNo-truescala" in result.output
    #
    #     if os.environ.get("DATABRICKS_HOST") != "test":
    #         assert "Refreshed job jobs/AutomatedPBT-truescala in fabric" in result.output
    #         assert "Refreshed job jobs/AutomatedPBTNo-truescala in fabric " in result.output
    #
    #
    # def test_deploy_with_dependant_path():
    #     runner = CliRunner()
    #     result = runner.invoke(deploy_v2, ["--path", PROJECT_PATH_SUB, "--release-version", "1.0", "--project-id", "1",
    #                                        "--dependent-projects-path", PROJECT_PATH_Base])
    #
    #     print(result.output)
    #
    #     assert "Building Pipelines 1" in result.output
    #     assert "Building pipeline gitUri=http://gitserver:3000/Jf9ltE5z_team_199/NpYHJr6Q_project_1767.git&subPath=&tag=BaseProject/0.1&projectSubscriptionProjectId=1767&path=pipelines/FirstPipeline" in result.output
    #     assert "Build was successful with exit code 0" in result.output
    #
    #     if os.environ.get("DATABRICKS_HOST") != "test":
    #         assert "Refreshed job jobs/Job01 in fabric " in result.output


def test_validate():
    runner = CliRunner()
    result = runner.invoke(validate_v2, ["--path", PROJECT_PATH])

    # print(result.output)

    assert "Pipeline is validated: customers_orders1243" in result.output
    assert "Pipeline is validated: report_top_customers" in result.output
    assert "Pipeline is validated: join_agg_sort" in result.output
    assert "Pipeline is validated: farmers-markets-irs" in result.output


def test_build():
    runner = CliRunner()
    result = runner.invoke(build_v2, ["--path", PROJECT_PATH])

    assert "Building pipelines 4" in result.output

    assert "Building pipeline `pipelines/customers_orders`" in result.output
    assert "Build for pipeline pipelines/customers_orders succeeded" in result.output

    assert "Building pipeline `pipelines/report_top_customers`" in result.output
    assert "Build for pipeline pipelines/report_top_customers succeeded" in result.output

    assert "Building pipeline `pipelines/join_agg_sort`" in result.output
    assert "Build for pipeline pipelines/join_agg_sort succeeded" in result.output

    assert "Building pipeline `pipelines/farmers-markets-irs`" in result.output
    assert "Build for pipeline pipelines/farmers-markets-irs succeeded" in result.output
