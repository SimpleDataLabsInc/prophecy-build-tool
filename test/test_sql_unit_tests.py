import os
import shutil
import tempfile
from unittest import mock

import pytest

from src.pbt.deployment.project import ProjectDeployment
from src.pbt.deployment.unit_tests import UNIT_TESTS, ProjectUnitTests
from src.pbt.utility import Either


@pytest.fixture
def sql_project_dir():
    directory = tempfile.mkdtemp()
    with open(os.path.join(directory, "pyproject.toml"), "w") as fd:
        fd.write('[project]\nname = "testsqlpy"\n')
    yield directory
    shutil.rmtree(directory, ignore_errors=True)


def _add_unit_test(project_dir, pipeline_name="schedule_test_multi", file_name="test_sales_reformatted.py"):
    test_dir = os.path.join(project_dir, "src", "pipelines", pipeline_name, "tests", "graph")
    os.makedirs(test_dir, exist_ok=True)
    with open(os.path.join(test_dir, file_name), "w") as fd:
        fd.write("class sales_reformattedTest:\n    pass\n")
    return test_dir


def _make_component(project_dir, tests_enabled=True, language="sql"):
    project = mock.MagicMock()
    project.project_language = language
    project.project_path = project_dir

    project_config = mock.MagicMock()
    project_config.configs_override.tests_enabled = tests_enabled

    return ProjectUnitTests(project, project_config)


class TestUnitTestDiscovery:

    def test_finds_generated_pyspark_test_folders_of_a_sql_project(self, sql_project_dir):
        expected = _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir)

        assert component.test_directories() == [expected]

    def test_finds_tests_matching_the_trailing_test_suffix_too(self, sql_project_dir):
        expected = _add_unit_test(sql_project_dir, file_name="sales_reformatted_test.py")
        component = _make_component(sql_project_dir)

        assert component.test_directories() == [expected]

    def test_spark_python_project_is_never_picked_up(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir, language="python")

        assert component.test_directories() == []
        assert component.headers() == []
        assert component.summary() == []

    def test_scala_project_is_never_picked_up(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir, language="scala")

        assert component.test_directories() == []
        assert component.headers() == []

    def test_sql_project_without_pyproject_is_not_supported(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        os.remove(os.path.join(sql_project_dir, "pyproject.toml"))
        component = _make_component(sql_project_dir)

        assert component.test_directories() == []
        assert component.headers() == []


class TestUnitTestSummaryAndHeaders:

    def test_summary_states_that_tests_are_disabled(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir, tests_enabled=False)

        assert component.summary() == [
            "Unit tests are disabled for this project, no unit tests will run during this release."
        ]

    def test_summary_states_that_there_are_no_unit_tests_to_run(self, sql_project_dir):
        component = _make_component(sql_project_dir)

        assert component.summary() == [
            "Unit tests are enabled but this project has no unit tests, none will run during this release."
        ]

    def test_summary_states_how_many_folders_will_run(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        _add_unit_test(sql_project_dir, pipeline_name="p1", file_name="test_order_amount_sum.py")
        component = _make_component(sql_project_dir)

        assert component.summary() == [
            "Unit tests are enabled, unit tests from 2 folder(s) will run before deployment."
        ]

    def test_header_step_is_emitted_only_when_tests_will_run(self, sql_project_dir):
        _add_unit_test(sql_project_dir)

        enabled = _make_component(sql_project_dir)
        assert [header.id for header in enabled.headers()] == [UNIT_TESTS]
        assert enabled.headers()[0].heading == "Run unit tests"

        disabled = _make_component(sql_project_dir, tests_enabled=False)
        assert disabled.headers() == []

    def test_header_step_is_not_emitted_when_project_has_no_unit_tests(self, sql_project_dir):
        assert _make_component(sql_project_dir).headers() == []


class TestUnitTestExecution:

    def test_failing_unit_tests_produce_a_left_so_the_release_fails(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir)

        with mock.patch.object(ProjectUnitTests, "_run", side_effect=[0, 1]):
            responses = component.deploy()

        assert len(responses) == 1
        assert responses[0].is_left
        assert "exit code 1" in str(responses[0].left)

    def test_passing_unit_tests_produce_a_right(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir)

        with mock.patch.object(ProjectUnitTests, "_run", side_effect=[0, 0]):
            responses = component.deploy()

        assert len(responses) == 1
        assert responses[0].is_right

    def test_no_tests_collected_is_treated_as_a_pass(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir)

        with mock.patch.object(ProjectUnitTests, "_run", side_effect=[0, 5]):
            responses = component.deploy()

        assert responses[0].is_right

    def test_failing_project_install_produces_a_left_and_skips_pytest(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir)

        with mock.patch.object(ProjectUnitTests, "_run", side_effect=[1]) as runner:
            responses = component.deploy()

        assert responses[0].is_left
        assert "install the project" in str(responses[0].left)
        assert runner.call_count == 1

    def test_nothing_runs_when_tests_are_disabled(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir, tests_enabled=False)

        with mock.patch.object(ProjectUnitTests, "_run") as runner:
            assert component.deploy() == []

        runner.assert_not_called()

    def test_nothing_runs_for_a_spark_python_project(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir, language="python")

        with mock.patch.object(ProjectUnitTests, "_run") as runner:
            assert component.deploy() == []

        runner.assert_not_called()

    def test_project_is_installed_into_a_throwaway_dir_kept_off_the_shared_environment(self, sql_project_dir):
        test_dir = _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir)
        calls = []

        def record(command, env):
            calls.append((command, env))
            return 0

        with mock.patch.object(ProjectUnitTests, "_run", side_effect=record):
            with mock.patch("src.pbt.deployment.unit_tests.get_python_commands", return_value=("python3", "pip3")):
                component.deploy()

        install_command, install_env = calls[0]
        test_command, test_env = calls[1]

        assert install_command[:5] == ["python3", "-m", "pip", "install", "--disable-pip-version-check"]
        assert "--no-deps" in install_command
        assert install_command[-1] == "."

        target_dir = install_command[install_command.index("--target") + 1]
        assert target_dir != sql_project_dir
        assert install_env["PYTHONPATH"].startswith(target_dir)
        assert test_env["PYTHONPATH"].startswith(target_dir)
        assert test_env["FABRIC_NAME"] == "default"

        assert test_command[:3] == ["python3", "-m", "pytest"]
        assert test_command[-1] == test_dir

    def test_pytest_debugging_plugin_is_replaced_by_the_stub(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir)
        calls = []

        with mock.patch.object(ProjectUnitTests, "_run", side_effect=lambda command, env: calls.append(command) or 0):
            component.deploy()

        test_command = calls[1]
        assert "no:debugging" in test_command
        assert "pbt.utils.pytest_debugging_stub" in test_command

    def test_throwaway_install_directory_is_removed_after_the_run(self, sql_project_dir):
        _add_unit_test(sql_project_dir)
        component = _make_component(sql_project_dir)
        captured = {}

        def record(command, env):
            if "--target" in command:
                captured["target"] = command[command.index("--target") + 1]
            return 0

        with mock.patch.object(ProjectUnitTests, "_run", side_effect=record):
            component.deploy()

        assert not os.path.exists(captured["target"])


class TestReleaseFailsOnFailingUnitTests:

    def test_release_raises_when_a_unit_test_fails(self):
        deployment = mock.MagicMock()
        deployment._unit_tests.deploy.return_value = [Either(left=Exception("Unit tests failed with exit code 1."))]

        with pytest.raises(Exception, match="Unit tests failed."):
            ProjectDeployment._run_unit_tests(deployment)

    def test_release_continues_when_unit_tests_pass(self):
        deployment = mock.MagicMock()
        deployment._unit_tests.deploy.return_value = [Either(right=True)]

        ProjectDeployment._run_unit_tests(deployment)

    def test_release_continues_when_there_are_no_unit_tests_to_run(self):
        deployment = mock.MagicMock()
        deployment._unit_tests.deploy.return_value = []

        ProjectDeployment._run_unit_tests(deployment)
