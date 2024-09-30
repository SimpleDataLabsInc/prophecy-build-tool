from click.testing import CliRunner
from src.pbt import test, test_v2
import os
import glob
import pytest
from test.isolated_repo_test_case import IsolatedRepoTestCase


@pytest.mark.dependency(depends="test_build_path_default")
@pytest.mark.xdist_group(name="serial_tests")  # these tests must be run serially or github action worker OOM
class TestTesting(IsolatedRepoTestCase):

    @staticmethod
    def _check_for_n_artifacts(project_path, language, n=5):
        artifacts = []
        # Find any .whl files in the current directory
        if language == 'python':
            # coverage and unit tests
            artifacts += glob.glob(os.path.join(project_path, "**", 'coverage.xml'), recursive=True)
            artifacts += glob.glob(os.path.join(project_path, "**", 'report.xml'), recursive=True)

            # Find any .jar files in the current directory
        if language == 'scala':
            # artifacts = glob.glob(os.path.join(project_path, "**", 'report.xml'), recursive=True)
            pass

            # make sure we found correct build artifacts:
        assert len(list(artifacts)) == n

    @pytest.mark.parametrize("language", ["python"])
    @pytest.mark.parametrize("command", [test, test_v2])
    @pytest.mark.parametrize("driver_library_path", ["./", "./fake.jar,fake2.jar", os.getcwd()])
    def test_driver_paths(self, language, command, driver_library_path):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        with open('./fake.jar', 'w') as fd:
            fd.write("fake")
        with open('./fake2.jar', 'w') as fd:
            fd.write("fake")

        result = runner.invoke(command, ["--path", project_path, "--driver-library-path", driver_library_path])
        assert result.exit_code == 0
        assert "fake.jar" in result.output.replace("\n", "")
        assert "fake2.jar" in result.output.replace("\n", "")

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [test, test_v2])
    def test_path_default(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(command, ["--path", project_path])
        assert result.exit_code == 0
        items = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                 if os.path.isdir(os.path.join(project_path, "pipelines", i))]
        for i in items:
            if command is test:
                assert f"Unit test for pipeline: pipelines/{i} succeeded." in result.output
            elif command is test_v2:
                assert f"Pipeline test succeeded : `pipelines/{i}`" in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [test_v2])  # not currently supported in v1
    def test_test_v2_relative_path(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(command, ["--path", os.path.relpath(project_path, os.getcwd())])
        print(result.output)
        assert result.exit_code == 0

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [test])  # TODO --pipelines option not present in v2
    def test_test_with_pipeline_filter(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        pipelines_to_test = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                             if os.path.isdir(os.path.join(project_path, "pipelines", i))][:2]
        runner = CliRunner()
        result = runner.invoke(command, ["--path", project_path, "--pipelines", ",".join(pipelines_to_test)])
        assert result.exit_code == 0
        for p in pipelines_to_test:
            if command is test:
                assert f"Unit test for pipeline: pipelines/{p} succeeded." in result.output
            elif command is test_v2:
                assert f"Pipeline test succeeded : `pipelines/{p}`" in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [test])  # TODO --pipelines option not present in test_v2
    def test_test_with_pipeline_filter_one_notfound_pipeline(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        pipeline_to_test = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                            if os.path.isdir(os.path.join(project_path, "pipelines", i))][0]
        runner = CliRunner()
        result = runner.invoke(command, ["--path", project_path, "--pipelines", f"{pipeline_to_test},notfound"])
        assert result.exit_code == 1
        assert "Filtered pipelines doesn't match with passed filter" in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [test])  # TODO --pipelines option not present in test_v2
    def test_test_with_pipeline_filter_all_notfound_pipelines(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(command, ["--path", project_path, "--pipelines", "notfound1,notfound2,notfound3"])
        assert result.exit_code == 1
        assert "Filtered pipelines doesn't match with passed filter" in result.output

    # scala does not currently output coverage reports or junit reports.
    # TODO once it does; consider adding this to the default test as a subtest (no need to test building again)
    @pytest.mark.parametrize("language", ["python"])
    @pytest.mark.parametrize("command", [test, test_v2])
    def test_coverage_and_test_report_generation(self, language, command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(command, ["--path", project_path])
        print(result.output)
        assert result.exit_code == 0

        pipelines_to_test = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                             if os.path.isdir(os.path.join(project_path, "pipelines", i))]
        for p in pipelines_to_test:
            coverage_path = os.path.join(project_path, f"./pipelines/{p}/code/coverage.xml")
            if command is test:
                assert f"Unit test for pipeline: pipelines/{p} succeeded." in result.output
            elif command is test_v2:
                assert f"Pipeline test succeeded : `pipelines/{p}`" in result.output
            assert "Coverage XML written to file coverage.xml" in result.output
            assert (os.path.exists(coverage_path))
            assert (os.path.exists(os.path.join(project_path, f"./pipelines/{p}/code/report.xml")))

            with open(coverage_path, 'r') as fd:
                content = fd.read()
                # check to make sure that .coveragerc got picked up and made absolute paths:
                assert ("<source>/" in content)
                # check that setup.py is ignored
                assert ("<class name=\"setup.py\"" not in content)
                # make sure we are not doing coverage for test directory
                assert ("<package name=\"test\"" not in content)
                # verify that some coverage was written
                assert ("<package name=" in content)

