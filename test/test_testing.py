from click.testing import CliRunner
from src.pbt import test, test_v2
import os
import shutil
import uuid
from git import Repo
import glob
import pytest

SAMPLE_REPO = "https://github.com/prophecy-samples/HelloProphecy.git"


@pytest.mark.dependency(depends="test_build_path_default")
class TestTesting:

    @staticmethod
    def _get_tmp_sample_repo(repo_url=SAMPLE_REPO):
        new_path = os.path.join("/tmp/", SAMPLE_REPO.split("/")[-1], f"{uuid.uuid4()}")
        repo = Repo.clone_from(repo_url, new_path)
        return repo, new_path

    def setup_method(self):
        self.repo, self.repo_path = TestTesting._get_tmp_sample_repo()
        self.python_project_path = os.path.join(self.repo_path, "prophecy")
        self.scala_project_path = os.path.join(self.repo_path, "prophecy_scala")

    def teardown_method(self):
        if self.repo_path:
            shutil.rmtree(self.repo_path, ignore_errors=True)

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
    @pytest.mark.parametrize("test_command", [test, test_v2])
    @pytest.mark.parametrize("driver_library_path", ["./", "./fake.jar,fake2.jar", os.getcwd()])
    def test_driver_paths(self, language, test_command, driver_library_path):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        with open('./fake.jar', 'w') as fd:
            fd.write("fake")
        with open('./fake2.jar', 'w') as fd:
            fd.write("fake")

        result = runner.invoke(test_command, ["--path", project_path, "--driver-library-path", driver_library_path])
        assert result.exit_code == 0
        assert "fake.jar" in result.output.replace("\n", "")
        assert "fake2.jar" in result.output.replace("\n", "")

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("test_command", [test, test_v2])
    def test_path_default(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", project_path])
        assert result.exit_code == 0
        items = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                 if os.path.isdir(os.path.join(project_path, "pipelines", i))]
        for i in items:
            if test_command is test:
                assert f"Unit test for pipeline: pipelines/{i} succeeded." in result.output
            elif test_command is test_v2:
                assert f"Pipeline test succeeded : `pipelines/{i}`" in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("test_command", [test_v2])  # not currently supported in v1
    def test_test_v2_relative_path(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", os.path.relpath(project_path, os.getcwd())])
        assert result.exit_code == 0

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("test_command", [test])  # TODO --pipelines option not present in v2
    def test_test_with_pipeline_filter(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        pipelines_to_test = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                             if os.path.isdir(os.path.join(project_path, "pipelines", i))][:2]
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", project_path, "--pipelines", ",".join(pipelines_to_test)])
        assert result.exit_code == 0
        for p in pipelines_to_test:
            if test_command is test:
                assert f"Unit test for pipeline: pipelines/{p} succeeded." in result.output
            elif test_command is test_v2:
                assert f"Pipeline test succeeded : `pipelines/{p}`" in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("test_command", [test])  # TODO --pipelines option not present in test_v2
    def test_test_with_pipeline_filter_one_notfound_pipeline(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        pipeline_to_test = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                            if os.path.isdir(os.path.join(project_path, "pipelines", i))][0]
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", project_path, "--pipelines", f"{pipeline_to_test},notfound"])
        assert result.exit_code == 1
        assert "Filtered pipelines doesn't match with passed filter" in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("test_command", [test])  # TODO --pipelines option not present in test_v2
    def test_test_with_pipeline_filter_all_notfound_pipelines(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", project_path, "--pipelines", f"notfound1,notfound2,notfound3"])
        assert result.exit_code == 1
        assert "Filtered pipelines doesn't match with passed filter" in result.output

    @pytest.mark.parametrize("language", ["python"])  # scala does not currently output coverage reports or junit reports
    @pytest.mark.parametrize("test_command", [test, test_v2])
    def test_coverage_and_test_report_generation(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        pipelines_to_test = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                             if os.path.isdir(os.path.join(project_path, "pipelines", i))]
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", project_path])
        assert result.exit_code == 0
        for p in pipelines_to_test:
            coverage_path = os.path.join(project_path, f"./pipelines/{p}/code/coverage.xml")
            if test_command is test:
                assert f"Unit test for pipeline: pipelines/{p} succeeded." in result.output
            elif test_command is test_v2:
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
