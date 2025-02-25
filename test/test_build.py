from src.pbt import build, build_v2
import pytest
import glob
import os
from test.isolated_repo_test_case import IsolatedRepoTestCase
from test import get_command_name


class TestBuild(IsolatedRepoTestCase):
    @staticmethod
    def _check_for_artifacts(project_path, language, n=5):
        artifacts = []
        # Find any .whl files in the current directory
        if language == "python":
            artifacts += glob.glob(os.path.join(project_path, "**", "*.whl"), recursive=True)

            # Find any .jar files in the current directory
        if language == "scala":
            artifacts += glob.glob(os.path.join(project_path, "**", "*.jar"), recursive=True)

            # make sure we found correct build artifacts:
        assert len(list(artifacts)) == n
        return artifacts

    def test_build_v2_binary_check(self, cli_runner, monkeypatch):
        monkeypatch.setenv("PATH", "")
        result = cli_runner.invoke(build_v2, ["--path", self.python_project_path])
        print(result.output)
        assert result.exit_code == 1
        assert "ERROR: no `python3` or `python` found" in result.output

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [build, build_v2], ids=get_command_name)
    def test_build_path_default(self, cli_runner, language, command):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        result = cli_runner.invoke(command, ["--path", project_path])
        print(result.stdout)
        assert result.exit_code == 0
        assert "Found 5 pipelines" in result.output
        TestBuild._check_for_artifacts(project_path, language, n=5)

    @pytest.mark.parametrize(
        "language, build_file_name",
        [
            ("python", "setup.py"),
            ("scala", "pom.xml"),
        ],
    )
    def test_build_v2_path_default_build_errors(self, cli_runner, language, build_file_name):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        a_file_to_mangle = list(
            glob.glob(os.path.join(project_path, "pipelines", "**", build_file_name), recursive=True)
        )[0]
        print("file to mangle: ", a_file_to_mangle)
        with open(a_file_to_mangle, "w") as fd:
            fd.write("oops")
            fd.close()
        result = cli_runner.invoke(build_v2, ["--path", project_path])
        print(result.stdout)
        assert result.exit_code == 1
        TestBuild._check_for_artifacts(project_path, language, n=4)

    @pytest.mark.parametrize(
        "language, build_file_name",
        [
            ("python", "setup.py"),
            ("scala", "pom.xml"),
        ],
    )
    def test_build_v2_path_default_build_errors_ignore_errors(self, cli_runner, language, build_file_name):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        a_file_to_mangle = list(
            glob.glob(os.path.join(project_path, "pipelines", "**", build_file_name), recursive=True)
        )[0]
        print("file to mangle: ", a_file_to_mangle)
        with open(a_file_to_mangle, "w") as fd:
            fd.write("oops")
            fd.close()
        result = cli_runner.invoke(build_v2, ["--path", project_path, "--ignore-build-errors"])
        print(result.stdout)
        assert result.exit_code == 0
        TestBuild._check_for_artifacts(project_path, language, n=4)

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [build, build_v2], ids=get_command_name)
    def test_build_path_pipeline_filter(self, cli_runner, language, command):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        result = cli_runner.invoke(command, ["--path", project_path, "--pipelines", "raw_bronze,gold_sales"])
        print(result.stdout)
        assert result.exit_code == 0
        TestBuild._check_for_artifacts(project_path, language, n=2)

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [build, build_v2], ids=get_command_name)
    def test_build_path_pipeline_with_invalid_filter(self, cli_runner, language, command):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        result = cli_runner.invoke(
            command,
            [
                "--path",
                project_path,
                "--pipelines",
                "raw_bronze,INVALID_PIPELINE_NAME",
            ],
        )
        print(result.stdout)
        assert result.exit_code == 0
        TestBuild._check_for_artifacts(project_path, language, n=1)

    @pytest.mark.parametrize("language", ["python", "scala"])
    @pytest.mark.parametrize("command", [build], ids=get_command_name)  # TODO currently different behavior for build-v2
    def test_build_path_pipeline_invalid_filter_only(self, cli_runner, language, command):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        result = cli_runner.invoke(command, ["--path", project_path, "--pipelines", "INVALID_PIPELINE_NAME"])
        print(result.stdout)
        assert result.exit_code == 1
        TestBuild._check_for_artifacts(project_path, language, n=0)

    def test_build_v2_add_pom_python(self, cli_runner):
        import zipfile

        project_path = self.python_project_path
        result = cli_runner.invoke(build_v2, ["--path", project_path, "--add-pom-python"])
        print(result.stdout)
        assert result.exit_code == 0
        artifacts = TestBuild._check_for_artifacts(project_path, "python", n=5)

        one_artifact = artifacts[0]
        package_name = os.path.basename(one_artifact).split("-")[0]
        with zipfile.ZipFile(one_artifact, "r") as zip_ref:
            print("namelist")
            print(zip_ref.namelist())

            for file in ["MAVEN_COORDINATES", "pom.xml"]:
                assert file in ", ".join(zip_ref.namelist())
                output_path = zip_ref.extract(f"{package_name}-1.0.data/data/{file}", "/tmp/")
                with open(output_path, "r") as fd:
                    # kind of lazy; just check if plibs is in the file:
                    assert "prophecy-libs" in fd.read()
