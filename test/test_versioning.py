from click.testing import CliRunner
from src.pbt import versioning, build_v2
import os
import glob
import pytest
from test.isolated_repo_test_case import IsolatedRepoTestCase


class TestVersioning(IsolatedRepoTestCase):
    @staticmethod
    def _get_pbt_version(path_to_project):
        with open(os.path.join(path_to_project, "pbt_project.yml"), "r") as fd:
            for line in fd:
                if line.startswith("version: "):
                    return line.split(":")[1].strip()

    @pytest.mark.parametrize("bump_type, version_result", [("major", "1.0.0"), ("minor", "0.1.0"), ("patch", "0.0.2")])
    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_versioning_bump(self, bump_type, version_result, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        pbt_version = TestVersioning._get_pbt_version(project_path)

        runner = CliRunner()
        result = runner.invoke(versioning, ["--path", project_path, "--bump", bump_type])
        assert result.exit_code == 0

        new_pbt_version = TestVersioning._get_pbt_version(project_path)
        assert new_pbt_version != pbt_version
        assert new_pbt_version == version_result

    @pytest.mark.parametrize(
        "bump_type, version_result",
        [
            ("build", "0.0.1+build.1"),
            ("prerelease", "0.0.1-rc.1"),
        ],
    )
    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_versioning_bump_build_pre_python(self, bump_type, version_result, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        pbt_version = TestVersioning._get_pbt_version(project_path)

        runner = CliRunner()
        # note using --force otherwise build and release candidates would not work.
        result = runner.invoke(versioning, ["--path", project_path, "--bump", bump_type, "--force"])
        assert result.exit_code == 0

        new_pbt_version = TestVersioning._get_pbt_version(project_path)
        assert new_pbt_version != pbt_version
        assert new_pbt_version == version_result

    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_versioning_sync(self, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        pbt_version = TestVersioning._get_pbt_version(project_path)

        runner = CliRunner()
        result = runner.invoke(versioning, ["--path", project_path, "--sync"])
        assert result.exit_code == 0

        result = runner.invoke(build_v2, ["--path", project_path])
        assert result.exit_code == 0

        # future TODO; building the artifacts is kind of lazy and causes dependency on buildv2 command.
        #              later on we should use a different mechanism to verify versions were correctly changed.

        # Find any .whl files in the current directory
        whl_files = glob.glob(os.path.join(project_path, "**", "*.whl"), recursive=True)
        for file_name in whl_files:
            assert f"-{pbt_version}-py3-none-any" in file_name

        # Find any .jar files in the current directory
        jar_files = glob.glob(os.path.join(project_path, "**", "*.jar"), recursive=True)
        for file_name in jar_files:
            assert f"{pbt_version}" in file_name

        # make sure that we at least found *some* build artifacts:
        assert len(list(whl_files) + list(jar_files)) > 0

    def test_versioning_version_set_below_no_force(self):
        project_path = self.python_project_path
        runner = CliRunner()
        result = runner.invoke(versioning, ["--path", project_path, "--set", "0.0.0"])
        assert result.exit_code == 1

    def test_versioning_bump_and_version_set_prerelease_maven(self):
        project_path = self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(versioning, ["--path", project_path, "--bump", "major"])
        assert result.exit_code == 0
        result = runner.invoke(versioning, ["--path", project_path, "--set-prerelease", "-SNAPSHOT", "--force"])
        assert result.exit_code == 0

        new_pbt_version = TestVersioning._get_pbt_version(project_path)
        assert new_pbt_version == "1.0.0-SNAPSHOT"

    def test_versioning_set_prerelease_and_bump_python(self):
        project_path = self.python_project_path
        runner = CliRunner()
        result = runner.invoke(versioning, ["--path", project_path, "--set-prerelease", "-rc.4", "--force"])
        assert result.exit_code == 0
        result = runner.invoke(versioning, ["--path", project_path, "--bump", "prerelease"])
        assert result.exit_code == 0
        new_pbt_version = TestVersioning._get_pbt_version(project_path)
        assert new_pbt_version == "0.0.1-rc.5"

    def test_versioning_version_set_below_force(self):
        project_path = self.python_project_path
        runner = CliRunner()
        result = runner.invoke(versioning, ["--path", project_path, "--set", "invalid-0.0.0-thing", "--force"])
        assert result.exit_code == 0

        new_pbt_version = TestVersioning._get_pbt_version(project_path)
        assert new_pbt_version == "invalid-0.0.0-thing"

    def test_versioning_version_set(self):
        project_path = self.python_project_path
        runner = CliRunner()
        result = runner.invoke(versioning, ["--path", project_path, "--set", "999999.0.0"])
        assert result.exit_code == 0

        new_pbt_version = TestVersioning._get_pbt_version(project_path)
        assert new_pbt_version == "999999.0.0"
