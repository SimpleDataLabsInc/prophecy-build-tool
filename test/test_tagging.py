import pytest
from click.testing import CliRunner
from src.pbt import tag
import os
from test.isolated_repo_test_case import IsolatedRepoTestCase


class TestTagging(IsolatedRepoTestCase):

    @staticmethod
    def _get_pbt_version(path_to_project):
        with open(os.path.join(path_to_project, "pbt_project.yml"), 'r') as fd:
            for line in fd:
                if line.startswith("version: "):
                    return line.split(":")[1].strip()

    def test_tagging_custom(self):
        project_path = self.python_project_path

        custom_tag = "CUSTOM_TAG_UNITTEST"
        runner = CliRunner()
        result = runner.invoke(tag, ["--path", project_path, "--repo-path", self.repo_path,
                                     "--no-push", "--custom", custom_tag])
        assert result.exit_code == 0
        assert custom_tag in self.repo.tags

    @pytest.mark.parametrize("language", ["python", "scala"])
    def test_tagging_default(self, language):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        pbt_version = TestTagging._get_pbt_version(project_path)

        branch_name = "custom_branch_unittest"
        new_branch = self.repo.create_head(branch_name)
        new_branch.checkout()

        custom_tag = f"{branch_name}/{pbt_version}"
        runner = CliRunner()
        result = runner.invoke(tag, ["--path", project_path, "--repo-path", self.repo_path,
                                     "--no-push"])
        assert result.exit_code == 0
        assert custom_tag in [str(t) for t in self.repo.tags]

    def test_tagging_omit_branchname(self):
        project_path = self.python_project_path
        pbt_version = TestTagging._get_pbt_version(project_path)

        custom_tag = f"{pbt_version}"
        runner = CliRunner()
        result = runner.invoke(tag, ["--path", project_path, "--repo-path", self.repo_path,
                                     "--no-push", "--branch", ""])
        assert result.exit_code == 0
        assert custom_tag in [str(t) for t in self.repo.tags]

    def test_tagging_custom_branchname(self):
        project_path = self.python_project_path
        pbt_version = TestTagging._get_pbt_version(project_path)

        custom_branch_name = "nonexistant_branch"
        custom_tag = f"{custom_branch_name}/{pbt_version}"
        runner = CliRunner()
        result = runner.invoke(tag, ["--path", project_path, "--repo-path", self.repo_path,
                                     "--no-push", "--branch", custom_branch_name])
        assert result.exit_code == 0
        assert custom_tag in [str(t) for t in self.repo.tags]
