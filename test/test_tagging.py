import unittest
from click.testing import CliRunner
from src.pbt import tag
import os
import shutil
import uuid
from git import Repo
from parameterized import parameterized

CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
REPO_PATH = os.path.dirname(CURRENT_DIRECTORY)
RESOURCES_PATH = os.path.join(CURRENT_DIRECTORY, "resources")
SAMPLE_REPO = "https://github.com/prophecy-samples/HelloProphecy.git"


class TaggingTestCase(unittest.TestCase):
    @staticmethod
    def _get_tmp_sample_repo(repo_url=SAMPLE_REPO):
        new_path = os.path.join("/tmp/", SAMPLE_REPO.split("/")[-1], f"{uuid.uuid4()}")
        repo = Repo.clone_from(repo_url, new_path)
        return repo, new_path

    def setUp(self):
        self.repo, self.repo_path = TaggingTestCase._get_tmp_sample_repo()
        self.python_project_path = os.path.join(self.repo_path, "prophecy")
        self.scala_project_path = os.path.join(self.repo_path, "prophecy_scala")

    def tearDown(self):
        if self.repo_path:
            shutil.rmtree(self.repo_path)

    @staticmethod
    def _get_pbt_version(path_to_project):
        with open(os.path.join(path_to_project, "pbt_project.yml"), "r") as fd:
            for line in fd:
                if line.startswith("version: "):
                    return line.split(":")[1].strip()

    def test_tagging_custom(self):
        project_path = self.python_project_path

        custom_tag = "CUSTOM_TAG_UNITTEST"
        runner = CliRunner()
        result = runner.invoke(
            tag, ["--path", project_path, "--repo-path", self.repo_path, "--no-push", "--custom", custom_tag]
        )
        assert result.exit_code == 0
        assert custom_tag in self.repo.tags

    @parameterized.expand(["python", "scala"])
    def test_tagging_default(self, language):
        project_path = self.python_project_path if language == "python" else self.scala_project_path
        pbt_version = TaggingTestCase._get_pbt_version(project_path)

        branch_name = "custom_branch_unittest"
        new_branch = self.repo.create_head(branch_name)
        new_branch.checkout()

        custom_tag = f"{branch_name}/{pbt_version}"
        runner = CliRunner()
        result = runner.invoke(tag, ["--path", project_path, "--repo-path", self.repo_path, "--no-push"])
        assert result.exit_code == 0
        assert custom_tag in [str(t) for t in self.repo.tags]

    def test_tagging_omit_branchname(self):
        project_path = self.python_project_path
        pbt_version = TaggingTestCase._get_pbt_version(project_path)

        custom_tag = f"{pbt_version}"
        runner = CliRunner()
        result = runner.invoke(
            tag, ["--path", project_path, "--repo-path", self.repo_path, "--no-push", "--branch", ""]
        )
        assert result.exit_code == 0
        assert custom_tag in [str(t) for t in self.repo.tags]

    def test_tagging_custom_branchname(self):
        project_path = self.python_project_path
        pbt_version = TaggingTestCase._get_pbt_version(project_path)

        custom_branch_name = "nonexistant_branch"
        custom_tag = f"{custom_branch_name}/{pbt_version}"
        runner = CliRunner()
        result = runner.invoke(
            tag, ["--path", project_path, "--repo-path", self.repo_path, "--no-push", "--branch", custom_branch_name]
        )
        assert result.exit_code == 0
        assert custom_tag in [str(t) for t in self.repo.tags]
