import unittest
from click.testing import CliRunner
from src.pbt import versioning, build_v2
import os
import git
import glob
import shutil
from parameterized import parameterized


CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
REPO_PATH = os.path.dirname(CURRENT_DIRECTORY)
PROJECT_PATH = CURRENT_DIRECTORY + "/resources/HelloWorld"
PROJECTS_TO_TEST = [("HelloWorld"), ("BaseDirectory"), ("ProjectCreatedOn160523")]


class VersioningTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize the repo object and set it as a class attribute
        cls.repo = git.Repo(os.getcwd())  # Change this to your repo path

    @classmethod
    def tearDownClass(cls):
        # Reset all changes in the 'test/' directory
        cls.reset_changed_files('test/resources/')
        cls.remove_tmp_dirs()

    @classmethod
    def reset_changed_files(cls, directory):
        # Get a list of all changed files in the specified directory
        changed_files = [item.a_path for item in cls.repo.index.diff(None) if item.a_path.startswith(directory)]

        # Check if there are any changed files
        if changed_files:
            # Restore the changes to the working directory
            cls.repo.git.checkout('--', *changed_files)
            print(f"Reset changed files: {changed_files}")
        else:
            print("No files were changed.")

    @classmethod
    def remove_tmp_dirs(cls):
        tmp_dirs = glob.glob(os.path.join(PROJECT_PATH, '**', "dist"), recursive=True) + \
                        glob.glob(os.path.join(PROJECT_PATH, '**', "build"), recursive=True)
        for d in tmp_dirs:
            if os.path.exists(d):
                shutil.rmtree(d)

    @staticmethod
    def _get_pbt_version(path_to_project):
        with open(os.path.join(path_to_project, "pbt_project.yml"), 'r') as fd:
            for line in fd:
                if line.startswith("version: "):
                    return line.split(":")[1].strip()

    @parameterized.expand([("HelloWorld"), ("BaseDirectory"), ("ProjectCreatedOn160523")])
    def test_versioning_sync_python(self, project_name):
        project_path = CURRENT_DIRECTORY + f"/resources/{project_name}"
        pbt_version = VersioningTestCase._get_pbt_version(project_path)

        runner = CliRunner()
        result = runner.invoke(versioning, ["--path", project_path, '--sync'])
        assert result.exit_code == 0

        result = runner.invoke(build_v2, ["--path", project_path])
        assert result.exit_code == 0
        # Find all .whl files in the current directory
        whl_files = glob.glob(os.path.join(project_path, "**", '*.whl'))
        # for file_name in whl_files:
        #     assert f"-{pbt_version}-py3-none-any" in file_name

