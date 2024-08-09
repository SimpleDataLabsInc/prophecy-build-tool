import unittest
from click.testing import CliRunner
from src.pbt import versioning, build_v2
import os
import git
import glob
import shutil


CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
REPO_PATH = os.path.dirname(CURRENT_DIRECTORY)
PROJECT_PATH = CURRENT_DIRECTORY + "/test/resources/HelloWorld"


class VersioningTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize the repo object and set it as a class attribute
        cls.repo = git.Repo(os.getcwd())  # Change this to your repo path

    @classmethod
    def tearDownClass(cls):
        # Reset all changes in the 'test/' directory
        cls.reset_changed_files('test/')
        #cls.remove_build_dirs()

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
    def remove_build_dirs(cls):
        build_dirs = glob.glob(os.path.join(PROJECT_PATH, '**', "build"))
        for dir in build_dirs:
            if os.path.exists(dir):
                print("hi")
                #shutil.rmtree(dir)


    def test_versioning_sync_python():
        runner = CliRunner()
        result = runner.invoke(versioning, ["--path", PROJECT_PATH, '--sync'])
        assert result.exit_code == 0

        result = runner.invoke(build_v2, ["--path", PROJECT_PATH])
        assert result.exit_code == 0
        # Find all .whl files in the current directory
        whl_files = glob.glob(os.path.join(PROJECT_PATH, "**", '*.whl'))
        for file_name in whl_files:
            assert "-1.0-py3-none-any" not in file_name

