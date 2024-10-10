import os
import pytest
from git import Repo
import shutil
import uuid
from abc import ABC

SAMPLE_REPO = "https://github.com/prophecy-samples/HelloProphecy.git"


class IsolatedRepoTestCase(ABC):
    @staticmethod
    def _get_tmp_sample_repo(repo_url=SAMPLE_REPO):
        new_path = os.path.join("/tmp/", SAMPLE_REPO.split("/")[-1], f"{uuid.uuid4()}")
        repo = Repo.clone_from(repo_url, new_path)
        repo.git.fetch(tags=True)
        repo.git.checkout("pbt-reference-do-not-delete")
        return repo, new_path

    @pytest.fixture(scope="function", autouse=True)
    def setup_and_teardown(self):
        self.repo, self.repo_path = IsolatedRepoTestCase._get_tmp_sample_repo()
        self.python_project_path = os.path.join(self.repo_path, "prophecy")
        self.scala_project_path = os.path.join(self.repo_path, "prophecy_scala")

        yield self.repo, self.repo_path, self.python_project_path, self.scala_project_path

        if self.repo_path:
            shutil.rmtree(self.repo_path, ignore_errors=True)
