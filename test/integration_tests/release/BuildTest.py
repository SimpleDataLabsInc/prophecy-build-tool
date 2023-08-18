import unittest

from src.pbt.v2.entities.project import Project
from src.pbt.release.Build import Build


class TestBuild(unittest.TestCase):
    def test_build(self):
        project = Project("data/sample_project", "test")
        print(project.pipeline_headers())
        build = Build(project).build_and_upload()
