import unittest

from src.pbt.v2.project.project_parser import ProjectParser
from src.pbt.release.Build import Build


class TestBuild(unittest.TestCase):
    def test_build(self):
        project = ProjectParser("data/sample_project", "test")
        print(project.pipeline_headers())
        build = Build(project).build_and_upload()
