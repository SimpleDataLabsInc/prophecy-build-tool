from src.pbt.v2.constants import SCALA_LANGUAGE
from src.pbt.v2.project.project_parser import ProjectParser
import subprocess


class Build:
    def __init__(self, project: ProjectParser):
        self.project = project

    def build(self):
        for pipeline, content in self.project.pipelines.items():
            files = self.project.get_files(pipeline, "code")
            PackageBuilder(files, self.project.project_language).build_pipeline()




