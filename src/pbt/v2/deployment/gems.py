from ..entities.project import Project
from ..project_config import ProjectConfig


# TODO: Implement GemsDeployment
class GemsDeployment:
    def __init__(self, project: Project, project_config: ProjectConfig):
        self.project = project
        self.project_config = project_config

    def _is_gems_exist(self):
        pass

    def summary(self):
        return []

    def headers(self):
        return []

    def deploy(self):
        pass
