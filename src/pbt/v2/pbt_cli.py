from src.pbt.v2.project.components.project import Project
from src.pbt.v2.project.project_parser import ProjectParser
from src.pbt.v2.state_config import StateConfigAndDBTokens


class PBTCli(object):
    """Command line interface for PBT."""

    def __init__(self, project_path, state_config_path, project_id, release_version):
        self.project_path = project_path
        self.state_config_path = state_config_path
        self.project = Project(ProjectParser(project_path, project_id, release_version),
                               StateConfigAndDBTokens(state_config_path))

    def headers(self):
        """Print headers."""
        self.project.headers()

    def build(self, pipeline_ids):
        """Build pipelines."""
        self.project.build(pipeline_ids)

    def deploy(self, job_ids):
        """Deploy pipelines."""
        self.project.deploy(job_ids)
