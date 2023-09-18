from .deployment.project import ProjectDeployment
from .entities.project import Project
from .project_config import ProjectConfig


class PBTCli(object):
    """Command line interface for PBT."""

    def __init__(self, project_path,
                 deployment_state_path,
                 system_config_path,
                 project_id,
                 deployment_state_override_config_path,
                 release_tag,
                 release_version):
        self.project_path = project_path
        self.deployment_state_path = deployment_state_path
        self.project = ProjectDeployment(Project(project_path, project_id, release_tag, release_version),
                                         ProjectConfig.from_path(deployment_state_path, system_config_path,
                                                                 deployment_state_override_config_path))

    def headers(self):
        """Print headers."""
        self.project.headers()

    def build(self, pipeline_ids):
        """Build pipelines."""
        self.project.build(pipeline_ids)

    def deploy(self, job_ids):
        """Deploy pipelines."""
        self.project.deploy(job_ids)
