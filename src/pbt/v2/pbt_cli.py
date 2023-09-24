from typing import Optional

from .deployment.project import ProjectDeployment
from .entities.project import Project
from .project_config import ProjectConfig


class PBTCli(object):
    """Command line interface for PBT."""

    def __init__(self, project: Project, project_config: ProjectConfig):
        self.project = ProjectDeployment(project, project_config)

    def headers(self):
        """Print headers."""
        self.project.headers()

    def build(self):
        """Build pipelines."""
        self.project.build()

    def deploy(self, job_ids):
        """Deploy pipelines."""
        self.project.deploy(job_ids)

    @classmethod
    def from_conf_folder(cls, project_path: str, project_id: str, conf_folder: str, release_tag: Optional[str],
                         release_version: str):
        """Create PBTCli from conf folder."""
        project = Project(project_path, project_id, release_tag, release_version)
        project_config = ProjectConfig.from_conf_folder(conf_folder)
        return cls(project, project_config)
