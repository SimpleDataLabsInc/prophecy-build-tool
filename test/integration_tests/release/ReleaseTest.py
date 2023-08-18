import unittest

from pydantic_yaml import to_yaml_str, parse_yaml_raw_as

from src.pbt.v2.client.databricks_client import DatabricksClient
from src.pbt.v2.deployment.project import ProjectDeployment
from src.pbt.v2.entities.project import Project
from src.pbt.v2.project_config import StateConfig, ProjectConfig
from src.pbt.v2.project_models import StepMetadata, LogEntry


class TestRelease(unittest.TestCase):

    # def test_release(self):
    #     deployment = Project(ProjectParser("data/new_project", "test", "data/new_project/state_conf.yml"))
    #     print("\n\n")
    #     release = DeployAndRelease(deployment, {'1': 'dapicd781b2014ab066f717e14a290eb13a8'})
    #     for header in release.release_headers():
    #         print(header)
    #     release.deploy()
    #     # print(deployment.release_headers())

    def test_new_log_line(self):
        stepMetadata = StepMetadata("stepId", "headers", "delta", "omega")
        logLine = LogEntry.from_step_metadata(stepMetadata)
        print(logLine.to_json())

    def test_subscribed_project(self):
        project_parser = Project("data/subscribed_project", "1", "0.5")
        project_config = ProjectConfig.from_path("data/subscribed_project/state_conf.yml",
                                                 "data/subscribed_project/system_conf.yml")
        project = ProjectDeployment(project_parser, project_config)
        project.headers()
        project.deploy()

    def test_complete_project(self):
        project_parser = Project("data/complete_project", "1", "0.1.2.3.4")
        project_config = ProjectConfig.from_path("data/complete_project/state_conf.yml",
                                       "data/complete_project/system_conf.yml")
        project = ProjectDeployment(project_parser, project_config)

        project.headers()

        project.deploy()


    def test_sample_project(self):
        project_parser = Project("data/sample_project", "1", "0.7")
        project_config = ProjectConfig("data/sample_project/state_conf.yml", )
        project = ProjectDeployment(project_parser, project_config)

        project.headers()

        project.deploy()

    def test_pydantic_parsing(self):
        x = to_yaml_str(StateConfig(name="conf",
                                    language="python",
                                    description="desc",
                                    version="1.0",
                                    fabrics=[{'id': '1', 'name': 'dev', 'url': 'http://dbc.com', 'isSpark': True,
                                              'isSql': True}],
                                    jobs=[{'name': 'job1', 'type': 'databricks', 'jobId': '1', 'fabricId': '1',
                                           'id': '1'}]))
        print(x)
        k = parse_yaml_raw_as(StateConfig, x)
        print(k)

    def test_upload(self):
        # print(json.dumps('{}'))
        DatabricksClient.from_host_and_token("https://dbc-147abc45-b6c7.cloud.databricks.com",
                                             "dapicd781b2014ab066f717e14a290eb13a8") \
            .upload_content("{'a':'b'}", "dbfs:/FileStore/prophecy/artifacts/dev/execution/1/1/1.json")
