import unittest

from pydantic_yaml import to_yaml_str, parse_yaml_raw_as

from src.pbt.v2.client.databricks_client import DatabricksClient
from src.pbt.v2.project.components.project import Project
from src.pbt.v2.project.project_parser import ProjectParser
from src.pbt.v2.state_config import StateConfig, StateConfigAndDBTokens


class TestRelease(unittest.TestCase):

    # def test_release(self):
    #     project = Project(ProjectParser("data/new_project", "test", "data/new_project/state_conf.yml"))
    #     print("\n\n")
    #     release = DeployAndRelease(project, {'1': 'dapicd781b2014ab066f717e14a290eb13a8'})
    #     for header in release.release_headers():
    #         print(header)
    #     release.deploy()
    #     # print(project.release_headers())

    def test_project(self):
        project_parser = ProjectParser("data/new_project", "test", "data/new_project/state_conf.yml")
        state_config_and_db_tokens = StateConfigAndDBTokens("data/new_project/state_conf.yml",
                                                            {"1": "dapicd781b2014ab066f717e14a290eb13a8"})
        project = Project(project_parser, state_config_and_db_tokens)

        project.headers()

        project.deploy()

    def test_subscribed_project(self):
        project_parser = ProjectParser("data/subscribed_project", "test", "1", "0.5")
        state_config_and_db_tokens = StateConfigAndDBTokens("data/subscribed_project/state_conf.yml",
                                                            {"1": "dapicd781b2014ab066f717e14a290eb13a8",
                                                             "413": "dapicd781b2014ab066f717e14a290eb13a8",
                                                             "412": "dapicd781b2014ab066f717e14a290eb13a8"})
        project = Project(project_parser, state_config_and_db_tokens)
        project.headers()
        project.deploy()

    def test_complete_project(self):
        project_parser = ProjectParser("data/complete_project", "test", "1")
        state_config_and_db_tokens = StateConfigAndDBTokens("data/complete_project/state_conf.yml",
                                                            {"1": "dapicd781b2014ab066f717e14a290eb13a8",
                                                            "2730": "dapicd781b2014ab066f717e14a290eb13a8"})
        project = Project(project_parser, state_config_and_db_tokens)

        project.headers()

        project.deploy()

    def test_sample_project(self):
        project_parser = ProjectParser("data/sample_project", "test", "1")
        state_config_and_db_tokens = StateConfigAndDBTokens("data/sample_project/state_conf.yml",
                                                            {"1": "dapicd781b2014ab066f717e14a290eb13a8",
                                                             "413": "dapicd781b2014ab066f717e14a290eb13a8",
                                                             "412": "dapicd781b2014ab066f717e14a290eb13a8"})
        project = Project(project_parser, state_config_and_db_tokens)

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
