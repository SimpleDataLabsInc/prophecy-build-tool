import unittest

from pydantic_yaml import to_yaml_str, parse_yaml_raw_as

from src.pbt.v2.project.project_parser import ProjectParser, Project
from src.pbt.v2.client.databricks_client import DatabricksClient
from src.pbt.release.DeployAndRelease import DeployAndRelease
from src.pbt.v2.state_config import StateConfig


class TestRelease(unittest.TestCase):

    def test_release(self):
        project = Project(ProjectParser("data/new_project", "test", "data/new_project/state_conf.yml"))
        print("\n\n")
        release = DeployAndRelease(project, {'1': 'dapicd781b2014ab066f717e14a290eb13a8'})
        for header in release.release_headers():
            print(header)
        release.deploy()
        # print(project.release_headers())

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
