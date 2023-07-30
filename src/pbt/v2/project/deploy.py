from src.pbt.v2.project.project_builder import ProjectBuilder
from src.pbt.v2.project.project_parser import Project
from src.pbt.v2.project_models import StepMetadata

'''
# Steps to deploy
# show headers
# Upload summary optional ? 
# upload pipeline configuration.
# upload scripts code
# upload dbt profiles
# upload dbt secrets
# upload airflow secrets
# upload databricks jobs 
# upload airflow jobs

## Auto deploy --> 
    Build everything Deploy everything
    # Tests will be off in this case by default. 
##Auto deploy is off and no job selection is there. 
    Fabric override is possible 
    Fabric Selection is possible 
    Build should not be repeated 
##AutoDeploy is off and selective job deployment is off. 
    Fabric override is possible
    job selection is possible. 
'''
class Deployer():

    def __init__(self, project: Project, builder: ProjectBuilder):
        self.project = project
        self.builder = builder

    def deploy(self):
        pass

