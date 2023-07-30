import subprocess
from typing import List

from src.pbt.v2.constants import SCALA_LANGUAGE
from src.pbt.v2.project.project_parser import Project


class ProjectBuilder:
    def __init__(self, project: Project):
        self.project = project
        self.pipelines = project.pipelines()

    # todo make it multi-threaded


def build(self) -> List[str]:
    file_paths = []

    for pipeline, content in self.pipelines.items():
        file_path = None
        try:
            file_path = Nexus(pipeline).download_and_get()
        except Exception as e:
            print("Nexus isn't supported as yet, building locally")
            files = self.project.get_files(pipeline, "code")
            file_path = PackageBuilder(files, self.project.project_language).build_and_get_pipeline()
        finally:
            if file_path is not None:
                file_paths.append(file_path)

    return file_paths


class PackageBuilder:

    def __init__(self, path: str, project_language: str, is_tests_enabled: bool = False):
        self.path = path
        self.is_tests_enabled = is_tests_enabled
        self.project_langauge = project_language

    def build_and_get_pipeline(self):
        if self.project_langauge == SCALA_LANGUAGE:
            self.mvn_build()
        else:
            self.wheel_build()

        return ""

    def mvn_build(self):
        command = "mvn package -DskipTests" if not self.is_tests_enabled else "mvn package"
        self.__build(command)

    def wheel_build(self):
        command = "python3 setup.py bdist_wheel"
        self.__build(command)

    def __build(self, command: str):
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=self.path)

        # Loop over stdout line by line
        while True:
            # Read line from stdout, break if EOF
            output = process.stdout.readline()
            if process.poll() is not None and not output:
                break
            # Decode line and print it
            print(output.decode().strip())

        # Wait for the process to finish and get the exit code
        return_code = process.wait()

        if return_code == 0:
            print("Build was successful.")
        else:
            print("Build failed.")


class Nexus:
    def __init__(self, project: Project):
        self.project = project
        self.pipelines = project.pipelines()

    def download_and_get(self):
        raise Exception("Not implemented yet")
