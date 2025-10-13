"""
Orchestration commands for PBT.
Handles generation of job JSONs, building wheels, and deploying to Databricks.
"""

import os
import sys
import json
import subprocess
import shutil
from typing import Optional, List

from .client.databricks import DatabricksClient
from .utils.orchestrator.orch_utils import (
    generate_job_json_template,
    generate_setup_py_content,
    get_resolved_pipeline_info,
    check_schedule_enabled,
    get_pipelines_from_project,
    sanitize_pipeline_name,
    get_project_version,
)


class OrchestrationCommands:
    """Main class for handling orchestration commands."""

    def __init__(self, project_path: str):
        """
        Initialize OrchestrationCommands.

        Args:
            project_path: Path to the project directory containing pbt_project.yml
        """
        self.project_path = os.path.abspath(project_path)
        self.pbt_project_file = os.path.join(self.project_path, "pbt_project.yml")

        # Verify project path
        if not os.path.exists(self.pbt_project_file):
            raise FileNotFoundError(
                f"pbt_project.yml not found in {self.project_path}. "
                "Please ensure you're pointing to a valid Prophecy project."
            )

        # Extract project name from the path (last folder name)
        self.project_name = os.path.basename(self.project_path)

        # Set up paths
        self.jobs_dir = os.path.join(self.project_path, "jobs")
        self.pipelines_dir = os.path.join(self.project_path, "pipelines")
        self.resolved_pipelines_dir = os.path.join(self.project_path, ".prophecy", "ide", "resolved_pipelines")

        # Wheel output directory (parent of project folder)
        self.parent_dir = os.path.dirname(self.project_path)
        self.wheel_output_dir = os.path.join(self.parent_dir, f"{self.project_name}_whl")

    def generate_p4b_job(self):
        """
        Generate Databricks job JSON files for all pipelines in the project.

        This command:
        1. Creates a jobs/ directory if it doesn't exist
        2. Scans the pipelines/ directory for all available pipelines
        3. For each pipeline, generates a {pipeline-name}-databricks-job.json file
        4. Checks for schedule information from resolved pipelines if available
        """
        print("\nStarting job JSON generation...")

        # Create jobs directory if it doesn't exist
        if not os.path.exists(self.jobs_dir):
            os.makedirs(self.jobs_dir)
            print(f"Created jobs directory: {self.jobs_dir}")
        else:
            print(f"Jobs directory already exists: {self.jobs_dir}")

        # Get list of all pipelines
        pipelines = get_pipelines_from_project(self.project_path)

        if not pipelines:
            print(f"ERROR: No pipelines found in {self.pipelines_dir}")
            print("Make sure your project has pipelines in the pipelines/ directory.")
            sys.exit(1)

        print(f"\nFound {len(pipelines)} pipeline(s):")
        for pipeline in pipelines:
            print(f"  - {pipeline}")

        # Generate job JSON for each pipeline
        print("\nGenerating job JSON files...")
        generated_count = 0

        for pipeline_name in pipelines:
            try:
                # Check for resolved pipeline info
                resolved_info = get_resolved_pipeline_info(self.project_path, pipeline_name)
                schedule_enabled, cron_expression, timezone = check_schedule_enabled(resolved_info)

                # Generate job JSON
                job_json = generate_job_json_template(
                    pipeline_name=pipeline_name,
                    project_name=self.project_name,
                    schedule_enabled=schedule_enabled,
                    cron_expression=cron_expression,
                    timezone=timezone or "UTC",
                )

                # Write to file
                job_filename = f"{pipeline_name}-databricks-job.json"
                job_filepath = os.path.join(self.jobs_dir, job_filename)

                with open(job_filepath, "w") as f:
                    json.dump(job_json, f, indent=2)

                schedule_status = "enabled" if schedule_enabled else "disabled"
                print(f"  Generated {job_filename} (schedule: {schedule_status})")
                generated_count += 1

            except Exception as e:
                print(f"  ERROR: Failed to generate job JSON for {pipeline_name}: {e}")

        print(f"\nSuccessfully generated {generated_count}/{len(pipelines)} job JSON file(s)")
        print(f"Job JSON files are located in: {self.jobs_dir}")

    def prepare_orch_project(self):
        """
        Build Python wheels for orchestration.

        This command:
        1. Verifies that job JSON files exist for all pipelines
        2. For each pipeline, creates a wheel package using a custom setup.py
        3. Stores wheels in parent_folder/{project-name}_whl/
        """
        print("\nStarting wheel build process...")

        # Verify jobs directory exists
        if not os.path.exists(self.jobs_dir):
            print(f"ERROR: Jobs directory not found: {self.jobs_dir}")
            print("Please run 'pbt generate-p4b-job' first to generate job definitions.")
            sys.exit(1)

        # Get list of all pipelines
        pipelines = get_pipelines_from_project(self.project_path)

        if not pipelines:
            print(f"ERROR: No pipelines found in {self.pipelines_dir}")
            sys.exit(1)

        # Build wheels for each pipeline
        print("\n preparing package files...")
        prep_count = 0
        failed_pipelines = []
        build_directory = os.path.join(self.project_path, "package_builds")

        # Delete and recreate the build directory if it exists
        if os.path.exists(build_directory):
            shutil.rmtree(build_directory)
        os.makedirs(build_directory, exist_ok=True)

        for pipeline_name in pipelines:
            print(f"\n  Preparing package files for pipeline: {pipeline_name}")

            try:
                # Create a temporary directory for building
                temp_dir = os.path.join(build_directory, pipeline_name)
                os.makedirs(temp_dir, exist_ok=True)

                # Copy only specific files and directories
                allowed_items = {
                    "tests",
                    "snapshots",
                    "seeds",
                    "prophecy-sources",
                    "pipelines",
                    "pbt_project.yml",
                    "packages.yml",
                    "models",
                    "macros",
                    "jobs",
                    "dbt_project.yml",
                    "dbt_packages",
                    "apps",
                    ".prophecy",
                }

                for item in allowed_items:
                    source_path = os.path.join(self.project_path, item)
                    dest_path = os.path.join(temp_dir, item)

                    if os.path.exists(source_path):
                        if os.path.isdir(source_path):
                            shutil.copytree(source_path, dest_path)
                        else:
                            shutil.copy(source_path, dest_path)

                # Create data package structure
                data_package_dir = os.path.join(temp_dir, "data")
                project_data_dir = os.path.join(data_package_dir, self.project_name)
                os.makedirs(project_data_dir, exist_ok=True)

                # Move all copied files into data/{project_name}/
                for item in os.listdir(temp_dir):
                    if item == "data":
                        continue
                    source = os.path.join(temp_dir, item)
                    dest = os.path.join(project_data_dir, item)
                    shutil.move(source, dest)

                # Create __init__.py files
                with open(os.path.join(data_package_dir, "__init__.py"), "w") as f:
                    f.write("")

                # ORCHESTRATOR_PATH will be retrieved from Databricks secrets at runtime
                # No need to bake it into the wheel

                # Create the main package
                package_name = f"{self.project_name}_{pipeline_name}".replace("-", "_").replace(" ", "_")
                package_dir = os.path.join(temp_dir, package_name)
                os.makedirs(package_dir, exist_ok=True)

                # Create __init__.py for the package that exports main
                init_content = '''"""Prophecy Orchestration Package."""
from .__main__ import main

__all__ = ['main']
'''
                with open(os.path.join(package_dir, "__init__.py"), "w") as f:
                    f.write(init_content)

                # Create __main__.py as the entry point module inside the package
                # copy the template file
                template_path = os.path.join(os.path.dirname(__file__), "utils", "orchestrator", "main_template.py")
                main_module_path = os.path.join(package_dir, "__main__.py")
                shutil.copy(template_path, main_module_path)

                # Generate setup.py content
                setup_py_content = generate_setup_py_content(
                    project_name=self.project_name,
                    pipeline_name=pipeline_name,
                    project_path=temp_dir,  # Use temp_dir as the base
                    version=get_project_version(self.project_path),
                )

                # Write setup.py to temp directory
                setup_py_path = os.path.join(temp_dir, "setup.py")
                with open(setup_py_path, "w") as f:
                    f.write(setup_py_content)

                prep_count += 1

            except Exception as e:
                print(f"    ERROR: Failed to prepare package for {pipeline_name}: {e}")
                failed_pipelines.append(pipeline_name)

        if failed_pipelines:
            print(f"\nFailed pipelines:")
            for pipeline in failed_pipelines:
                print(f"  - {pipeline}")
            exit(1)

        # Summary
        print(f"\nSuccessfully prepared {prep_count}/{len(pipelines)} pipeline package(s)")

        print(f"\n package files located in: {self.wheel_output_dir}")

    def build_orch(self):
        """
        Build Python wheels for orchestration.

        This command:
        1. Verifies that job JSON files exist for all pipelines
        2. For each pipeline, creates a wheel package using a custom setup.py
        3. Stores wheels in parent_folder/{project-name}_whl/
        """
        print("\nStarting wheel build process...")

        # Verify jobs directory exists
        if not os.path.exists(self.jobs_dir):
            print(f"ERROR: Jobs directory not found: {self.jobs_dir}")
            print("Please run 'pbt generate-p4b-job' first to generate job definitions.")
            sys.exit(1)

        # Get list of all pipelines
        pipelines = get_pipelines_from_project(self.project_path)

        if not pipelines:
            print(f"ERROR: No pipelines found in {self.pipelines_dir}")
            sys.exit(1)

        build_directory = os.path.join(self.project_path, "package_builds")

        # Verify all job JSONs exist
        print("\nVerifying job JSON files...")
        missing_jobs = []
        missing_prep = []

        for pipeline_name in pipelines:
            job_filename = f"{pipeline_name}-databricks-job.json"
            job_filepath = os.path.join(self.jobs_dir, job_filename)

            if not os.path.exists(job_filepath):
                missing_jobs.append(job_filename)
                print(f"  Missing: {job_filename}")
            else:
                print(f"  Found: {job_filename}")

            # check that package files have been prepared
            package_dir = os.path.join(build_directory, pipeline_name)

            # check if setup.py file exists in the package directory
            setup_py_path = os.path.join(package_dir, "setup.py")
            if not os.path.exists(setup_py_path):
                missing_prep.append(f"{pipeline_name} setup.py")
                print(f"  Missing: {pipeline_name} setup.py")

        if missing_jobs:
            print(f"\nERROR: Missing job JSON files for {len(missing_jobs)} pipeline(s)")
            print("Please run 'pbt generate-p4b-job' first to generate missing job definitions.")
            sys.exit(1)
        if missing_prep:
            print(f"\nERROR: Missing package files for {len(missing_prep)} pipeline(s)")
            print("Please run 'pbt build-orch --prep-only' first to generate missing package files.")
            sys.exit(1)

        # Create wheel output directory
        if not os.path.exists(self.wheel_output_dir):
            os.makedirs(self.wheel_output_dir)
            print(f"\nCreated wheel output directory: {self.wheel_output_dir}")
        else:
            print(f"\nWheel output directory already exists: {self.wheel_output_dir}")

        # Build wheels for each pipeline
        print("\nBuilding wheels...")
        built_count = 0
        failed_pipelines = []

        for pipeline_name in pipelines:
            print(f"\n  Building wheel for pipeline: {pipeline_name}")

            try:
                temp_dir = os.path.join(build_directory, pipeline_name)
                # Build the wheel
                build_cmd = [sys.executable, "setup.py", "bdist_wheel", "--dist-dir", self.wheel_output_dir]

                result = subprocess.run(
                    build_cmd,
                    cwd=temp_dir,
                    capture_output=True,
                    text=True,
                )

                if result.returncode == 0:
                    print(f"    Successfully built wheel for {pipeline_name}")
                    built_count += 1
                else:
                    print(f"    ERROR: Failed to build wheel for {pipeline_name}")
                    if result.stderr:
                        print(f"    {result.stderr}")
                    failed_pipelines.append(pipeline_name)

            except Exception as e:
                print(f"    ERROR: Failed to build wheel for {pipeline_name}: {e}")
                failed_pipelines.append(pipeline_name)

        # Summary
        print(f"\nSuccessfully built {built_count}/{len(pipelines)} wheel(s)")

        if failed_pipelines:
            print(f"\nFailed pipelines:")
            for pipeline in failed_pipelines:
                print(f"  - {pipeline}")

        print(f"\nWheel files located in: {self.wheel_output_dir}")

    def deploy_orch(self, pipeline_name: Optional[str] = None):
        """
        Deploy orchestration: upload wheels and create Databricks jobs.

        Args:
            pipeline_name: Optional specific pipeline name to deploy. If None, deploys all pipelines.

        This command:
        1. Verifies DATABRICKS_HOST and DATABRICKS_TOKEN environment variables
        2. Checks that job JSONs and wheels exist
        3. Uploads wheels to DBFS
        4. Creates Databricks jobs using the job JSON definitions
        """
        print("\nStarting orchestration deployment...")

        # Verify environment variables
        databricks_host = os.environ.get("DATABRICKS_HOST")
        databricks_token = os.environ.get("DATABRICKS_TOKEN")

        if not databricks_host or not databricks_token:
            print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set")
            print("\nPlease set the following environment variables:")
            print("  export DATABRICKS_HOST=https://your-databricks-instance.cloud.databricks.com")
            print("  export DATABRICKS_TOKEN=your-databricks-token")
            sys.exit(1)

        print(f"Connected to: {databricks_host}")

        # Create Databricks client
        try:
            client = DatabricksClient.from_databricks_info(
                host=databricks_host,
                auth_type=None,
                token=databricks_token,
                oauth_client_secret=None,
                user_agent="Prophecy-Build-Tool-Orchestration",
            )
        except Exception as e:
            print(f"ERROR: Failed to connect to Databricks: {e}")
            sys.exit(1)

        # Determine which pipelines to deploy
        if pipeline_name:
            pipelines_to_deploy = [pipeline_name]
            print(f"\nDeploying specific pipeline: {pipeline_name}")
        else:
            pipelines_to_deploy = get_pipelines_from_project(self.project_path)
            print(f"\nDeploying all pipelines ({len(pipelines_to_deploy)} total)")

        if not pipelines_to_deploy:
            print(f"ERROR: No pipelines to deploy")
            sys.exit(1)

        # Verify all required files exist
        print("\nVerifying deployment artifacts...")
        missing_artifacts = []

        for pname in pipelines_to_deploy:
            # Check job JSON
            job_filename = f"{pname}-databricks-job.json"
            job_filepath = os.path.join(self.jobs_dir, job_filename)

            if not os.path.exists(job_filepath):
                missing_artifacts.append(f"Job JSON: {job_filename}")
                print(f"  Missing job JSON: {job_filename}")
            else:
                print(f"  Found job JSON: {job_filename}")

            # Check wheel file
            # setuptools normalizes package names to lowercase
            package_name = f"{self.project_name}_{pname}".replace("-", "_").replace(" ", "_").lower()
            wheel_pattern = f"{package_name}-*.whl"

            wheel_found = False
            if os.path.exists(self.wheel_output_dir):
                for f in os.listdir(self.wheel_output_dir):
                    if f.startswith(package_name) and f.endswith(".whl"):
                        wheel_found = True
                        print(f"  Found wheel: {f}")
                        break

            if not wheel_found:
                missing_artifacts.append(f"Wheel: {wheel_pattern}")
                print(f"  Missing wheel: {wheel_pattern}")

        if missing_artifacts:
            print(f"\nERROR: Missing deployment artifacts")
            print("Please run the following commands first:")
            print("  1. pbt generate-p4b-job --path <project-path>")
            print("  2. pbt build-orch --path <project-path>")
            sys.exit(1)

        # Deploy each pipeline
        print("\nDeploying pipelines...")
        deployed_count = 0
        failed_deployments = []

        for pname in pipelines_to_deploy:
            print(f"\n  Deploying pipeline: {pname}")

            try:
                # Find the wheel file
                # setuptools normalizes package names to lowercase
                package_name = f"{self.project_name}_{pname}".replace("-", "_").replace(" ", "_").lower()
                wheel_file = None
                wheel_filename = None

                for f in os.listdir(self.wheel_output_dir):
                    if f.startswith(package_name) and f.endswith(".whl"):
                        wheel_filename = f
                        wheel_file = os.path.join(self.wheel_output_dir, f)
                        break

                if not wheel_file:
                    raise FileNotFoundError(f"Wheel file not found for {pname}")

                # Upload wheel to Workspace Files (required for DBR 15+)
                workspace_path = (
                    f"/Workspace/Shared/prophecy/orch/jpmc/wheel-exec/{self.project_name}/{pname}/{wheel_filename}"
                )

                print(f"    Uploading wheel...")
                client.upload_src_path(src_path=wheel_file, destination_path=workspace_path)

                # Read job JSON
                job_filename = f"{pname}-databricks-job.json"
                job_filepath = os.path.join(self.jobs_dir, job_filename)

                with open(job_filepath, "r") as f:
                    job_json = json.load(f)

                    # Update wheel paths in tasks
                for task in job_json.get("tasks", []):
                    if "libraries" in task:
                        for lib in task["libraries"]:
                            if "whl" in lib:
                                lib["whl"] = workspace_path

                # Update package_name in python_wheel_task
                for task in job_json.get("tasks", []):
                    if "python_wheel_task" in task:
                        task["python_wheel_task"]["package_name"] = package_name

                # Check if job already exists
                job_name = job_json.get("name")
                existing_job_id = client.find_job(job_name)

                if existing_job_id:
                    print(f"    Updating job (ID: {existing_job_id})")
                    client.reset_job(existing_job_id, job_json)
                    job_id = existing_job_id
                else:
                    print(f"    Creating job...")
                    response = client.create_job(job_json)
                    job_id = response.get("job_id")

                # Show job details with clickable link
                job_url = f"{databricks_host.rstrip('/')}/jobs/{job_id}"
                print(f"    Job: {job_name}")
                print(f"    URL: {job_url}")

                deployed_count += 1

            except Exception as e:
                print(f"    ERROR: Failed to deploy {pname}: {e}")
                failed_deployments.append(pname)

        # Summary
        print(f"\nSuccessfully deployed {deployed_count}/{len(pipelines_to_deploy)} pipeline(s)")

        if failed_deployments:
            print(f"\nFailed deployments:")
            for pipeline in failed_deployments:
                print(f"  - {pipeline}")
            sys.exit(1)

        print("\nDeployment completed successfully!")
