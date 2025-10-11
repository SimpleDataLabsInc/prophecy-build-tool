"""
Orchestration commands for PBT.
Handles generation of job JSONs, building wheels, and deploying to Databricks.
"""

import os
import sys
import json
import subprocess
import tempfile
from typing import Optional, List

from .client.databricks import DatabricksClient
from .utils.orch_utils import (
    generate_job_json_template,
    generate_setup_py_content,
    get_resolved_pipeline_info,
    check_schedule_enabled,
    get_pipelines_from_project,
    sanitize_pipeline_name,
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
        self.resolved_pipelines_dir = os.path.join(
            self.project_path, ".prophecy", "ide", "resolved_pipelines"
        )
        
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
                schedule_enabled, cron_expression = check_schedule_enabled(resolved_info)
                
                # Generate job JSON
                job_json = generate_job_json_template(
                    pipeline_name=pipeline_name,
                    project_name=self.project_name,
                    schedule_enabled=schedule_enabled,
                    cron_expression=cron_expression,
                )
                
                # Write to file
                job_filename = f"{pipeline_name}-databricks-job.json"
                job_filepath = os.path.join(self.jobs_dir, job_filename)
                
                with open(job_filepath, 'w') as f:
                    json.dump(job_json, f, indent=2)
                
                schedule_status = "enabled" if schedule_enabled else "disabled"
                print(f"  Generated {job_filename} (schedule: {schedule_status})")
                generated_count += 1
                
            except Exception as e:
                print(f"  ERROR: Failed to generate job JSON for {pipeline_name}: {e}")
        
        print(f"\nSuccessfully generated {generated_count}/{len(pipelines)} job JSON file(s)")
        print(f"Job JSON files are located in: {self.jobs_dir}")
    
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
        
        # Verify all job JSONs exist
        print("\nVerifying job JSON files...")
        missing_jobs = []
        
        for pipeline_name in pipelines:
            job_filename = f"{pipeline_name}-databricks-job.json"
            job_filepath = os.path.join(self.jobs_dir, job_filename)
            
            if not os.path.exists(job_filepath):
                missing_jobs.append(job_filename)
                print(f"  Missing: {job_filename}")
            else:
                print(f"  Found: {job_filename}")
        
        if missing_jobs:
            print(f"\nERROR: Missing job JSON files for {len(missing_jobs)} pipeline(s)")
            print("Please run 'pbt generate-p4b-job' first to generate missing job definitions.")
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
                # Create a temporary directory for building
                with tempfile.TemporaryDirectory() as temp_dir:
                    # Copy ALL files from the project folder to temp directory
                    import shutil
                    
                    print(f"    Copying all project files from: {self.project_path}")
                    
                    # Copy ALL items from project directory (everything, no exceptions)
                    for item in os.listdir(self.project_path):
                        source_path = os.path.join(self.project_path, item)
                        dest_path = os.path.join(temp_dir, item)
                        
                        if os.path.isdir(source_path):
                            shutil.copytree(source_path, dest_path)
                        else:
                            shutil.copy(source_path, dest_path)
                    
                    print(f"    Copied all project files to temp directory")
                    
                    # Get ORCHESTRATION_BINARY_PATH from environment (to bake into wheel)
                    orch_binary_path_raw = os.environ.get("ORCHESTRATION_BINARY_PATH")
                    if not orch_binary_path_raw:
                        print(f"    Warning: ORCHESTRATION_BINARY_PATH not set, using placeholder")
                        orch_binary_path = "/dbfs/FileStore/prophecy/orch/deploy-cli"
                    else:
                        orch_binary_path = orch_binary_path_raw
                        print(f"    Using ORCHESTRATION_BINARY_PATH: {orch_binary_path}")
                    
                    # Create simple package structure
                    # Package name: {project_name}_{pipeline_name}
                    package_name = f"{self.project_name}_{pipeline_name}".replace("-", "_").replace(" ", "_")
                    package_dir = os.path.join(temp_dir, package_name)
                    os.makedirs(package_dir, exist_ok=True)
                    
                    # Move all project files into package/data/
                    data_in_package = os.path.join(package_dir, "data")
                    os.makedirs(data_in_package, exist_ok=True)
                    
                    for item in os.listdir(temp_dir):
                        if item == package_name:  # Skip the package dir itself
                            continue
                        source = os.path.join(temp_dir, item)
                        dest = os.path.join(data_in_package, item)
                        shutil.move(source, dest)
                    
                    print(f"    Moved all project files into {package_name}/data/")
                    
                    # Create __init__.py for the package that exports main
                    init_content = '''"""Prophecy Orchestration Package."""
from .__main__ import main

__all__ = ['main']
'''
                    with open(os.path.join(package_dir, "__init__.py"), 'w') as f:
                        f.write(init_content)
                    
                    # Create __main__.py as the entry point module inside the package
                    main_module_content = f'''"""Orchestration entry point module."""
import os
import sys
import subprocess

def main():
    """Main orchestration entry point - runs deploy-cli binary."""
    print("Starting orchestration execution...")
    
    # Initialize dbutils (not auto-available in python_wheel_task)
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    
    # Orchestration binary path (baked in at build time)
    orch_binary_path = "{orch_binary_path}"
    print(f"Orchestration binary path: {{orch_binary_path}}")
    
    # Set up execution paths
    project_name = "{self.project_name}"
    pipeline_name = "{pipeline_name}"
    
    execution_base = f"/tmp/prophecy/orchestrate-exec/{{project_name}}/{{pipeline_name}}"
    os.makedirs(execution_base, exist_ok=True)
    print(f"Execution directory: {{execution_base}}")
    
    # Change to execution directory
    os.chdir(execution_base)
    
    # Get access token from auth.py if it exists
    try:
        print(f"Getting access token from auth.py...")
        
        # Project files are in data/project_name/ within the package
        import site
        site_packages = site.getsitepackages()[0]
        project_data_path = os.path.join(site_packages, "data", project_name)
        cicd_scripts_dir = os.path.join(project_data_path, "cicd", "scripts")
        
        if os.path.exists(os.path.join(cicd_scripts_dir, "auth.py")):
            sys.path.insert(0, cicd_scripts_dir)
            import auth
            access_token = auth.get_access_token()
            print(f"Successfully obtained access token")
            
            os.environ["PROPHECY_CREDS_DBX_TOKEN"] = access_token
            print(f"Set PROPHECY_CREDS_DBX_TOKEN environment variable")
        else:
            print(f"Warning: auth.py not found, skipping token generation")
            
    except Exception as e:
        print(f"Warning: Failed to get access token from auth.py: {{e}}")
    
    # Set other credential environment variables from Databricks secrets
    try:
        dbx_jdbcurl = dbutils.secrets.get(scope="prophecy", key="dbx_jdbcurl")
        tableau_token = dbutils.secrets.get(scope="prophecy", key="tableau_token")
        
        os.environ["PROPHECY_CREDS_DBX_JDBCURL"] = dbx_jdbcurl
        os.environ["PROPHECY_CREDS_TABLEAU_TOKEN"] = tableau_token
        print(f"Set credential environment variables from Databricks secrets")
    except Exception as e:
        print(f"Warning: Failed to get secrets from Databricks: {{e}}")
    
    # Copy orchestration binary to execution directory using shutil
    try:
        print(f"Copying orchestration binary from: {{orch_binary_path}}")
        binary_name = "deploy-cli"
        local_binary_path = os.path.join(execution_base, binary_name)
        
        # Use shutil.copy() - path should be accessible as local filesystem
        import shutil
        shutil.copy(orch_binary_path, local_binary_path)
        print(f"Binary copied to: {{local_binary_path}}")
        
        # Make it executable
        os.chmod(local_binary_path, 0o755)
        print(f"Binary is now executable")
        
        # Verify the binary file format
        try:
            with open(local_binary_path, 'rb') as f:
                header = f.read(100)
                if header[:4] == b'\\x7fELF':
                    print(f"Binary is ELF format (Linux executable)")
                elif header[:2] == b'#!':
                    # It's a script with shebang
                    first_line = header.split(b'\\n')[0].decode('utf-8', errors='ignore')
                    print(f"Binary is a script with shebang: {{first_line}}")
                else:
                    print(f"WARNING: Binary format unknown")
                    print(f"First 20 bytes (hex): {{header[:20].hex()}}")
                    print(f"First 50 chars (text): {{header[:50].decode('utf-8', errors='ignore')}}")
        except Exception as check_err:
            print(f"WARNING: Could not verify binary format: {{check_err}}")
    except Exception as e:
        print(f"ERROR: Failed to copy binary: {{e}}")
        print(f"Binary path: {{orch_binary_path}}")
        print(f"Target path: {{local_binary_path}}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Copy project files from installed package
    try:
        print(f"Copying project files to execution directory...")
        
        # Project files are in {package}/data/ within the installed package
        import site
        site_packages = site.getsitepackages()[0]
        package_name_full = "{self.project_name}_{pipeline_name}".replace("-", "_").replace(" ", "_").lower()
        package_data_path = os.path.join(site_packages, package_name_full, "data")
        
        print(f"Looking for project files at: {{package_data_path}}")
        
        if os.path.exists(package_data_path):
            import shutil
            for item in os.listdir(package_data_path):
                src = os.path.join(package_data_path, item)
                dst = os.path.join(execution_base, item)
                if os.path.isdir(src):
                    if not os.path.exists(dst):
                        shutil.copytree(src, dst)
                else:
                    shutil.copy(src, dst)
            print(f"Project files copied successfully")
        else:
            print(f"WARNING: Project data directory not found at {{package_data_path}}")
    except Exception as e:
        print(f"Warning: Failed to copy project files: {{e}}")
        import traceback
        traceback.print_exc()
    
    # Run the orchestration binary with streaming output
    try:
        print(f"\\nExecuting orchestration binary...")
        print("=" * 80)
        
        # Run the binary from execution_base (where we copied it)
        binary_name = "deploy-cli"
        command = [
            f"./{{binary_name}}",
            "run",
            execution_base,
            "--project-id", project_name,
            "--pipeline-name", pipeline_name
        ]
        
        print(f"Command: {{' '.join(command)}}")
        print("=" * 80)
        
        env = os.environ.copy()
        env["ORCHESTRATION_BINARY_PATH"] = orch_binary_path
        
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            env=env
        )
        
        for line in process.stdout:
            print(line, end='')
            sys.stdout.flush()
        
        return_code = process.wait()
        
        print("=" * 80)
        print(f"\\nOrchestration completed with exit code: {{return_code}}")
        
        if return_code != 0:
            print(f"ERROR: Orchestration failed with exit code {{return_code}}")
            sys.exit(return_code)
        
    except Exception as e:
        print(f"ERROR: Failed to execute orchestration binary: {{e}}")
        sys.exit(1)
    
    print("\\nOrchestration execution completed successfully!")

if __name__ == "__main__":
    main()
'''
                    main_module_path = os.path.join(package_dir, "__main__.py")
                    with open(main_module_path, 'w') as f:
                        f.write(main_module_content)
                    
                    print(f"    Created package: {package_name}/")
                    
                    # Generate setup.py content
                    setup_py_content = generate_setup_py_content(
                        project_name=self.project_name,
                        pipeline_name=pipeline_name,
                        project_path=temp_dir,  # Use temp_dir as the base
                    )
                    
                    # Write setup.py to temp directory
                    setup_py_path = os.path.join(temp_dir, "setup.py")
                    with open(setup_py_path, 'w') as f:
                        f.write(setup_py_content)
                    
                    # Debug: Show temp directory structure before building
                    print(f"    Temp directory structure:")
                    for root, dirs, files in os.walk(temp_dir):
                        level = root.replace(temp_dir, '').count(os.sep)
                        indent = ' ' * 2 * level
                        print(f"    {indent}{os.path.basename(root)}/")
                        if level < 2:  # Only show first 2 levels
                            subindent = ' ' * 2 * (level + 1)
                            for file in files[:5]:  # Show first 5 files
                                print(f"    {subindent}{file}")
                            if len(files) > 5:
                                print(f"    {subindent}... and {len(files)-5} more files")
                    
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
                        print(f"    Return code: {result.returncode}")
                        if result.stdout:
                            print(f"    STDOUT:\n{result.stdout}")
                        if result.stderr:
                            print(f"    STDERR:\n{result.stderr}")
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
        
        print(f"\nWheel files are located in: {self.wheel_output_dir}")
        
        # List generated wheels
        if os.path.exists(self.wheel_output_dir):
            wheels = [f for f in os.listdir(self.wheel_output_dir) if f.endswith('.whl')]
            if wheels:
                print(f"\nGenerated wheel files:")
                for wheel in wheels:
                    print(f"  - {wheel}")
    
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
        
        print(f"Found Databricks credentials")
        print(f"Host: {databricks_host}")
        
        # Create Databricks client
        try:
            client = DatabricksClient.from_databricks_info(
                host=databricks_host,
                auth_type=None,
                token=databricks_token,
                oauth_client_secret=None,
                user_agent="Prophecy-Build-Tool-Orchestration"
            )
            print(f"Connected to Databricks")
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
                    if f.startswith(package_name) and f.endswith('.whl'):
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
                    if f.startswith(package_name) and f.endswith('.whl'):
                        wheel_filename = f
                        wheel_file = os.path.join(self.wheel_output_dir, f)
                        break
                
                if not wheel_file:
                    raise FileNotFoundError(f"Wheel file not found for {pname}")
                
                # Upload wheel to Workspace Files (required for DBR 15+)
                workspace_path = f"/Workspace/Shared/prophecy/orch/jpmc/wheel-exec/{self.project_name}/{pname}/{wheel_filename}"
                
                print(f"    Uploading wheel to Workspace Files...")
                print(f"    Source: {wheel_file}")
                print(f"    Destination: {workspace_path}")
                
                client.upload_src_path(src_path=wheel_file, destination_path=workspace_path)
                print(f"    Wheel uploaded successfully")
                
                # Read job JSON
                job_filename = f"{pname}-databricks-job.json"
                job_filepath = os.path.join(self.jobs_dir, job_filename)
                
                with open(job_filepath, 'r') as f:
                    job_json = json.load(f)
                
                # Update wheel paths in tasks to ensure job uses the correct wheel path
                for task in job_json.get("tasks", []):
                    if "libraries" in task:
                        for lib in task["libraries"]:
                            if "whl" in lib:
                                lib["whl"] = workspace_path
                                print(f"    Updated task library path to: {workspace_path}")
                
                # Also update in python_wheel_task if needed
                for task in job_json.get("tasks", []):
                    if "python_wheel_task" in task:
                        # Ensure package_name matches
                        task["python_wheel_task"]["package_name"] = package_name
                        print(f"    Updated task package_name to: {package_name}")
                
                # Check if job already exists
                job_name = job_json.get("name")
                print(f"    Checking if job '{job_name}' already exists...")
                
                existing_job_id = client.find_job(job_name)
                
                if existing_job_id:
                    # Job exists - update it
                    print(f"    Job already exists with ID: {existing_job_id}")
                    print(f"    Updating existing job...")
                    client.reset_job(existing_job_id, job_json)
                    job_id = existing_job_id
                    print(f"    Job updated successfully")
                else:
                    # Job doesn't exist - create new one
                    print(f"    Job does not exist, creating new job...")
                    response = client.create_job(job_json)
                    job_id = response.get("job_id")
                    print(f"    Job created successfully")
                
                print(f"    Job ID: {job_id}")
                print(f"    Job Name: {job_name}")
                
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

