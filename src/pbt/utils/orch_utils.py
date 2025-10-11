"""
Orchestration utilities for PBT commands.
Provides helper functions for generating job JSONs, setup.py templates, and managing orchestration workflows.
"""

import os
import json
from typing import Dict, Optional, Tuple


def generate_job_json_template(
    pipeline_name: str,
    project_name: str,
    project_id: str = "__PROJECT_ID_PLACEHOLDER__",
    release_version: str = "__PROJECT_RELEASE_VERSION_PLACEHOLDER__",
    prophecy_url: str = "__PROPHECY_URL_PLACEHOLDER__",
    schedule_enabled: bool = False,
    cron_expression: Optional[str] = None,
    spark_version: str = "16.4.x-scala2.12",
    node_type: str = "m7gd.large",
) -> dict:
    """
    Generate a Databricks job JSON template for a pipeline.
    
    Args:
        pipeline_name: Name of the pipeline
        project_name: Name of the project
        project_id: Project ID placeholder
        release_version: Release version placeholder
        prophecy_url: Prophecy URL placeholder
        schedule_enabled: Whether scheduling is enabled
        cron_expression: Cron expression for scheduling
        spark_version: Databricks Spark version
        node_type: AWS node type
    
    Returns:
        Dictionary containing the job JSON structure
    """
    # Sanitize pipeline name for package naming
    package_name = f"{project_name}_{pipeline_name}".replace("-", "_").replace(" ", "_")
    wheel_filename = f"{package_name}-1.0.0-py3-none-any.whl"
    
    # Base path for wheel file
    wheel_path = f"dbfs:/FileStore/prophecy/artifacts/saas/app/jpmc/orchestrate/{project_id}/{wheel_filename}"
    
    # Job name and cluster key
    job_name = f"{pipeline_name}_orchestration_job"
    job_cluster_key = f"{job_name}_cluster"
    
    job_json = {
        "name": job_name,
        "email_notifications": {},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": pipeline_name,
                "run_if": "ALL_SUCCESS",
                "python_wheel_task": {
                    "package_name": package_name,
                    "entry_point": "main",
                    "parameters": ["-i", "default", "-O", "{}"]
                },
                "job_cluster_key": job_cluster_key,
                "libraries": [
                    {
                        "whl": wheel_path
                    }
                ],
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": job_cluster_key,
                "new_cluster": {
                    "spark_version": spark_version,
                    "spark_conf": {
                        "spark.prophecy.metadata.job.uri": f"{project_id}/jobs/{pipeline_name}_job",
                        "spark.prophecy.metadata.is.interactive.run": "false",
                        "spark.prophecy.project.id": project_id,
                        "spark.prophecy.metadata.job.branch": release_version,
                        "spark.prophecy.metadata.url": prophecy_url,
                        "spark.prophecy.execution.metrics.disabled": "true",
                        "spark.prophecy.execution.service.url": "wss://execution.dp.app.prophecy.io/eventws",
                        "spark.databricks.isv.product": "prophecy"
                    },
                    "aws_attributes": {
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "zone_id": "auto",
                        "spot_bid_price_percent": 100
                    },
                    "node_type_id": node_type,
                    "enable_elastic_disk": False,
                    "data_security_mode": "DATA_SECURITY_MODE_DEDICATED",
                    "runtime_engine": "PHOTON",
                    "kind": "CLASSIC_PREVIEW",
                    "is_single_node": True,
                    "num_workers": 0
                }
            }
        ]
    }
    
    # Add schedule if enabled
    if schedule_enabled and cron_expression:
        job_json["schedule"] = {
            "quartz_cron_expression": cron_expression,
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED"
        }
    else:
        # Default to paused schedule
        job_json["schedule"] = {
            "quartz_cron_expression": "0 0 0 1/1 * ? *",
            "timezone_id": "UTC",
            "pause_status": "PAUSED"
        }
    
    return job_json


def generate_setup_py_content(project_name: str, pipeline_name: str, project_path: str, version: str = "1.0.0") -> str:
    """
    Generate setup.py content for orchestration execution.
    
    This creates a special setup.py that:
    1. Gets the orchestration binary from environment
    2. Creates execution directory
    3. Downloads and extracts the wheel from DBFS
    4. Runs the orchestration binary with streaming output
    
    Args:
        project_name: Name of the project
        pipeline_name: Name of the pipeline
        project_path: Path to the project directory
        version: Version string for the package
    
    Returns:
        String containing the setup.py content
    """
    package_name = f"{project_name}_{pipeline_name}".replace("-", "_").replace(" ", "_")
    
    setup_py_content = f'''import os
import sys
import subprocess
from setuptools import setup, find_packages
from setuptools.command.install import install

class OrchestrationInstall(install):
    """Custom install command that runs orchestration."""
    
    def run(self):
        """Execute the orchestration workflow."""

        # Detect if we're in a wheel build context vs actual install
        # During bdist_wheel, install_lib path contains 'build/bdist' or 'build\\\\bdist'
        # During real install in Databricks, it goes to site-packages
        install_lib = getattr(self, 'install_lib', '')
        
        if 'build' in install_lib and ('bdist' in install_lib or 'wheel' in install_lib):
            # We're building the wheel, not actually installing - skip orchestration
            install.run(self)
            return
        
        # Check if we're in Databricks orchestration context
        orch_binary_path = os.environ.get("ORCHESTRATION_BINARY_PATH")
        
        if not orch_binary_path:
            # Not in orchestration context - just do standard install
            install.run(self)
            return
        
        print("Starting orchestration execution...")
        
        print(f"Orchestration binary path: {{orch_binary_path}}")
        
        # Set up execution paths
        project_name = "{project_name}"
        pipeline_name = "{pipeline_name}"
        package_name = "{package_name}"
        
        execution_base = f"/tmp/prophecy/orchestrate-exec/{{project_name}}/{{pipeline_name}}"
        os.makedirs(execution_base, exist_ok=True)
        print(f"Execution directory: {{execution_base}}")
        
        # Change to execution directory
        os.chdir(execution_base)
        
        # Get the wheel filename
        wheel_filename = f"{{package_name}}-{version}-py3-none-any.whl"
        wheel_path = os.path.join(execution_base, wheel_filename)
        
        # Copy wheel from DBFS to execution path using dbutils
        try:
            print(f"Downloading wheel from DBFS...")
            # Construct DBFS source path
            dbfs_wheel_path = f"dbfs:/FileStore/prophecy/orch/{{project_name}}/{{pipeline_name}}/{{wheel_filename}}"
            
            # Use dbutils to copy from DBFS
            dbutils.fs.cp(dbfs_wheel_path, f"file://{{wheel_path}}")
            print(f"Downloaded wheel to: {{wheel_path}}")
        
        except Exception as e:
            print(f"ERROR: Failed to download wheel: {{e}}")
            sys.exit(1)
        
        # Unzip the wheel using unzip command
        try:
            print(f"Extracting wheel...")
            result = subprocess.run(
                ['unzip', '-o', wheel_path, '-d', execution_base],
                check=True,
                capture_output=True,
                text=True
            )
            print(f"Wheel extracted to: {{execution_base}}")
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Failed to extract wheel: {{e}}")
            if e.stderr:
                print(f"{{e.stderr}}")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: Failed to extract wheel: {{e}}")
            sys.exit(1)
        
        # Get access token from auth.py if it exists
        try:
            print(f"Getting access token from auth.py...")
            
            # Add extraction directory to Python path to import auth module
            sys.path.insert(0, execution_base)
            
            # Try to import and call get_access_token from cicd/scripts/auth.py
            auth_module_path = os.path.join(execution_base, "cicd", "scripts", "auth.py")
            
            if os.path.exists(auth_module_path):
                # Import the auth module
                import importlib.util
                spec = importlib.util.spec_from_file_location("auth", auth_module_path)
                auth = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(auth)
                
                # Call get_access_token() to get the token
                access_token = auth.get_access_token()
                print(f"Successfully obtained access token")
                
                # Set environment variables
                os.environ["PROPHECY_CREDS_DBX_TOKEN"] = access_token
                print(f"Set PROPHECY_CREDS_DBX_TOKEN environment variable")
            else:
                print(f"Warning: auth.py not found at {{auth_module_path}}, skipping token generation")
                
        except Exception as e:
            print(f"Warning: Failed to get access token from auth.py: {{e}}")
            # Continue execution - token might be provided another way
        
        # Set other credential environment variables from Databricks secrets
        try:
            # Get secrets from Databricks secret scope using dbutils
            dbx_jdbcurl = dbutils.secrets.get(scope="prophecy", key="dbx_jdbcurl")
            tableau_token = dbutils.secrets.get(scope="prophecy", key="tableau_token")
            
            os.environ["PROPHECY_CREDS_DBX_JDBCURL"] = dbx_jdbcurl
            os.environ["PROPHECY_CREDS_TABLEAU_TOKEN"] = tableau_token
            print(f"Set credential environment variables from Databricks secrets")
        except Exception as e:
            print(f"Warning: Failed to get secrets from Databricks: {{e}}")
        
        # Copy orchestration binary to execution path using dbutils
        try:
            print(f"Copying orchestration binary (deploy-cli)...")
            binary_name = "deploy-cli"
            local_binary_path = os.path.join(execution_base, binary_name)
            
            # Use dbutils to copy the binary
            dbutils.fs.cp(f"file://{{orch_binary_path}}", f"file://{{local_binary_path}}")
            print(f"Binary copied to: {{local_binary_path}}")
            
            # Make the binary executable
            os.chmod(binary_name, 0o755)
            print(f"Binary is now executable")
        except Exception as e:
            print(f"ERROR: Failed to copy/setup binary: {{e}}")
            sys.exit(1)
        
        # Run the orchestration binary with streaming output
        try:
            print(f"\\nExecuting orchestration binary...")
            print("=" * 80)
            
            # Build the command: ./deploy-cli run <execution_base> --project-id <id> --pipeline-name <name>
            # The execution_base already points to /tmp/prophecy/orchestrate-exec/{project_name}/{pipeline_name}
            # We're already in execution_base (os.chdir above), so use ./deploy-cli
            command = [
                f"./{{binary_name}}",
                "run",
                execution_base,
                "--project-id", project_name,
                "--pipeline-name", pipeline_name
            ]
            
            print(f"Command: {{' '.join(command)}}")
            print("=" * 80)
            
            # Prepare environment variables for the deploy-cli execution
            env = os.environ.copy()
            env["ORCHESTRATION_BINARY_PATH"] = orch_binary_path
            
            # Run the binary and stream output in real-time
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                env=env
            )
            
            # Stream output line by line
            for line in process.stdout:
                print(line, end='')
                sys.stdout.flush()
            
            # Wait for process to complete
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
        
        # Now run the standard install
        install.run(self)

# The pipeline .py file should be in the same directory as this setup.py
# (it will be copied there during build)

# Read dependencies from requirements-pipeline.txt if it exists
install_requires = []
requirements_file = os.path.join("{project_path}", "cicd", "requirements-pipeline.txt")

if os.path.exists(requirements_file):
    with open(requirements_file, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if line and not line.startswith('#'):
                install_requires.append(line)

setup(
    name="{package_name}",
    version="{version}",
    py_modules=["{pipeline_name}"],  # Single Python module: {pipeline_name}.py
    packages=find_packages(include=['cicd*']),  # Include cicd package if it exists
    install_requires=install_requires,
    cmdclass={{
        'install': OrchestrationInstall,
    }},
    python_requires=">=3.7",
    description="Prophecy Pipeline Orchestration: {pipeline_name}",
)
'''
    return setup_py_content


def get_resolved_pipeline_info(project_path: str, pipeline_name: str) -> Optional[dict]:
    """
    Read and return resolved pipeline JSON information.
    
    Args:
        project_path: Path to the project directory
        pipeline_name: Name of the pipeline
    
    Returns:
        Dictionary containing the resolved pipeline info, or None if not found
    """
    resolved_pipeline_path = os.path.join(
        project_path, 
        ".prophecy", 
        "ide", 
        "resolved_pipelines", 
        f"{pipeline_name}.json"
    )
    
    if os.path.exists(resolved_pipeline_path):
        try:
            with open(resolved_pipeline_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not read resolved pipeline info for {pipeline_name}: {e}")
            return None
    
    return None


def check_schedule_enabled(resolved_pipeline_json: Optional[dict]) -> Tuple[bool, Optional[str]]:
    """
    Check if schedule is enabled and return the cron expression.

    Args:
        resolved_pipeline_json: Resolved pipeline JSON dictionary

    Returns:
        Tuple of (is_enabled, cron_expression)
    """
    if not resolved_pipeline_json:
        return False, None

    try:
        # Check if there's a schedule configuration
        schedule = resolved_pipeline_json.get("schedule", {})
        
        if not schedule:
            return False, None
        
        # Check if schedule is enabled
        enabled = schedule.get("enabled", False)
        cron_expression = schedule.get("cronExpression", None)
        
        return enabled, cron_expression
    
    except Exception as e:
        print(f"Warning: Could not parse schedule information: {e}")
        return False, None


def sanitize_pipeline_name(pipeline_name: str) -> str:
    """
    Sanitize pipeline name to be used in file names and package names.
    
    Args:
        pipeline_name: Original pipeline name
    
    Returns:
        Sanitized pipeline name
    """
    return pipeline_name.replace("-", "_").replace(" ", "_").replace(".", "_")


def get_pipelines_from_project(project_path: str) -> list:
    """
    Get list of all pipelines in the project by scanning for .py files in the pipelines directory.
    
    Args:
        project_path: Path to the project directory
    
    Returns:
        List of pipeline names (without .py extension)
    """
    pipelines_dir = os.path.join(project_path, "pipelines")
    
    if not os.path.exists(pipelines_dir):
        return []
    
    pipelines = []
    for item in os.listdir(pipelines_dir):
        # Check if it's a .py file
        if item.endswith('.py') and os.path.isfile(os.path.join(pipelines_dir, item)):
            # Remove .py extension to get pipeline name
            pipeline_name = item[:-3]
            pipelines.append(pipeline_name)
    
    return pipelines

