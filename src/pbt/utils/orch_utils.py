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
    
    # Base path for wheel file (Workspace Files for DBR 15+ compatibility)
    wheel_path = f"/Workspace/Shared/prophecy/orch/jpmc/wheel-exec/{project_name}/{pipeline_name}/{wheel_filename}"
    
    # Job name and cluster key
    job_name = f"prophecy_{project_name}_{pipeline_name}_orchestration_job"
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
                    "data_security_mode": "NONE",
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
    requirements_path = os.path.join(project_path, "data", project_name, "cicd", "requirements-pipeline.txt")
    
    setup_py_content = f'''import os
from setuptools import setup

# Read dependencies from requirements-pipeline.txt if it exists
install_requires = []
requirements_file = "{requirements_path}"

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
    packages=['{package_name}', 'data'],  # Two packages
    package_data={{
        'data': ['{project_name}/**/*', '{project_name}/**/.*'],  # Include all project files
    }},
    include_package_data=True,
    install_requires=install_requires,
    entry_points={{
        'console_scripts': [
            'main = {package_name}.__main__:main',
        ],
    }},
    python_requires=">=3.7",
    description="Prophecy Pipeline Orchestration: {pipeline_name}",
    zip_safe=False,
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

