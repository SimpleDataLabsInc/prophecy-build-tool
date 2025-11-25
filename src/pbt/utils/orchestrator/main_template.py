"""Orchestration entry point module."""

import argparse
import os
import shutil
import subprocess
import sys
import yaml
from datetime import datetime


def main():
    """Main orchestration entry point - runs deploy-cli binary."""
    # Parse command line arguments for Databricks runtime variables
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Input parameter")
    parser.add_argument("-O", "--output", help="Output parameter")
    parser.add_argument("--job-id", help="Databricks Job ID")
    parser.add_argument("--run-id", help="Databricks Run ID")
    parser.add_argument("--workspace-id", help="Databricks Workspace ID")
    args, unknown = parser.parse_known_args()

    # Set Databricks environment variables if provided
    if args.job_id:
        os.environ["DATABRICKS_JOB_ID"] = args.job_id
    if args.run_id:
        os.environ["DATABRICKS_JOB_RUN_ID"] = args.run_id
    if args.workspace_id:
        os.environ["DATABRICKS_WORKSPACE_ID"] = args.workspace_id

    exec_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get the path to the data folder of this wheel package
    this_file_dir = os.path.dirname(os.path.abspath(__file__))
    site_packages = os.path.dirname(this_file_dir)
    data_package_path = os.path.join(site_packages, "data")

    project_name = None
    for item in os.listdir(data_package_path):
        candidate_dir = os.path.join(data_package_path, item)
        if os.path.isdir(candidate_dir):
            candidate_yml = os.path.join(candidate_dir, "pbt_project.yml")
            if os.path.isfile(candidate_yml):
                project_name = item
                break
    if project_name is None:
        print("✗ ERROR: Could not find a subdirectory in 'data' containing 'pbt_project.yml'.")
        sys.exit(1)

    # Dynamically derive project_name and pipeline_name from pbt_project.yml stored in data section
    with open(os.path.join(data_package_path, project_name, "pbt_project.yml"), "r") as f:
        pbt_project_dict = yaml.safe_load(f)
        pipeline_name = pbt_project_dict["pipeline_name"]

    print("\n\n" + "=" * 80)
    print(f"  PROPHECY ORCHESTRATION")
    print(f"  Project: {project_name} | Pipeline: {pipeline_name}")
    print(f"  Execution ID: {exec_id}")
    print("=" * 80 + "\n")

    # Step 1: Validate environment
    print("[1/4] Validating environment...")
    orchestrator_path = os.environ.get("ORCHESTRATOR_PATH")
    if not orchestrator_path:
        print("✗ ERROR: ORCHESTRATOR_PATH environment variable not set")
        print("  Ensure spark_env_vars is configured with secrets in job definition")
        sys.exit(1)
    print(f"✓ Orchestrator path: {orchestrator_path[1:]}")

    # Step 2: Setup execution environment
    print("\n[2/4] Setting up execution environment...")

    execution_base = f"/tmp/prophecy/orchestrate-exec/{project_name}/{pipeline_name}"
    os.makedirs(execution_base, exist_ok=True)
    os.chdir(execution_base)
    print(f"✓ Execution directory: {execution_base}")

    # Retrieve credentials (auth token if available)
    try:

        for item in os.listdir(data_package_path):
            item_path = os.path.join(data_package_path, item)
            if os.path.isdir(item_path):
                cicd_scripts_dir = os.path.join(item_path, "cicd", "scripts")
                if os.path.exists(os.path.join(cicd_scripts_dir, "auth.py")):
                    sys.path.insert(0, cicd_scripts_dir)
                    import auth

                    access_token = auth.get_access_token()
                    os.environ["PROPHECY_CREDS_DBX_TOKEN"] = access_token
                    print("✓ Access token obtained from auth.py")
                    break
    except Exception:
        print("ℹ No custom auth.py found, using environment credentials")

    # Step 3: Prepare orchestration binary
    print("\n[3/4] Preparing orchestration binary...")
    try:
        binary_name = "deploy-cli"
        binary_source = f"{orchestrator_path}/deploy-cli"
        local_binary_path = os.path.join(execution_base, binary_name)

        shutil.copy(binary_source, local_binary_path)
        os.chmod(local_binary_path, 0o755)
        print(f"✓ Binary ready: {binary_source[1:]}")

    except Exception as e:
        print(f"✗ ERROR: Failed to prepare binary: {e}")
        sys.exit(1)

    # Copy project files from wheel data package
    try:
        package_name_full = f"{project_name}_{pipeline_name}".replace("-", "_").replace(" ", "_").lower()

        data_package_path = os.path.join(site_packages, "data")
        data_in_main_path = os.path.join(site_packages, package_name_full, "data")

        project_dir_found = None
        if os.path.exists(data_in_main_path):
            project_dir_found = data_in_main_path
        elif os.path.exists(data_package_path):
            data_contents = [d for d in os.listdir(data_package_path) if d != "__pycache__" and d != "__init__.py"]
            for item in data_contents:
                item_path = os.path.join(data_package_path, item)
                if os.path.isdir(item_path):
                    project_dir_found = item_path
                    break

        if project_dir_found:
            copied_count = 0
            for item in os.listdir(project_dir_found):
                src = os.path.join(project_dir_found, item)
                dst = os.path.join(execution_base, item)
                if os.path.isdir(src):
                    if not os.path.exists(dst):
                        shutil.copytree(src, dst)
                        copied_count += 1
                else:
                    shutil.copy(src, dst)
                    copied_count += 1
            print(f"✓ Copied {copied_count} project files to execution directory")
    except Exception:
        print("ℹ No additional project files to copy")

    # Step 4: Execute deploy-cli
    print("\n[4/4] Executing deploy-cli orchestrator...")
    print("-" * 80)

    try:
        binary_name = "deploy-cli"
        command = [
            f"./{binary_name}",
            "run",
            execution_base,
            "--project-id",
            project_name,
            "--pipeline-name",
            pipeline_name,
        ]

        # Configure environment for deploy-cli
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = f"{orchestrator_path}/bin"
        env["CONFIG_FILE"] = f"{orchestrator_path}/config/embedded.yml"
        env["LOG_FORMAT"] = "console"
        env["ORCHESTRATOR_PATH"] = orchestrator_path

        print(f"Command: {' '.join(command)}")
        print(f"Config: {env['CONFIG_FILE'][1:]}")
        print("-" * 80 + "\n")

        # Execute binary with streaming output
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            env=env,
        )

        for line in process.stdout:
            print(line, end="")
            sys.stdout.flush()

        return_code = process.wait()

        # Report final status
        print("\n" + "=" * 80)
        if return_code != 0:
            print(f"✗ ORCHESTRATION FAILED (Exit code: {return_code})")
            print("=" * 80)
            sys.exit(return_code)
        else:
            print(f"✓ ORCHESTRATION COMPLETED SUCCESSFULLY")
            print(f"  Project: {project_name} | Pipeline: {pipeline_name}")
            print("=" * 80 + "\n")

    except Exception as e:
        print("\n" + "=" * 80)
        if "Exec format error" in str(e):
            print("✗ ERROR: Binary architecture mismatch")
            print("  The deploy-cli binary is not compiled for Linux x86_64")
            print("  Please provide a Linux-compatible binary")
        else:
            print(f"✗ ERROR: Execution failed - {e}")
        print("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()
