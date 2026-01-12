import json
import os
import re
import shutil
from typing import Dict, Optional, Tuple
import yaml

from ..utility import custom_print as log, python_pipeline_name
from ..utils.constants import PBT_FILE_NAME


def get_pipeline_identifiers(workflow_path: str) -> Dict[str, str]:
    if not os.path.exists(workflow_path):
        raise FileNotFoundError(f"Workflow file not found: {workflow_path}")
    
    with open(workflow_path, 'r') as f:
        workflow = json.load(f)
    
    metainfo = workflow.get('metainfo', {})
    pipeline_settings = metainfo.get('pipelineSettingsInfo', {})
    
    return {
        'package_name': metainfo.get('topLevelPackage', ''),
        'config_package_name': metainfo.get('configTopLevelPackage', ''),
        'application_name': pipeline_settings.get('applicationName', ''),
        'uri': metainfo.get('uri', '')
    }


def derive_new_identifiers(new_pipeline_name: str) -> Dict[str, str]:
    safe_name = python_pipeline_name(new_pipeline_name)
    package_suffix = safe_name
    app_suffix = safe_name if not safe_name.endswith('App') else safe_name[:-3]
    return {
        'package_name': f'io.prophecy.pipe.{package_suffix}',
        'config_package_name': f'io.prophecy.config.{package_suffix}',
        'application_name': f'io.prophecy.{app_suffix}App',
        'new_pipeline_id': f'pipelines/{new_pipeline_name}'
    }


def validate_rename(project_path: str, old_name: str, new_name: str) -> Tuple[bool, Optional[str]]:
    old_pipeline_path = os.path.join(project_path, 'pipelines', old_name)
    new_pipeline_path = os.path.join(project_path, 'pipelines', new_name)
    if not os.path.exists(old_pipeline_path):
        return False, f"Pipeline '{old_name}' does not exist at {old_pipeline_path}"    
    if os.path.exists(new_pipeline_path):
        return False, f"Pipeline '{new_name}' already exists at {new_pipeline_path}"
    pbt_project_file = os.path.join(project_path, PBT_FILE_NAME)
    if os.path.exists(pbt_project_file):
        with open(pbt_project_file, 'r') as f:
            project_config = yaml.safe_load(f)
        
        pipelines = project_config.get('pipelines', {})
        old_pipeline_id = f'pipelines/{old_name}'
        
        if old_pipeline_id not in pipelines:
            return False, f"Pipeline '{old_name}' not found in {PBT_FILE_NAME}"
        
        new_pipeline_id = f'pipelines/{new_name}'
        if new_pipeline_id in pipelines:
            return False, f"Pipeline '{new_name}' already exists in {PBT_FILE_NAME}"
    
    return True, None


def validate_package_scoping(
    project_path: str,
    target_pipeline_name: str,
    old_package_name: str
) -> Tuple[bool, Optional[str]]:
    pipelines_dir = os.path.join(project_path, 'pipelines')
    
    if not os.path.exists(pipelines_dir):
        return True, None
    
    conflicting_pipelines = []
    
    for pipeline_dir in os.listdir(pipelines_dir):
        if pipeline_dir == target_pipeline_name:
            continue
        
        pipeline_path = os.path.join(pipelines_dir, pipeline_dir)
        if not os.path.isdir(pipeline_path):
            continue
        
        workflow_path = os.path.join(pipeline_path, 'code', '.prophecy', 'workflow.latest.json')
        if os.path.exists(workflow_path):
            try:
                identifiers = get_pipeline_identifiers(workflow_path)
                other_package = identifiers.get('package_name', '')
                if old_package_name == other_package:
                    conflicting_pipelines.append(pipeline_dir)
                elif old_package_name in other_package or other_package in old_package_name:
                    log(f"Warning: Package name '{old_package_name}' may overlap with pipeline '{pipeline_dir}' (package: '{other_package}')")
            except Exception:
                continue
    
    if conflicting_pipelines:
        return False, f"Package name '{old_package_name}' is also used by other pipelines: {', '.join(conflicting_pipelines)}"
    
    return True, None


def rename_pipeline_directory(project_path: str, old_name: str, new_name: str) -> None:
    old_path = os.path.join(project_path, 'pipelines', old_name)
    new_path = os.path.join(project_path, 'pipelines', new_name)
    
    if os.path.exists(old_path):
        log(f"Renaming directory: {old_path} -> {new_path}")
        shutil.move(old_path, new_path)
    else:
        raise FileNotFoundError(f"Pipeline directory not found: {old_path}")


def rename_package_directories(
    pipeline_code_path: str,
    old_package_name: str,
    new_package_name: str
) -> None:
    old_package_path = os.path.join(pipeline_code_path, *old_package_name.split('.'))
    new_package_path = os.path.join(pipeline_code_path, *new_package_name.split('.'))
    
    if os.path.exists(old_package_path):
        log(f"Renaming package directory: {old_package_path} -> {new_package_path}")
        os.makedirs(os.path.dirname(new_package_path), exist_ok=True)
        shutil.move(old_package_path, new_package_path)
    
    old_config_package = old_package_name.replace('io.prophecy.pipe', 'io.prophecy.config')
    new_config_package = new_package_name.replace('io.prophecy.pipe', 'io.prophecy.config')
    
    configs_path = os.path.join(pipeline_code_path, 'configs', 'resources')
    old_config_path = os.path.join(configs_path, *old_config_package.split('.'))
    new_config_path = os.path.join(configs_path, *new_config_package.split('.'))
    
    if os.path.exists(old_config_path):
        log(f"Renaming config package directory: {old_config_path} -> {new_config_path}")
        os.makedirs(os.path.dirname(new_config_path), exist_ok=True)
        shutil.move(old_config_path, new_config_path)
        
        if os.path.isdir(new_config_path):
            for filename in os.listdir(new_config_path):
                if filename.endswith('.conf') and old_package_name.split('.')[-1] in filename:
                    old_file = os.path.join(new_config_path, filename)
                    new_filename = filename.replace(
                        old_package_name.split('.')[-1],
                        new_package_name.split('.')[-1]
                    )
                    new_file = os.path.join(new_config_path, new_filename)
                    log(f"Renaming config file: {old_file} -> {new_file}")
                    shutil.move(old_file, new_file)


def update_workflow_json(
    workflow_path: str,
    old_identifiers: Dict[str, str],
    new_identifiers: Dict[str, str]
) -> None:
    if not os.path.exists(workflow_path):
        log(f"Workflow file not found: {workflow_path}, skipping update")
        return
    
    with open(workflow_path, 'r') as f:
        workflow = json.load(f)
    
    if 'metainfo' in workflow:
        workflow['metainfo']['uri'] = new_identifiers['new_pipeline_id']
        workflow['metainfo']['topLevelPackage'] = new_identifiers['package_name']
        workflow['metainfo']['configTopLevelPackage'] = new_identifiers['config_package_name']
        
        if 'pipelineSettingsInfo' in workflow['metainfo']:
            workflow['metainfo']['pipelineSettingsInfo']['applicationName'] = new_identifiers['application_name']
    
    with open(workflow_path, 'w') as f:
        json.dump(workflow, f, indent=2)
    
    log(f"Updated workflow.latest.json with new identifiers")


def update_workflow_json_uri_only(
    workflow_path: str,
    old_pipeline_id: str,
    new_pipeline_id: str
) -> None:
    if not os.path.exists(workflow_path):
        log(f"Workflow file not found: {workflow_path}, skipping update")
        return
    
    with open(workflow_path, 'r') as f:
        workflow = json.load(f)
    
    if 'metainfo' in workflow:
        workflow['metainfo']['uri'] = new_pipeline_id
    
    with open(workflow_path, 'w') as f:
        json.dump(workflow, f, indent=2)
    
    log(f"Updated workflow.latest.json URI: {old_pipeline_id} -> {new_pipeline_id}")


def update_python_files(
    pipeline_code_path: str,
    old_identifiers: Dict[str, str],
    new_identifiers: Dict[str, str],
    old_pipeline_id: str,
    new_pipeline_id: str
) -> None:
    old_package = old_identifiers['package_name']
    new_package = new_identifiers['package_name']
    old_config_package = old_identifiers['config_package_name']
    new_config_package = new_identifiers['config_package_name']
    old_app_name = old_identifiers['application_name']
    new_app_name = new_identifiers['application_name']
    
    for root, dirs, files in os.walk(pipeline_code_path):
        if 'build' in root or 'dist' in root or '__pycache__' in root:
            continue
        
        for filename in files:
            if filename.endswith('.py'):
                file_path = os.path.join(root, filename)
                update_python_file(
                    file_path,
                    old_package,
                    new_package,
                    old_config_package,
                    new_config_package,
                    old_app_name,
                    new_app_name,
                    old_pipeline_id,
                    new_pipeline_id
                )


def update_python_file(
    file_path: str,
    old_package: str,
    new_package: str,
    old_config_package: str,
    new_config_package: str,
    old_app_name: str,
    new_app_name: str,
    old_pipeline_id: str,
    new_pipeline_id: str
) -> None:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    old_package_escaped = re.escape(old_package)
    old_config_package_escaped = re.escape(old_config_package)
    old_package_pattern = r'(?<![a-zA-Z0-9_.])' + old_package_escaped + r'(?![a-zA-Z0-9_])'
    old_config_package_pattern = r'(?<![a-zA-Z0-9_.])' + old_config_package_escaped + r'(?![a-zA-Z0-9_])'
    content = re.sub(old_package_pattern, new_package, content)
    content = re.sub(old_config_package_pattern, new_config_package, content)
    old_app_simple = old_app_name.split('.')[-1].replace('App', '')
    new_app_simple = new_app_name.split('.')[-1].replace('App', '')
    content = re.sub(
        r'\.appName\(["\']' + re.escape(old_app_simple) + r'["\']\)',
        f'.appName("{new_app_simple}")',
        content
    )
    content = re.sub(
        r'\.appName\(["\']' + re.escape(old_package) + r'["\']\)',
        f'.appName("{new_app_simple}")',
        content
    )
    
    old_pipeline_name_safe = old_pipeline_id.split('/')[-1]
    new_pipeline_name_safe = new_pipeline_id.split('/')[-1]
    content = re.sub(
        r'prophecy_config_instances\.' + re.escape(old_pipeline_name_safe) + r'(?![a-zA-Z0-9_])',
        f'prophecy_config_instances.{new_pipeline_name_safe}',
        content
    )
    
    if os.path.basename(file_path) == '__init__.py':
        content = re.sub(
            r'from\s+\.' + re.escape(old_pipeline_name_safe) + r'\s+import',
            f'from .{new_pipeline_name_safe} import',
            content
        )
    
    old_pipeline_name = old_pipeline_id.split('/')[-1]
    new_pipeline_name = new_pipeline_id.split('/')[-1]
    # Use regex with word boundaries for pipeline name in various contexts
    # First handle quoted strings with just pipeline name
    content = re.sub(
        r'"pipelines/' + re.escape(old_pipeline_name) + r'"',
        f'"{new_pipeline_id}"',
        content
    )
    content = re.sub(
        r"'pipelines/" + re.escape(old_pipeline_name) + r"'",
        f"'{new_pipeline_id}'",
        content
    )
    # Also handle cases where pipeline URI might have the full package name (incorrect format)
    content = re.sub(
        r'"pipelines/' + re.escape(old_package) + r'"',
        f'"{new_pipeline_id}"',
        content
    )
    content = re.sub(
        r"'pipelines/" + re.escape(old_package) + r"'",
        f"'{new_pipeline_id}'",
        content
    )
    # Then handle unquoted pipeline ID references (with word boundary to avoid substring matches)
    content = re.sub(
        r'\b' + re.escape(f'pipelines/{old_pipeline_name}') + r'\b',
        new_pipeline_id,
        content
    )
    # Also handle unquoted full package name references
    content = re.sub(
        r'\b' + re.escape(f'pipelines/{old_package}') + r'\b',
        new_pipeline_id,
        content
    )
    
    # Update pipelineId in MetricsCollector.instrument calls
    content = re.sub(
        r'pipelineId\s*=\s*["\']' + re.escape(old_pipeline_id) + r'["\']',
        f'pipelineId = "{new_pipeline_id}"',
        content
    )
    
    # Update setup.py specific patterns
    if os.path.basename(file_path) == 'setup.py':
        # Update package name
        content = re.sub(
            r"name\s*=\s*['\"]" + re.escape(old_pipeline_name) + r"['\"]",
            f'name = "{new_pipeline_id.split("/")[-1]}"',
            content
        )
        
        # Update find_packages
        content = re.sub(
            r"find_packages\(include\s*=\s*\('" + re.escape(old_package) + r"\*'",
            f"find_packages(include = ('{new_package}*'",
            content
        )
        
        # Update package_dir - handle both package names and prophecy_config_instances
        content = re.sub(
            r"['\"]" + re.escape(old_package) + r"['\"]\s*:\s*['\"][^'\"]+['\"]",
            lambda m: m.group(0).replace(old_package, new_package),
            content
        )
        # Also update prophecy_config_instances.{old_name} patterns
        old_pipeline_name_safe = old_pipeline_id.split('/')[-1]
        new_pipeline_name_safe = new_pipeline_id.split('/')[-1]
        content = re.sub(
            r"['\"]prophecy_config_instances\." + re.escape(old_pipeline_name_safe) + r"['\"]\s*:\s*['\"][^'\"]+['\"]",
            lambda m: m.group(0).replace(f'prophecy_config_instances.{old_pipeline_name_safe}', f'prophecy_config_instances.{new_pipeline_name_safe}'),
            content
        )
        
        # Update package_data - handle both package names and prophecy_config_instances
        content = re.sub(
            r"['\"]" + re.escape(old_package) + r"['\"]\s*:\s*\[[^\]]+\]",
            lambda m: m.group(0).replace(old_package, new_package),
            content
        )
        # Also update prophecy_config_instances.{old_name} in package_data
        content = re.sub(
            r"['\"]prophecy_config_instances\." + re.escape(old_pipeline_name_safe) + r"['\"]\s*:\s*\[[^\]]+\]",
            lambda m: m.group(0).replace(f'prophecy_config_instances.{old_pipeline_name_safe}', f'prophecy_config_instances.{new_pipeline_name_safe}'),
            content
        )
        
        # Update packages list - handle prophecy_config_instances.{old_name}
        content = re.sub(
            r"\['prophecy_config_instances\." + re.escape(old_pipeline_name_safe) + r"'\]",
            f"['prophecy_config_instances.{new_pipeline_name_safe}']",
            content
        )
        content = re.sub(
            r'\["prophecy_config_instances\.' + re.escape(old_pipeline_name_safe) + r'"\]',
            f'["prophecy_config_instances.{new_pipeline_name_safe}"]',
            content
        )
        
        # Update entry points
        content = re.sub(
            r"['\"]main\s*=\s*" + re.escape(old_package) + r"\.pipeline:main['\"]",
            f"'main = {new_package}.pipeline:main'",
            content
        )
    
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        log(f"Updated Python file: {file_path}")


def update_python_files_pipeline_id_only(
    pipeline_code_path: str,
    old_pipeline_id: str,
    new_pipeline_id: str,
    old_name: str,
    new_name: str
) -> None:
    for root, dirs, files in os.walk(pipeline_code_path):
        # Skip build and dist directories
        if 'build' in root or 'dist' in root or '__pycache__' in root:
            continue
        
        for filename in files:
            if filename.endswith('.py'):
                file_path = os.path.join(root, filename)
                update_python_file_pipeline_id_only(
                    file_path,
                    old_pipeline_id,
                    new_pipeline_id,
                    old_name,
                    new_name
                )


def update_python_file_pipeline_id_only(
    file_path: str,
    old_pipeline_id: str,
    new_pipeline_id: str,
    old_name: str,
    new_name: str
) -> None:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    content = re.sub(
        r'"pipelines/' + re.escape(old_name) + r'"',
        f'"{new_pipeline_id}"',
        content
    )
    content = re.sub(
        r"'pipelines/" + re.escape(old_name) + r"'",
        f"'{new_pipeline_id}'",
        content
    )
    # Then handle unquoted references (with word boundary to avoid substring matches)
    content = re.sub(
        r'\b' + re.escape(f'pipelines/{old_name}') + r'\b',
        new_pipeline_id,
        content
    )
    
    # Update pipelineId in MetricsCollector.instrument calls
    content = re.sub(
        r'pipelineId\s*=\s*["\']' + re.escape(old_pipeline_id) + r'["\']',
        f'pipelineId = "{new_pipeline_id}"',
        content
    )
    
    # Update .appName() calls - only the pipeline name part
    content = re.sub(
        r'\.appName\(["\']' + re.escape(old_name) + r'["\']\)',
        f'.appName("{new_name}")',
        content
    )
    
    # Update spark.conf.set for pipeline URI
    content = re.sub(
        r'spark\.conf\.set\(["\']prophecy\.metadata\.pipeline\.uri["\'],\s*["\']' + re.escape(old_pipeline_id) + r'["\']\)',
        f'spark.conf.set("prophecy.metadata.pipeline.uri", "{new_pipeline_id}")',
        content
    )
    
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        log(f"Updated Python file: {file_path}")


def update_pbt_project_yml(
    project_path: str,
    old_pipeline_id: str,
    new_pipeline_id: str,
    new_pipeline_name: str
) -> None:
    pbt_project_file = os.path.join(project_path, PBT_FILE_NAME)
    
    if not os.path.exists(pbt_project_file):
        raise FileNotFoundError(f"Project file not found: {pbt_project_file}")
    
    # Read and parse YAML
    with open(pbt_project_file, 'r') as f:
        project_config = yaml.safe_load(f)
    
    if not project_config:
        raise ValueError(f"Invalid YAML in {PBT_FILE_NAME}")
    
    # Update pipeline entry - only update the specific pipeline
    pipelines = project_config.get('pipelines', {})
    if old_pipeline_id not in pipelines:
        raise ValueError(f"Pipeline '{old_pipeline_id}' not found in {PBT_FILE_NAME}")
    
    # Move pipeline entry to new key
    pipeline_data = pipelines[old_pipeline_id]
    del pipelines[old_pipeline_id]
    pipelines[new_pipeline_id] = pipeline_data
    
    # Update pipeline name field
    if isinstance(pipeline_data, dict):
        pipeline_data['name'] = new_pipeline_name
    
    # Update job references - only update references to this specific pipeline
    jobs = project_config.get('jobs', {})
    for job_id, job_data in jobs.items():
        if isinstance(job_data, dict):
            # Update pipelines list if it exists
            if 'pipelines' in job_data and isinstance(job_data['pipelines'], list):
                job_data['pipelines'] = [
                    new_pipeline_id if pipeline_id == old_pipeline_id else pipeline_id
                    for pipeline_id in job_data['pipelines']
                ]
    
    # Write back to file, preserving YAML structure
    with open(pbt_project_file, 'w') as f:
        yaml.dump(project_config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    log(f"Updated {PBT_FILE_NAME}: {old_pipeline_id} -> {new_pipeline_id}")


def update_job_json_files(
    project_path: str,
    old_pipeline_id: str,
    new_pipeline_id: str,
    old_name: str = None,
    new_name: str = None,
    old_package_name: str = None,
    new_package_name: str = None
) -> None:
    jobs_path = os.path.join(project_path, 'jobs')
    
    if not os.path.exists(jobs_path):
        return
    
    # Find all prophecy-job.json and databricks-job.json files
    for root, dirs, files in os.walk(jobs_path):
        for filename in files:
            if filename in ['prophecy-job.json', 'databricks-job.json']:
                json_file_path = os.path.join(root, filename)
                update_json_file(
                    json_file_path,
                    old_pipeline_id,
                    new_pipeline_id,
                    old_name,
                    new_name,
                    old_package_name,
                    new_package_name
                )
    
    for root, dirs, files in os.walk(jobs_path):
        if 'build' in root or 'dist' in root or '__pycache__' in root:
            continue
        
        for filename in files:
            if filename.endswith('.py'):
                file_path = os.path.join(root, filename)
                update_job_python_file(
                    file_path,
                    old_pipeline_id,
                    new_pipeline_id,
                    old_name,
                    new_name,
                    old_package_name,
                    new_package_name
                )


def update_job_python_file(
    file_path: str,
    old_pipeline_id: str,
    new_pipeline_id: str,
    old_name: str = None,
    new_name: str = None,
    old_package_name: str = None,
    new_package_name: str = None
) -> None:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    old_pipeline_name = old_pipeline_id.split('/')[-1] if '/' in old_pipeline_id else old_pipeline_id
    new_pipeline_name = new_pipeline_id.split('/')[-1] if '/' in new_pipeline_id else new_pipeline_id
    
    old_name_to_use = old_name if old_name else old_pipeline_name
    new_name_to_use = new_name if new_name else new_pipeline_name
    
    content = re.sub(
        r'"pipelines/' + re.escape(old_name_to_use) + r'":',
        f'"{new_pipeline_id}":',
        content
    )
    content = re.sub(
        r"'pipelines/" + re.escape(old_name_to_use) + r"':",
        f"'{new_pipeline_id}':",
        content
    )
    
    if old_package_name and new_package_name:
        content = re.sub(
            r'"pipelines/' + re.escape(old_name_to_use) + r'"\s*:\s*["\']' + re.escape(old_package_name) + r'["\']',
            f'"{new_pipeline_id}": "{new_package_name}"',
            content
        )
        content = re.sub(
            r"'pipelines/" + re.escape(old_name_to_use) + r"'\s*:\s*['\"]" + re.escape(old_package_name) + r"['\"]",
            f"'{new_pipeline_id}': '{new_package_name}'",
            content
        )
    
    if old_package_name and new_package_name:
        content = re.sub(
            r'dbfs:/[^"\']*' + re.escape(old_package_name) + r'[^"\']*\.whl',
            lambda m: m.group(0).replace(old_package_name, new_package_name),
            content
        )
    content = re.sub(
        r'dbfs:/[^"\']*' + re.escape(old_name_to_use) + r'[^"\']*\.whl',
        lambda m: m.group(0).replace(old_name_to_use, new_name_to_use),
        content
    )
    
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        log(f"Updated job Python file: {file_path}")


def update_job_python_file(
    file_path: str,
    old_pipeline_id: str,
    new_pipeline_id: str,
    old_name: str = None,
    new_name: str = None,
    old_package_name: str = None,
    new_package_name: str = None
) -> None:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    old_pipeline_name = old_pipeline_id.split('/')[-1] if '/' in old_pipeline_id else old_pipeline_id
    new_pipeline_name = new_pipeline_id.split('/')[-1] if '/' in new_pipeline_id else new_pipeline_id
    
    old_name_to_use = old_name if old_name else old_pipeline_name
    new_name_to_use = new_name if new_name else new_pipeline_name
    
    content = re.sub(
        r'"pipelines/' + re.escape(old_name_to_use) + r'":',
        f'"{new_pipeline_id}":',
        content
    )
    content = re.sub(
        r"'pipelines/" + re.escape(old_name_to_use) + r"':",
        f"'{new_pipeline_id}':",
        content
    )
    
    if old_package_name and new_package_name:
        content = re.sub(
            r'"pipelines/' + re.escape(old_name_to_use) + r'"\s*:\s*["\']' + re.escape(old_package_name) + r'["\']',
            f'"{new_pipeline_id}": "{new_package_name}"',
            content
        )
        content = re.sub(
            r"'pipelines/" + re.escape(old_name_to_use) + r"'\s*:\s*['\"]" + re.escape(old_package_name) + r"['\"]",
            f"'{new_pipeline_id}': '{new_package_name}'",
            content
        )
    
    # Update wheel file paths
    if old_package_name and new_package_name:
        content = re.sub(
            r'dbfs:/[^"\']*' + re.escape(old_package_name) + r'[^"\']*\.whl',
            lambda m: m.group(0).replace(old_package_name, new_package_name),
            content
        )
    # Also update if path contains old pipeline name
    content = re.sub(
        r'dbfs:/[^"\']*' + re.escape(old_name_to_use) + r'[^"\']*\.whl',
        lambda m: m.group(0).replace(old_name_to_use, new_name_to_use),
        content
    )
    
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        log(f"Updated job Python file: {file_path}")


def update_json_file(
    json_file_path: str,
    old_pipeline_id: str,
    new_pipeline_id: str,
    old_name: str = None,
    new_name: str = None,
    old_package_name: str = None,
    new_package_name: str = None
) -> None:
    with open(json_file_path, 'r') as f:
        data = json.load(f)
    
    updated = False
    old_pipeline_name = old_pipeline_id.split('/')[-1] if '/' in old_pipeline_id else old_pipeline_id
    new_pipeline_name = new_pipeline_id.split('/')[-1] if '/' in new_pipeline_id else new_pipeline_id
    
    # Use provided names if available, otherwise derive from pipeline ID
    old_name_to_use = old_name if old_name else old_pipeline_name
    new_name_to_use = new_name if new_name else new_pipeline_name
    
    # Update prophecy-job.json structure
    if 'processes' in data:
        for process_key, process_value in data['processes'].items():
            if isinstance(process_value, dict):
                # Update pipelineId
                if 'properties' in process_value:
                    if 'pipelineId' in process_value['properties']:
                        if process_value['properties']['pipelineId'] == old_pipeline_id:
                            process_value['properties']['pipelineId'] = new_pipeline_id
                            updated = True
                
                # Update metadata label and slug
                if 'metadata' in process_value:
                    metadata = process_value['metadata']
                    if 'label' in metadata and metadata['label'] == old_name_to_use:
                        metadata['label'] = new_name_to_use
                        updated = True
                    if 'slug' in metadata and metadata['slug'] == old_name_to_use:
                        metadata['slug'] = new_name_to_use
                        updated = True
    
    # Update databricks-job.json structure
    if 'components' in data:
        for component in data['components']:
            if 'PipelineComponent' in component:
                pc = component['PipelineComponent']
                # Update pipelineId
                if 'pipelineId' in pc:
                    if pc['pipelineId'] == old_pipeline_id:
                        pc['pipelineId'] = new_pipeline_id
                        updated = True
                
                # Update nodeName
                if 'nodeName' in pc and pc['nodeName'] == old_name_to_use:
                    pc['nodeName'] = new_name_to_use
                    updated = True
                
                # Update path (wheel file path)
                if 'path' in pc and old_package_name and new_package_name:
                    if old_package_name in pc['path']:
                        pc['path'] = pc['path'].replace(old_package_name, new_package_name)
                        updated = True
                
                # Also update path if it contains old pipeline name
                if 'path' in pc and old_name_to_use in pc['path']:
                    pc['path'] = pc['path'].replace(old_name_to_use, new_name_to_use)
                    updated = True
    
    # Update request/tasks structure for databricks-job.json
    if 'request' in data:
        request = data['request']
        if 'CreateNewJobRequest' in request:
            request = request['CreateNewJobRequest']
        
        # Update tasks
        if 'tasks' in request:
            for task in request['tasks']:
                # Update task_key
                if 'task_key' in task and task['task_key'] == old_name_to_use:
                    task['task_key'] = new_name_to_use
                    updated = True
                
                # Update python_wheel_task.package_name
                if 'python_wheel_task' in task:
                    pwt = task['python_wheel_task']
                    if 'package_name' in pwt:
                        if old_package_name and new_package_name and old_package_name in pwt['package_name']:
                            pwt['package_name'] = pwt['package_name'].replace(old_package_name, new_package_name)
                            updated = True
                        elif old_name_to_use in pwt['package_name']:
                            pwt['package_name'] = pwt['package_name'].replace(old_name_to_use, new_name_to_use)
                            updated = True
                
                # Update libraries (wheel paths)
                if 'libraries' in task:
                    for library in task['libraries']:
                        # Update PipelineComponent.pipelineId in libraries
                        if 'PipelineComponent' in library:
                            if 'pipelineId' in library['PipelineComponent']:
                                if library['PipelineComponent']['pipelineId'] == old_pipeline_id:
                                    library['PipelineComponent']['pipelineId'] = new_pipeline_id
                                    updated = True
                        
                        # Update whl path
                        if 'whl' in library:
                            if old_package_name and new_package_name and old_package_name in library['whl']:
                                library['whl'] = library['whl'].replace(old_package_name, new_package_name)
                                updated = True
                            elif old_name_to_use in library['whl']:
                                library['whl'] = library['whl'].replace(old_name_to_use, new_name_to_use)
                                updated = True
                
                # Update depends_on task_key references
                if 'depends_on' in task:
                    for dep in task['depends_on']:
                        if isinstance(dep, dict) and 'task_key' in dep:
                            if dep['task_key'] == old_name_to_use:
                                dep['task_key'] = new_name_to_use
                                updated = True
        
        # Update job_clusters spark_conf prophecy.packages.path
        if 'job_clusters' in request:
            for cluster in request['job_clusters']:
                if 'new_cluster' in cluster:
                    nc = cluster['new_cluster']
                    if 'spark_conf' in nc:
                        spark_conf = nc['spark_conf']
                        if 'prophecy.packages.path' in spark_conf:
                            # This is a JSON string, so we need to parse it, update, and stringify
                            try:
                                packages_path = json.loads(spark_conf['prophecy.packages.path'])
                                path_updated = False
                                
                                # Update pipeline ID keys
                                if old_pipeline_id in packages_path:
                                    packages_path[new_pipeline_id] = packages_path.pop(old_pipeline_id)
                                    path_updated = True
                                
                                # Update package names in paths
                                if old_package_name and new_package_name:
                                    for key, path_value in packages_path.items():
                                        if old_package_name in path_value:
                                            packages_path[key] = path_value.replace(old_package_name, new_package_name)
                                            path_updated = True
                                
                                # Update pipeline names in paths
                                for key, path_value in packages_path.items():
                                    if old_name_to_use in path_value:
                                        packages_path[key] = path_value.replace(old_name_to_use, new_name_to_use)
                                        path_updated = True
                                
                                if path_updated:
                                    spark_conf['prophecy.packages.path'] = json.dumps(packages_path)
                                    updated = True
                            except (json.JSONDecodeError, TypeError):
                                # If it's not valid JSON, try simple string replacement
                                if old_pipeline_id in spark_conf['prophecy.packages.path']:
                                    spark_conf['prophecy.packages.path'] = spark_conf['prophecy.packages.path'].replace(
                                        old_pipeline_id, new_pipeline_id
                                    )
                                    updated = True
                                if old_package_name and new_package_name and old_package_name in spark_conf['prophecy.packages.path']:
                                    spark_conf['prophecy.packages.path'] = spark_conf['prophecy.packages.path'].replace(
                                        old_package_name, new_package_name
                                    )
                                    updated = True
                                if old_name_to_use in spark_conf['prophecy.packages.path']:
                                    spark_conf['prophecy.packages.path'] = spark_conf['prophecy.packages.path'].replace(
                                        old_name_to_use, new_name_to_use
                                    )
                                    updated = True
    
    if updated:
        with open(json_file_path, 'w') as f:
            json.dump(data, f, indent=2)
        log(f"Updated JSON file: {json_file_path}")


def rename_pipeline(
    project_path: str,
    old_name: str,
    new_name: str,
    unsafe: bool = False
) -> None:
    log(f"Starting pipeline rename: {old_name} -> {new_name}")
    
    if unsafe:
        log(f"UNSAFE MODE: Will rename package names, app names, and all identifiers.")
        print("WARNING: This will update all package imports, directory structures, and identifiers throughout the codebase.")
        print("It can corrupt the project, please proceed at your own risk.")
    else:
        log(f"SAFE MODE: Only pipeline name/ID will be changed. Package names and app names remain unchanged.")
    
    # Validate rename
    is_valid, error_msg = validate_rename(project_path, old_name, new_name)
    if not is_valid:
        raise ValueError(error_msg)
    
    old_pipeline_id = f'pipelines/{old_name}'
    new_pipeline_id = f'pipelines/{new_name}'
    
    # Step 1: Rename main pipeline directory
    rename_pipeline_directory(project_path, old_name, new_name)
    
    new_pipeline_code_path = os.path.join(project_path, 'pipelines', new_name, 'code')
    new_workflow_path = os.path.join(new_pipeline_code_path, '.prophecy', 'workflow.latest.json')
    
    if unsafe:
        old_workflow_path = os.path.join(new_pipeline_code_path, '.prophecy', 'workflow.latest.json')
        old_identifiers = get_pipeline_identifiers(old_workflow_path)
        
        is_valid, warning = validate_package_scoping(
            project_path,
            new_name,
            old_identifiers['package_name']
        )
        if not is_valid:
            raise ValueError(f"Package scoping validation failed: {warning}")
        if warning:
            log(f"Warning: {warning}")
        
        new_identifiers = derive_new_identifiers(new_name)
        
        log(f"Old package: {old_identifiers['package_name']}")
        log(f"New package: {new_identifiers['package_name']}")
        
        rename_package_directories(
            new_pipeline_code_path,
            old_identifiers['package_name'],
            new_identifiers['package_name']
        )
        
        update_workflow_json(new_workflow_path, old_identifiers, new_identifiers)
        update_python_files(
            new_pipeline_code_path,
            old_identifiers,
            new_identifiers,
            old_pipeline_id,
            new_pipeline_id
        )
    else:
        update_workflow_json_uri_only(new_workflow_path, old_pipeline_id, new_pipeline_id)
        update_python_files_pipeline_id_only(
            new_pipeline_code_path,
            old_pipeline_id,
            new_pipeline_id,
            old_name,
            new_name
        )
    
    update_pbt_project_yml(project_path, old_pipeline_id, new_pipeline_id, new_name)
    old_package_name_for_jobs = None
    new_package_name_for_jobs = None
    
    if unsafe:
        old_package_name_for_jobs = old_identifiers['package_name'].split('.')[-1]
        new_package_name_for_jobs = new_identifiers['package_name'].split('.')[-1]
    else:
        setup_py_path = os.path.join(new_pipeline_code_path, 'setup.py')
        if os.path.exists(setup_py_path):
            try:
                with open(setup_py_path, 'r') as f:
                    setup_content = f.read()
                    name_match = re.search(r"name\s*=\s*['\"]([^'\"]+)['\"]", setup_content)
                    if name_match:
                        package_name = name_match.group(1)
                        old_package_name_for_jobs = package_name
                        new_package_name_for_jobs = package_name
            except Exception:
                pass
    
    update_job_json_files(
        project_path,
        old_pipeline_id,
        new_pipeline_id,
        old_name,
        new_name,
        old_package_name_for_jobs,
        new_package_name_for_jobs
    )

    log(f"Pipeline rename completed successfully: {old_name} -> {new_name}")

