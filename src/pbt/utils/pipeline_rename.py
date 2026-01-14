import json
import os
import re
import shutil
import time
from typing import Dict, List, Optional, Tuple
import yaml

from rich import print
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..utility import custom_print as log, python_pipeline_name
from ..utils.constants import PBT_FILE_NAME


class PipelineRenameError(Exception):
    pass


class PipelineNotFoundError(PipelineRenameError):
    pass


class PipelineAlreadyExistsError(PipelineRenameError):
    pass


class FileOperationError(PipelineRenameError):
    pass


class ValidationError(PipelineRenameError):
    pass


def _detect_json_format_style(original_content: str) -> dict:
    style = {
        "space_before_colon": False,
        "space_in_empty_object": False,
        "space_in_empty_array": False,
        "space_after_colon_before_array": True,  # Default to True (most common)
        "space_after_colon_before_object": True,  # Default to True (most common)
        "single_line_arrays": False,
    }

    if re.search(r'"[^"]+"\s+:', original_content):
        style["space_before_colon"] = True

    if re.search(r"\{\s+\}", original_content):
        style["space_in_empty_object"] = True

    if re.search(r"\[\s+\]", original_content):
        style["space_in_empty_array"] = True

    if re.search(r":\s*\[", original_content):
        if re.search(r":\[", original_content) and not re.search(r":\s+\[", original_content):
            style["space_after_colon_before_array"] = False

    if re.search(r":\s*\{", original_content):
        if re.search(r":\{", original_content) and not re.search(r":\s+\{", original_content):
            style["space_after_colon_before_object"] = False

    if re.search(r':\s*\[\s*"[^"]+"\s*\]', original_content):
        style["single_line_arrays"] = True

    return style


def _format_json_preserve_structure(data: dict, original_content: str, format_style: Optional[dict] = None) -> str:
    original_indent = 2
    lines = original_content.split("\n")
    for line in lines[:20]:
        if line.strip() and not line.strip().startswith(("}", "]", ",")):
            stripped = line.lstrip(" \t")
            indent_str = line[: len(line) - len(stripped)]
            if indent_str:
                original_indent = len(indent_str.replace("\t", "    "))
                break

    # Format with indent
    formatted = json.dumps(data, indent=original_indent, ensure_ascii=False, sort_keys=False)

    if format_style:
        if format_style.get("space_before_colon", False):
            formatted = formatted.replace('": ', '" : ')
            formatted = re.sub(r'([^"\s]): ', r"\1 : ", formatted)

        if not format_style.get("space_after_colon_before_array", True):
            formatted = re.sub(r'":\s+\[', r'":[', formatted)
        if not format_style.get("space_after_colon_before_object", True):
            formatted = re.sub(r'":\s+\{', r'":{', formatted)

        if format_style.get("space_in_empty_object", False):
            formatted = re.sub(r"(:\s*)\{\}([,}\]]|\s*$)", r"\1{ }\2", formatted, flags=re.MULTILINE)
            formatted = re.sub(r"(,\s*)\{\}([,}\]]|\s*$)", r"\1{ }\2", formatted, flags=re.MULTILINE)

        if format_style.get("space_in_empty_array", False):
            formatted = re.sub(r"(:\s*)\[\]([,}\]]|\s*$)", r"\1[ ]\2", formatted, flags=re.MULTILINE)
            formatted = re.sub(r"(,\s*)\[\]([,}\]]|\s*$)", r"\1[ ]\2", formatted, flags=re.MULTILINE)

    if format_style and format_style.get("single_line_arrays", False):
        formatted = re.sub(r'(\[\s*)\n(\s+)("[^"]+")\n(\s+)\](\s*),?', r"\1\3 \4]\5", formatted)
    has_compact_array_object = re.search(r"\[\s*\{", original_content) or re.search(r"\}\s*\]", original_content)

    if has_compact_array_object:
        has_space_in_compact = re.search(r"\[\s+\{", original_content)
        compact_space = " " if has_space_in_compact else ""
        formatted = re.sub(r"(\[\s*)\n(\s+)\{", rf"\1{compact_space}{{", formatted)
        has_space_in_close = re.search(r"\}\s+\]", original_content)
        close_space = " " if has_space_in_close else ""
        formatted = re.sub(r"\}\n(\s+)\](\s*),?", rf"}}{close_space}]", formatted)
        formatted = re.sub(r"\}\n(\s+)\](\s*)$", rf"}}{close_space}]", formatted, flags=re.MULTILINE)

    lines = formatted.split("\n")
    cleaned_lines = [line.rstrip() for line in lines]
    return "\n".join(cleaned_lines)


def _format_json(data: dict, indent: int = 2, format_style: Optional[dict] = None) -> str:
    formatted = json.dumps(data, indent=indent, ensure_ascii=False, sort_keys=False)

    if format_style:
        if format_style.get("space_before_colon", False):
            formatted = formatted.replace('": ', '" : ')
            formatted = re.sub(r'([^"\s]): ', r"\1 : ", formatted)
        if format_style.get("space_in_empty_object", False):
            formatted = formatted.replace("{}", "{ }")
        if format_style.get("space_in_empty_array", False):
            formatted = formatted.replace("[]", "[ ]")

    lines = formatted.split("\n")
    cleaned_lines = [line.rstrip() for line in lines]
    return "\n".join(cleaned_lines)


def _detect_line_ending(content: str) -> str:
    if not content:
        return "\n"
    if "\r\n" in content:
        return "\r\n"
    elif "\r" in content:
        return "\r"
    else:
        return "\n"


@retry(
    retry=retry_if_exception_type((IOError, OSError, PermissionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    reraise=True,
)
def _read_file_with_retry(file_path: str, mode: str = "r", encoding: str = "utf-8"):
    try:
        with open(file_path, mode, encoding=encoding if "b" not in mode else None) as f:
            return f.read()
    except (IOError, OSError, PermissionError) as e:
        raise FileOperationError(
            f"Failed to read file '{file_path}' after retries. "
            f"Error: {str(e)}. "
            f"Please check file permissions and ensure the file is not locked by another process."
        ) from e


@retry(
    retry=retry_if_exception_type((IOError, OSError, PermissionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    reraise=True,
)
def _write_file_with_retry(
    file_path: str, content: str, mode: str = "w", encoding: str = "utf-8", preserve_line_ending: Optional[str] = None
):
    try:
        if preserve_line_ending is None and os.path.exists(file_path):
            try:
                original_content = _read_file_with_retry(file_path, mode="r", encoding=encoding)
                line_ending = _detect_line_ending(original_content)
            except Exception:
                line_ending = "\n"
        elif preserve_line_ending is not None:
            line_ending = preserve_line_ending
        else:
            line_ending = "\n"
        content_normalized = content.replace("\r\n", "\n").replace("\r", "\n")
        if line_ending != "\n":
            content = content_normalized.replace("\n", line_ending)
        else:
            content = content_normalized

        with open(file_path, mode, encoding=encoding if "b" not in mode else None, newline="") as f:
            f.write(content)
    except (IOError, OSError, PermissionError) as e:
        raise FileOperationError(
            f"Failed to write file '{file_path}' after retries. "
            f"Error: {str(e)}. "
            f"Please check file permissions and ensure the file is not locked by another process."
        ) from e


@retry(
    retry=retry_if_exception_type((IOError, OSError, PermissionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    reraise=True,
)
def _move_with_retry(old_path: str, new_path: str):
    try:
        shutil.move(old_path, new_path)
    except (IOError, OSError, PermissionError) as e:
        raise FileOperationError(
            f"Failed to move '{old_path}' to '{new_path}' after retries. "
            f"Error: {str(e)}. "
            f"Please check file permissions and ensure the paths are not locked by another process."
        ) from e


def get_pipeline_identifiers(workflow_path: str) -> Dict[str, str]:
    if not os.path.exists(workflow_path):
        raise PipelineNotFoundError(
            f"Workflow file not found: {workflow_path}\n"
            f"ACTION: Ensure the pipeline directory exists and contains a '.prophecy/workflow.latest.json' file."
        )

    try:
        content = _read_file_with_retry(workflow_path)
        workflow = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValidationError(
            f"Invalid JSON in workflow file: {workflow_path}\n"
            f"Error: {str(e)}\n"
            f"ACTION: Check if the file is corrupted. You may need to restore it from version control or backup."
        ) from e
    except Exception as e:
        raise FileOperationError(
            f"Failed to read workflow file: {workflow_path}\n"
            f"Error: {str(e)}\n"
            f"ACTION: Check file permissions and ensure the file is accessible."
        ) from e

    metainfo = workflow.get("metainfo", {})
    pipeline_settings = metainfo.get("pipelineSettingsInfo", {})

    return {
        "package_name": metainfo.get("topLevelPackage", ""),
        "config_package_name": metainfo.get("configTopLevelPackage", ""),
        "application_name": pipeline_settings.get("applicationName", ""),
        "uri": metainfo.get("uri", ""),
    }


def derive_new_identifiers(new_pipeline_name: str) -> Dict[str, str]:
    safe_name = python_pipeline_name(new_pipeline_name)
    package_suffix = safe_name
    app_suffix = safe_name if not safe_name.endswith("App") else safe_name[:-3]
    return {
        "package_name": f"io.prophecy.pipe.{package_suffix}",
        "config_package_name": f"io.prophecy.config.{package_suffix}",
        "application_name": f"io.prophecy.{app_suffix}App",
        "new_pipeline_id": f"pipelines/{new_pipeline_name}",
    }


def find_pipeline_in_project_config(project_config: dict, pipeline_name: str) -> Optional[str]:
    pipelines = project_config.get("pipelines", {})
    pipeline_id = f"pipelines/{pipeline_name}"

    if pipeline_id in pipelines:
        return pipeline_id
    for pid, pipeline_data in pipelines.items():
        if isinstance(pipeline_data, dict) and pipeline_data.get("name") == pipeline_name:
            return pid

    return None


def _validate_pipeline_name(name: str, field_name: str = "Pipeline name") -> None:
    if not name:
        raise ValidationError(f"{field_name} cannot be empty.\n" f"  ACTION: Provide a valid {field_name.lower()}.")

    name_part = name.split("/")[-1] if "/" in name else name

    if not re.match(r"^[A-Za-z0-9_-]+$", name_part):
        invalid_chars = set(re.findall(r"[^A-Za-z0-9_-]", name_part))
        raise ValidationError(
            f"Invalid {field_name.lower()}: '{name_part}'\n"
            f"  Allowed characters: alphanumeric [A-Z, a-z, 0-9], underscore (_), and hyphen (-)\n"
            f"  Found invalid characters: {', '.join(repr(c) for c in sorted(invalid_chars))}\n"
            f"  ACTION: Rename to use only allowed characters."
        )


def find_pipeline_by_id_or_name(
    project_config: dict, pipeline_id: Optional[str] = None, pipeline_name: Optional[str] = None
) -> Tuple[Optional[str], Optional[str], Optional[dict]]:
    pipelines = project_config.get("pipelines", {})

    if pipeline_id:
        full_id = f"pipelines/{pipeline_id}" if not pipeline_id.startswith("pipelines/") else pipeline_id
        if full_id in pipelines:
            pipeline_data = pipelines[full_id]
            pipeline_name_from_config = pipeline_data.get("name") if isinstance(pipeline_data, dict) else None
            return full_id, pipeline_name_from_config, pipeline_data

    if pipeline_name:
        for pid, pdata in pipelines.items():
            if isinstance(pdata, dict) and pdata.get("name") == pipeline_name:
                return pid, pipeline_name, pdata

    return None, None, None


def validate_sync(
    project_path: str, pipeline_id: Optional[str] = None, pipeline_name: Optional[str] = None
) -> Tuple[str, str, str]:
    if not pipeline_id and not pipeline_name:
        raise ValidationError(
            "Either --pipeline-id or --pipeline-name must be provided.\n"
            f"  ACTION: Specify either --pipeline-id <id> or --pipeline-name <name> to identify the pipeline to sync."
        )

    if pipeline_id and pipeline_name:
        raise ValidationError(
            "Cannot specify both --pipeline-id and --pipeline-name.\n"
            f"  ACTION: Use either --pipeline-id <id> or --pipeline-name <name>, not both."
        )

    if pipeline_id:
        _validate_pipeline_name(pipeline_id, "Pipeline ID")
    if pipeline_name:
        _validate_pipeline_name(pipeline_name, "Pipeline name")

    pbt_project_file = os.path.join(project_path, PBT_FILE_NAME)
    if not os.path.exists(pbt_project_file):
        raise ValidationError(
            f"Project file not found: {pbt_project_file}\n"
            f"  ACTION: Ensure you're running the command from the project root directory containing {PBT_FILE_NAME}."
        )

    try:
        content = _read_file_with_retry(pbt_project_file)
        project_config = yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise ValidationError(
            f"Invalid YAML in {PBT_FILE_NAME}.\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Fix the YAML syntax errors in {pbt_project_file} before syncing."
        ) from e
    except Exception as e:
        raise FileOperationError(
            f"Failed to read {PBT_FILE_NAME}.\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check file permissions and ensure the file is accessible."
        ) from e

    if not project_config:
        raise ValidationError(
            f"Empty or invalid {PBT_FILE_NAME}.\n"
            f"  ACTION: Ensure {PBT_FILE_NAME} contains valid project configuration."
        )

    # Find pipeline by ID or name
    found_id, found_name, pipeline_data = find_pipeline_by_id_or_name(project_config, pipeline_id, pipeline_name)

    if not found_id:
        pipelines = project_config.get("pipelines", {})
        if pipeline_id:
            for pid, pdata in pipelines.items():
                if isinstance(pdata, dict) and pdata.get("name") == pipeline_id:
                    raise PipelineNotFoundError(
                        f"Pipeline ID 'pipelines/{pipeline_id}' not found in {PBT_FILE_NAME}.\n"
                        f"  However, a pipeline with name '{pipeline_id}' exists with ID '{pid}'.\n"
                        f"  ACTION: Use --pipeline-name {pipeline_id} instead, or verify the pipeline ID is correct."
                    )
        elif pipeline_name:
            name_as_id = f"pipelines/{pipeline_name}"
            if name_as_id in pipelines:
                raise PipelineNotFoundError(
                    f"Pipeline name '{pipeline_name}' not found in {PBT_FILE_NAME}.\n"
                    f"  However, a pipeline with ID '{name_as_id}' exists.\n"
                    f"  ACTION: Use --pipeline-id {pipeline_name} instead, or verify the pipeline name is correct."
                )

        raise PipelineNotFoundError(
            f"Pipeline not found in {PBT_FILE_NAME}.\n"
            f"  Searched for: {'ID' if pipeline_id else 'name'} = '{pipeline_id or pipeline_name}'\n"
            f"  ACTION: Verify the pipeline exists in {PBT_FILE_NAME} and check both the pipeline ID and 'name' field."
        )

    target_pipeline_name = found_name
    if not target_pipeline_name:
        raise ValidationError(
            f"Pipeline '{found_id}' found but has no 'name' field in {PBT_FILE_NAME}.\n"
            f"  ACTION: Ensure the pipeline entry has a 'name' field."
        )

    # Validate the target pipeline name from config
    _validate_pipeline_name(target_pipeline_name, "Pipeline name (from config)")

    current_directory_name = found_id.split("/")[-1] if "/" in found_id else found_id

    if current_directory_name == target_pipeline_name:
        raise ValidationError(
            f"Pipeline is already synced.\n"
            f"  Pipeline ID: {found_id}\n"
            f"  Directory name: {current_directory_name}\n"
            f"  Pipeline name: {target_pipeline_name}\n"
            f"  ACTION: No sync needed - directory name and pipeline name already match."
        )

    # Check if target directory already exists (different pipeline with target name)
    target_directory_path = os.path.join(project_path, "pipelines", target_pipeline_name)
    if os.path.exists(target_directory_path) and current_directory_name != target_pipeline_name:
        raise PipelineAlreadyExistsError(
            f"Target directory '{target_pipeline_name}' already exists.\n"
            f"  Location: {target_directory_path}\n"
            f"  ACTION: Delete or rename the existing directory first, or choose a different pipeline name."
        )

    return found_id, current_directory_name, target_pipeline_name


def validate_package_scoping(
    project_path: str, target_pipeline_name: str, old_package_name: str
) -> Tuple[bool, Optional[str]]:
    pipelines_dir = os.path.join(project_path, "pipelines")

    if not os.path.exists(pipelines_dir):
        return True, None

    conflicting_pipelines = []

    for pipeline_dir in os.listdir(pipelines_dir):
        if pipeline_dir == target_pipeline_name:
            continue

        pipeline_path = os.path.join(pipelines_dir, pipeline_dir)
        if not os.path.isdir(pipeline_path):
            continue

        workflow_path = os.path.join(pipeline_path, "code", ".prophecy", "workflow.latest.json")
        if os.path.exists(workflow_path):
            try:
                identifiers = get_pipeline_identifiers(workflow_path)
                other_package = identifiers.get("package_name", "")
                if old_package_name == other_package:
                    conflicting_pipelines.append(pipeline_dir)
                elif old_package_name in other_package or other_package in old_package_name:
                    log(
                        f"Warning: Package name '{old_package_name}' may overlap with pipeline '{pipeline_dir}' (package: '{other_package}')"
                    )
            except Exception:
                continue

    if conflicting_pipelines:
        return (
            False,
            f"Package name '{old_package_name}' is also used by other pipelines: {', '.join(conflicting_pipelines)}",
        )

    return True, None


def rename_pipeline_directory(project_path: str, current_name: str, new_name: str) -> None:
    current_path = os.path.join(project_path, "pipelines", current_name)
    new_path = os.path.join(project_path, "pipelines", new_name)

    if not os.path.exists(current_path):
        raise PipelineNotFoundError(
            f"Pipeline directory not found: {current_path}\n"
            f"  ACTION: Verify the pipeline name is correct and the directory exists."
        )

    if os.path.exists(new_path):
        raise PipelineAlreadyExistsError(
            f"Target directory already exists: {new_path}\n" f"  ACTION: Delete or rename the existing directory first."
        )

    try:
        log(f"Renaming directory: {current_path} -> {new_path}")
        _move_with_retry(current_path, new_path)
    except PermissionError as e:
        raise FileOperationError(
            f"Permission denied while renaming directory.\n"
            f"  Source: {current_path}\n"
            f"  Target: {new_path}\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check file permissions. On Unix systems, ensure you have write access to the parent directory."
        ) from e


def rename_package_directories(pipeline_code_path: str, current_package_name: str, new_package_name: str) -> None:
    current_package_path = os.path.join(pipeline_code_path, *current_package_name.split("."))
    new_package_path = os.path.join(pipeline_code_path, *new_package_name.split("."))

    if os.path.exists(current_package_path):
        try:
            log(f"Renaming package directory: {current_package_path} -> {new_package_path}")
            os.makedirs(os.path.dirname(new_package_path), exist_ok=True)
            _move_with_retry(current_package_path, new_package_path)
        except PermissionError as e:
            raise FileOperationError(
                f"Permission denied while renaming package directory.\n"
                f"  Source: {current_package_path}\n"
                f"  Target: {new_package_path}\n"
                f"  Error: {str(e)}\n"
                f"  ACTION: Check file permissions and ensure you have write access."
            ) from e

    current_config_package = current_package_name.replace("io.prophecy.pipe", "io.prophecy.config")
    new_config_package = new_package_name.replace("io.prophecy.pipe", "io.prophecy.config")

    configs_path = os.path.join(pipeline_code_path, "configs", "resources")
    current_config_path = os.path.join(configs_path, *current_config_package.split("."))
    new_config_path = os.path.join(configs_path, *new_config_package.split("."))

    if os.path.exists(current_config_path):
        try:
            log(f"Renaming config package directory: {current_config_path} -> {new_config_path}")
            os.makedirs(os.path.dirname(new_config_path), exist_ok=True)
            _move_with_retry(current_config_path, new_config_path)

            if os.path.isdir(new_config_path):
                for filename in os.listdir(new_config_path):
                    if filename.endswith(".conf") and current_package_name.split(".")[-1] in filename:
                        current_file = os.path.join(new_config_path, filename)
                        new_filename = filename.replace(
                            current_package_name.split(".")[-1], new_package_name.split(".")[-1]
                        )
                        new_file = os.path.join(new_config_path, new_filename)
                        log(f"Renaming config file: {current_file} -> {new_file}")
                        _move_with_retry(current_file, new_file)
        except PermissionError as e:
            raise FileOperationError(
                f"Permission denied while renaming config package directory.\n"
                f"  Source: {current_config_path}\n"
                f"  Target: {new_config_path}\n"
                f"  Error: {str(e)}\n"
                f"  ACTION: Check file permissions and ensure you have write access."
            ) from e


def update_workflow_json(
    workflow_path: str, current_identifiers: Dict[str, str], new_identifiers: Dict[str, str]
) -> None:
    if not os.path.exists(workflow_path):
        log(f"Workflow file not found: {workflow_path}, skipping update")
        return

    try:
        original_content = _read_file_with_retry(workflow_path)
        json.loads(original_content)
        original_line_ending = _detect_line_ending(original_content)
    except json.JSONDecodeError as e:
        raise ValidationError(
            f"Invalid JSON in workflow file: {workflow_path}\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check if the file is corrupted. You may need to restore it from version control."
        ) from e

    content = original_content
    content = re.sub(
        r'("uri"\s*:\s*)"[^"]*"',
        rf'\1"{new_identifiers["new_pipeline_id"]}"',
        content,
    )

    content = re.sub(
        r'("topLevelPackage"\s*:\s*)"[^"]*"',
        rf'\1"{new_identifiers["package_name"]}"',
        content,
    )

    content = re.sub(
        r'("configTopLevelPackage"\s*:\s*)"[^"]*"',
        rf'\1"{new_identifiers["config_package_name"]}"',
        content,
    )

    content = re.sub(
        r'("applicationName"\s*:\s*)"[^"]*"',
        rf'\1"{new_identifiers["application_name"]}"',
        content,
    )

    if content != original_content:
        try:
            _write_file_with_retry(workflow_path, content, preserve_line_ending=original_line_ending)
            log(f"Updated workflow.latest.json with new identifiers")
        except Exception as e:
            raise FileOperationError(
                f"Failed to write workflow file: {workflow_path}\n"
                f"  Error: {str(e)}\n"
                f"  ACTION: Check file permissions and ensure the file is not locked."
            ) from e


def update_workflow_json_uri_only(workflow_path: str, current_pipeline_id: str, new_pipeline_id: str) -> None:
    """Update workflow.latest.json URI only using string replacement to preserve formatting."""
    if not os.path.exists(workflow_path):
        log(f"Workflow file not found: {workflow_path}, skipping update")
        return

    try:
        original_content = _read_file_with_retry(workflow_path)
        json.loads(original_content)
        original_line_ending = _detect_line_ending(original_content)
    except json.JSONDecodeError as e:
        raise ValidationError(
            f"Invalid JSON in workflow file: {workflow_path}\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check if the file is corrupted. You may need to restore it from version control."
        ) from e

    content = original_content

    # Update uri - preserve the original spacing around colon
    content = re.sub(
        r'"uri"\s*:\s*"' + re.escape(current_pipeline_id) + r'"',
        lambda m: m.group(0).replace(current_pipeline_id, new_pipeline_id),
        content,
    )

    if content != original_content:
        try:
            _write_file_with_retry(workflow_path, content, preserve_line_ending=original_line_ending)
            log(f"Updated workflow.latest.json URI: {current_pipeline_id} -> {new_pipeline_id}")
        except Exception as e:
            raise FileOperationError(
                f"Failed to write workflow file: {workflow_path}\n"
                f"  Error: {str(e)}\n"
                f"  ACTION: Check file permissions and ensure the file is not locked."
            ) from e


def update_python_files(
    pipeline_code_path: str,
    current_identifiers: Dict[str, str],
    new_identifiers: Dict[str, str],
    current_pipeline_id: str,
    new_pipeline_id: str,
) -> None:
    current_package = current_identifiers["package_name"]
    new_package = new_identifiers["package_name"]
    current_config_package = current_identifiers["config_package_name"]
    new_config_package = new_identifiers["config_package_name"]
    current_app_name = current_identifiers["application_name"]
    new_app_name = new_identifiers["application_name"]

    for root, dirs, files in os.walk(pipeline_code_path):
        if "build" in root or "dist" in root or "__pycache__" in root:
            continue

        for filename in files:
            if filename.endswith(".py"):
                file_path = os.path.join(root, filename)
                update_python_file(
                    file_path,
                    current_package,
                    new_package,
                    current_config_package,
                    new_config_package,
                    current_app_name,
                    new_app_name,
                    current_pipeline_id,
                    new_pipeline_id,
                )


def update_python_file(
    file_path: str,
    current_package: str,
    new_package: str,
    current_config_package: str,
    new_config_package: str,
    current_app_name: str,
    new_app_name: str,
    current_pipeline_id: str,
    new_pipeline_id: str,
) -> None:
    try:
        content = _read_file_with_retry(file_path, encoding="utf-8")
    except Exception as e:
        raise FileOperationError(
            f"Failed to read Python file: {file_path}\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check file permissions and ensure the file is accessible."
        ) from e

    original_content = content
    current_pipeline_name = current_pipeline_id.split("/")[-1]
    new_pipeline_name = new_pipeline_id.split("/")[-1]

    # IMPORTANT: Update pipeline URIs FIRST, before package name replacement
    # This prevents pipeline URIs from being incorrectly updated during package name replacement
    content = re.sub(r'"pipelines/' + re.escape(current_pipeline_name) + r'"', f'"{new_pipeline_id}"', content)
    content = re.sub(r"'pipelines/" + re.escape(current_pipeline_name) + r"'", f"'{new_pipeline_id}'", content)
    # Handle cases where pipeline URI might have the full package name (incorrect format)
    content = re.sub(r'"pipelines/' + re.escape(current_package) + r'"', f'"{new_pipeline_id}"', content)
    content = re.sub(r"'pipelines/" + re.escape(current_package) + r"'", f"'{new_pipeline_id}'", content)
    # Handle unquoted pipeline ID references
    content = re.sub(r"\b" + re.escape(f"pipelines/{current_pipeline_name}") + r"\b", new_pipeline_id, content)
    content = re.sub(r"\b" + re.escape(f"pipelines/{current_package}") + r"\b", new_pipeline_id, content)

    # Update pipelineId in MetricsCollector.instrument calls
    content = re.sub(
        r'pipelineId\s*=\s*["\']' + re.escape(current_pipeline_id) + r'["\']',
        f'pipelineId = "{new_pipeline_id}"',
        content,
    )
    # Also handle spark.conf.set patterns
    content = re.sub(
        r'spark\.conf\.set\(["\']prophecy\.metadata\.pipeline\.uri["\'],\s*["\']'
        + re.escape(current_pipeline_id)
        + r'["\']\)',
        f'spark.conf.set("prophecy.metadata.pipeline.uri", "{new_pipeline_id}")',
        content,
    )
    content = re.sub(
        r'spark\.conf\.set\(["\']prophecy\.metadata\.pipeline\.uri["\'],\s*["\']pipelines/'
        + re.escape(current_pipeline_name)
        + r'["\']\)',
        f'spark.conf.set("prophecy.metadata.pipeline.uri", "{new_pipeline_id}")',
        content,
    )

    # Now update package names (after pipeline URIs are fixed)
    current_package_escaped = re.escape(current_package)
    current_config_package_escaped = re.escape(current_config_package)
    current_package_pattern = r"(?<![a-zA-Z0-9_.])" + current_package_escaped + r"(?![a-zA-Z0-9_])"
    current_config_package_pattern = r"(?<![a-zA-Z0-9_.])" + current_config_package_escaped + r"(?![a-zA-Z0-9_])"
    content = re.sub(current_package_pattern, new_package, content)
    content = re.sub(current_config_package_pattern, new_config_package, content)

    # Update app names
    current_app_simple = current_app_name.split(".")[-1].replace("App", "")
    new_app_simple = new_app_name.split(".")[-1].replace("App", "")
    content = re.sub(
        r'\.appName\(["\']' + re.escape(current_app_simple) + r'["\']\)', f'.appName("{new_app_simple}")', content
    )
    content = re.sub(
        r'\.appName\(["\']' + re.escape(current_package) + r'["\']\)', f'.appName("{new_app_simple}")', content
    )
    # Fix appName if it contains the full new package name (shouldn't happen, but handle it)
    new_package_escaped = re.escape(new_package)
    content = re.sub(r'\.appName\(["\']' + new_package_escaped + r'["\']\)', f'.appName("{new_app_simple}")', content)

    # Update prophecy_config_instances references
    content = re.sub(
        r"prophecy_config_instances\." + re.escape(current_pipeline_name) + r"(?![a-zA-Z0-9_])",
        f"prophecy_config_instances.{new_pipeline_name}",
        content,
    )

    if os.path.basename(file_path) == "__init__.py":
        content = re.sub(
            r"from\s+\." + re.escape(current_pipeline_name) + r"\s+import", f"from .{new_pipeline_name} import", content
        )

    # Final cleanup: Fix any pipeline URIs that might have been incorrectly updated during package name replacement
    # This is a safety net in case the order of operations didn't work as expected
    content = re.sub(r'"pipelines/' + new_package_escaped + r'"', f'"{new_pipeline_id}"', content)
    content = re.sub(r"'pipelines/" + new_package_escaped + r"'", f"'{new_pipeline_id}'", content)
    content = re.sub(
        r'pipelineId\s*=\s*["\']pipelines/' + new_package_escaped + r'["\']',
        f'pipelineId = "{new_pipeline_id}"',
        content,
    )
    content = re.sub(
        r'spark\.conf\.set\(["\']prophecy\.metadata\.pipeline\.uri["\'],\s*["\']pipelines/'
        + new_package_escaped
        + r'["\']\)',
        f'spark.conf.set("prophecy.metadata.pipeline.uri", "{new_pipeline_id}")',
        content,
    )

    # Update setup.py specific patterns
    if os.path.basename(file_path) == "setup.py":
        # Update package name
        content = re.sub(
            r"name\s*=\s*['\"]" + re.escape(current_pipeline_name) + r"['\"]",
            f'name = "{new_pipeline_id.split("/")[-1]}"',
            content,
        )

        # Update find_packages
        content = re.sub(
            r"find_packages\(include\s*=\s*\('" + re.escape(current_package) + r"\*'",
            f"find_packages(include = ('{new_package}*'",
            content,
        )

        # Update package_dir - handle both package names and prophecy_config_instances
        content = re.sub(
            r"['\"]" + re.escape(current_package) + r"['\"]\s*:\s*['\"][^'\"]+['\"]",
            lambda m: m.group(0).replace(current_package, new_package),
            content,
        )
        content = re.sub(
            r"['\"]prophecy_config_instances\." + re.escape(current_pipeline_name) + r"['\"]\s*:\s*['\"][^'\"]+['\"]",
            lambda m: m.group(0).replace(
                f"prophecy_config_instances.{current_pipeline_name}", f"prophecy_config_instances.{new_pipeline_name}"
            ),
            content,
        )

        # Update package_data - handle both package names and prophecy_config_instances
        content = re.sub(
            r"['\"]" + re.escape(current_package) + r"['\"]\s*:\s*\[[^\]]+\]",
            lambda m: m.group(0).replace(current_package, new_package),
            content,
        )
        content = re.sub(
            r"['\"]prophecy_config_instances\." + re.escape(current_pipeline_name) + r"['\"]\s*:\s*\[[^\]]+\]",
            lambda m: m.group(0).replace(
                f"prophecy_config_instances.{current_pipeline_name}", f"prophecy_config_instances.{new_pipeline_name}"
            ),
            content,
        )

        # Update packages list - handle prophecy_config_instances.{current_name}
        content = re.sub(
            r"\['prophecy_config_instances\." + re.escape(current_pipeline_name) + r"'\]",
            f"['prophecy_config_instances.{new_pipeline_name}']",
            content,
        )
        content = re.sub(
            r'\["prophecy_config_instances\.' + re.escape(current_pipeline_name) + r'"\]',
            f'["prophecy_config_instances.{new_pipeline_name}"]',
            content,
        )

        # Update entry points
        content = re.sub(
            r"['\"]main\s*=\s*" + re.escape(current_package) + r"\.pipeline:main['\"]",
            f"'main = {new_package}.pipeline:main'",
            content,
        )

    if content != original_content:
        try:
            _write_file_with_retry(file_path, content, encoding="utf-8")
            log(f"Updated Python file: {file_path}")
        except Exception as e:
            raise FileOperationError(
                f"Failed to write Python file: {file_path}\n"
                f"  Error: {str(e)}\n"
                f"  ACTION: Check file permissions and ensure the file is not locked."
            ) from e


def update_python_files_pipeline_id_only(
    pipeline_code_path: str, current_pipeline_id: str, new_pipeline_id: str, current_name: str, new_name: str
) -> None:
    for root, dirs, files in os.walk(pipeline_code_path):
        # Skip build and dist directories
        if "build" in root or "dist" in root or "__pycache__" in root:
            continue

        for filename in files:
            if filename.endswith(".py"):
                file_path = os.path.join(root, filename)
                update_python_file_pipeline_id_only(
                    file_path, current_pipeline_id, new_pipeline_id, current_name, new_name
                )


def update_python_file_pipeline_id_only(
    file_path: str, current_pipeline_id: str, new_pipeline_id: str, current_name: str, new_name: str
) -> None:
    try:
        content = _read_file_with_retry(file_path, encoding="utf-8")
    except Exception as e:
        raise FileOperationError(
            f"Failed to read Python file: {file_path}\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check file permissions and ensure the file is accessible."
        ) from e

    original_content = content
    content = re.sub(r'"pipelines/' + re.escape(current_name) + r'"', f'"{new_pipeline_id}"', content)
    content = re.sub(r"'pipelines/" + re.escape(current_name) + r"'", f"'{new_pipeline_id}'", content)
    # Then handle unquoted references (with word boundary to avoid substring matches)
    content = re.sub(r"\b" + re.escape(f"pipelines/{current_name}") + r"\b", new_pipeline_id, content)

    # Update pipelineId in MetricsCollector.instrument calls
    content = re.sub(
        r'pipelineId\s*=\s*["\']' + re.escape(current_pipeline_id) + r'["\']',
        f'pipelineId = "{new_pipeline_id}"',
        content,
    )

    # Update .appName() calls - only the pipeline name part
    content = re.sub(r'\.appName\(["\']' + re.escape(current_name) + r'["\']\)', f'.appName("{new_name}")', content)

    # Update spark.conf.set for pipeline URI
    content = re.sub(
        r'spark\.conf\.set\(["\']prophecy\.metadata\.pipeline\.uri["\'],\s*["\']'
        + re.escape(current_pipeline_id)
        + r'["\']\)',
        f'spark.conf.set("prophecy.metadata.pipeline.uri", "{new_pipeline_id}")',
        content,
    )
    # Also handle cases where URI might use just the pipeline name
    content = re.sub(
        r'spark\.conf\.set\(["\']prophecy\.metadata\.pipeline\.uri["\'],\s*["\']pipelines/'
        + re.escape(current_name)
        + r'["\']\)',
        f'spark.conf.set("prophecy.metadata.pipeline.uri", "{new_pipeline_id}")',
        content,
    )

    if content != original_content:
        try:
            _write_file_with_retry(file_path, content, encoding="utf-8")
            log(f"Updated Python file: {file_path}")
        except Exception as e:
            raise FileOperationError(
                f"Failed to write Python file: {file_path}\n"
                f"  Error: {str(e)}\n"
                f"  ACTION: Check file permissions and ensure the file is not locked."
            ) from e


def update_pbt_project_yml(
    project_path: str, current_pipeline_id: str, new_pipeline_id: str, new_pipeline_name: str
) -> None:
    pbt_project_file = os.path.join(project_path, PBT_FILE_NAME)

    if not os.path.exists(pbt_project_file):
        raise ValidationError(
            f"Project file not found: {pbt_project_file}\n"
            f"  ACTION: Ensure you're running the command from the project root directory containing {PBT_FILE_NAME}."
        )

    # Read and parse YAML
    try:
        original_content = _read_file_with_retry(pbt_project_file)
        project_config = yaml.safe_load(original_content)
        original_line_ending = _detect_line_ending(original_content)
    except yaml.YAMLError as e:
        raise ValidationError(
            f"Invalid YAML in {PBT_FILE_NAME}.\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Fix the YAML syntax errors in {pbt_project_file} before renaming."
        ) from e
    except Exception as e:
        raise FileOperationError(
            f"Failed to read {PBT_FILE_NAME}.\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check file permissions and ensure the file is accessible."
        ) from e

    if not project_config:
        raise ValidationError(
            f"Empty or invalid {PBT_FILE_NAME}.\n"
            f"  ACTION: Ensure {PBT_FILE_NAME} contains valid project configuration."
        )

    # Update pipeline entry - only update the specific pipeline
    pipelines = project_config.get("pipelines", {})
    # Try to find by ID first, then by name
    current_pipeline_id_found = current_pipeline_id if current_pipeline_id in pipelines else None
    if not current_pipeline_id_found:
        # Search by name field
        for pid, pipeline_data in pipelines.items():
            if isinstance(pipeline_data, dict) and pipeline_data.get("name") == current_pipeline_id.split("/")[-1]:
                current_pipeline_id_found = pid
                break

    if not current_pipeline_id_found:
        raise ValidationError(
            f"Pipeline '{current_pipeline_id}' not found in {PBT_FILE_NAME}.\n"
            f"  ACTION: Ensure the pipeline is registered in {PBT_FILE_NAME}. "
            f"Check both the pipeline ID and the 'name' field."
        )

    # Move pipeline entry to new key
    pipeline_data = pipelines[current_pipeline_id_found]
    del pipelines[current_pipeline_id_found]
    pipelines[new_pipeline_id] = pipeline_data

    # Update pipeline name field
    if isinstance(pipeline_data, dict):
        pipeline_data["name"] = new_pipeline_name

    # Update job references - only update references to this specific pipeline
    jobs = project_config.get("jobs", {})
    for job_id, job_data in jobs.items():
        if isinstance(job_data, dict):
            # Update pipelines list if it exists
            if "pipelines" in job_data and isinstance(job_data["pipelines"], list):
                job_data["pipelines"] = [
                    new_pipeline_id if pipeline_id == current_pipeline_id_found else pipeline_id
                    for pipeline_id in job_data["pipelines"]
                ]

    # Write back to file, preserving YAML structure and formatting
    try:
        yaml_content = yaml.dump(
            project_config, default_flow_style=False, sort_keys=False, allow_unicode=True, width=float("inf")
        )
        _write_file_with_retry(pbt_project_file, yaml_content, preserve_line_ending=original_line_ending)
        log(f"Updated {PBT_FILE_NAME}: {current_pipeline_id_found} -> {new_pipeline_id}")
    except Exception as e:
        raise FileOperationError(
            f"Failed to write {PBT_FILE_NAME}.\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check file permissions and ensure the file is not locked."
        ) from e


def validate_current_name_removed(project_path: str, new_name: str, current_pipeline_id: str) -> Tuple[bool, List[str]]:
    files_with_current_name = []
    current_pipeline_name = current_pipeline_id.split("/")[-1] if "/" in current_pipeline_id else current_pipeline_id
    new_pipeline_id = f"pipelines/{new_name}"

    # Search in pipeline code directory (after rename, it's now new_name)
    new_pipeline_code_path = os.path.join(project_path, "pipelines", new_name, "code")
    if os.path.exists(new_pipeline_code_path):
        for root, dirs, files in os.walk(new_pipeline_code_path):
            if "build" in root or "dist" in root or "__pycache__" in root:
                continue

            for filename in files:
                if filename.endswith((".py", ".json", ".yml", ".yaml")):
                    file_path = os.path.join(root, filename)
                    try:
                        content = _read_file_with_retry(file_path)
                        if filename == "workflow.latest.json":
                            import json

                            try:
                                data = json.loads(content)
                                metainfo = data.get("metainfo", {})
                                uri = metainfo.get("uri", "")
                                if uri == current_pipeline_id or (
                                    current_pipeline_id in uri and new_pipeline_id not in uri
                                ):
                                    files_with_current_name.append(file_path)
                            except Exception:
                                if current_pipeline_id in content and new_pipeline_id not in content:
                                    files_with_current_name.append(file_path)
                        elif filename.endswith(".py"):
                            import re

                            pipeline_id_pattern = re.compile(
                                r'["\']pipelines/' + re.escape(current_pipeline_name) + r'["\']'
                            )
                            if pipeline_id_pattern.search(content) and new_pipeline_id not in content:
                                files_with_current_name.append(file_path)
                            elif (
                                re.search(r"\b" + re.escape(current_pipeline_id) + r"\b", content)
                                and new_pipeline_id not in content
                            ):
                                files_with_current_name.append(file_path)
                        else:
                            if current_pipeline_id in content and new_pipeline_id not in content:
                                files_with_current_name.append(file_path)
                    except Exception:
                        continue

    # Search in job directories
    jobs_path = os.path.join(project_path, "jobs")
    if os.path.exists(jobs_path):
        for root, dirs, files in os.walk(jobs_path):
            if "build" in root or "dist" in root or "__pycache__" in root:
                continue

            for filename in files:
                if filename.endswith((".py", ".json")):
                    file_path = os.path.join(root, filename)
                    try:
                        content = _read_file_with_retry(file_path)
                        if filename.endswith(".json"):
                            try:
                                data = json.loads(content)
                                has_old_reference = False

                                if "processes" in data:
                                    for process_value in data["processes"].values():
                                        if isinstance(process_value, dict) and "properties" in process_value:
                                            pipeline_id = process_value["properties"].get("pipelineId")
                                            if isinstance(pipeline_id, dict) and "value" in pipeline_id:
                                                pipeline_id = pipeline_id["value"]
                                            if isinstance(pipeline_id, str):
                                                if pipeline_id == current_pipeline_id or (
                                                    current_pipeline_id in pipeline_id
                                                    and new_pipeline_id not in pipeline_id
                                                ):
                                                    has_old_reference = True
                                                    break

                                if "components" in data:
                                    for component in data["components"]:
                                        if "PipelineComponent" in component:
                                            pc = component["PipelineComponent"]
                                            pipeline_id = pc.get("pipelineId")
                                            if isinstance(pipeline_id, str):
                                                if pipeline_id == current_pipeline_id or (
                                                    current_pipeline_id in pipeline_id
                                                    and new_pipeline_id not in pipeline_id
                                                ):
                                                    has_old_reference = True
                                                    break

                                    if "request" in data:
                                        request = data["request"]
                                        if "CreateNewJobRequest" in request:
                                            request = request["CreateNewJobRequest"]
                                        if "tasks" in request:
                                            for task in request["tasks"]:
                                                if "libraries" in task:
                                                    for library in task["libraries"]:
                                                        if "PipelineComponent" in library:
                                                            pipeline_id = library["PipelineComponent"].get("pipelineId")
                                                            if isinstance(pipeline_id, str):
                                                                if pipeline_id == current_pipeline_id or (
                                                                    current_pipeline_id in pipeline_id
                                                                    and new_pipeline_id not in pipeline_id
                                                                ):
                                                                    has_old_reference = True
                                                                    break

                                if has_old_reference:
                                    files_with_current_name.append(file_path)
                            except (json.JSONDecodeError, Exception):
                                # Fallback to simple check if JSON parsing fails
                                if current_pipeline_id in content and new_pipeline_id not in content:
                                    files_with_current_name.append(file_path)
                        else:
                            import re

                            pipeline_id_pattern = re.compile(r'["\']' + re.escape(current_pipeline_id) + r'["\']')
                            if pipeline_id_pattern.search(content) and new_pipeline_id not in content:
                                files_with_current_name.append(file_path)
                    except Exception:
                        continue

    return len(files_with_current_name) == 0, files_with_current_name


def update_job_json_files(
    project_path: str,
    current_pipeline_id: str,
    new_pipeline_id: str,
    current_name: str = None,
    new_name: str = None,
    current_package_name: str = None,
    new_package_name: str = None,
) -> None:
    jobs_path = os.path.join(project_path, "jobs")

    if not os.path.exists(jobs_path):
        return

    for root, dirs, files in os.walk(jobs_path):
        for filename in files:
            if filename in ["prophecy-job.json", "databricks-job.json"]:
                json_file_path = os.path.join(root, filename)
                update_json_file(
                    json_file_path,
                    current_pipeline_id,
                    new_pipeline_id,
                    current_name,
                    new_name,
                    current_package_name,
                    new_package_name,
                )

    for root, dirs, files in os.walk(jobs_path):
        if "build" in root or "dist" in root or "__pycache__" in root:
            continue

        for filename in files:
            if filename.endswith(".py"):
                file_path = os.path.join(root, filename)
                update_job_python_file(
                    file_path,
                    current_pipeline_id,
                    new_pipeline_id,
                    current_name,
                    new_name,
                    current_package_name,
                    new_package_name,
                )


def update_job_python_file(
    file_path: str,
    current_pipeline_id: str,
    new_pipeline_id: str,
    current_name: str = None,
    new_name: str = None,
    current_package_name: str = None,
    new_package_name: str = None,
) -> None:
    try:
        content = _read_file_with_retry(file_path, encoding="utf-8")
    except Exception as e:
        raise FileOperationError(
            f"Failed to read job Python file: {file_path}\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check file permissions and ensure the file is accessible."
        ) from e

    original_content = content

    current_pipeline_name = current_pipeline_id.split("/")[-1] if "/" in current_pipeline_id else current_pipeline_id
    new_pipeline_name = new_pipeline_id.split("/")[-1] if "/" in new_pipeline_id else new_pipeline_id

    current_name_to_use = current_name if current_name else current_pipeline_name
    new_name_to_use = new_name if new_name else new_pipeline_name

    content = re.sub(r'"pipelines/' + re.escape(current_name_to_use) + r'":', f'"{new_pipeline_id}":', content)
    content = re.sub(r"'pipelines/" + re.escape(current_name_to_use) + r"':", f"'{new_pipeline_id}':", content)

    # Update pipeline package name in dictionaries
    if current_package_name and new_package_name:
        content = re.sub(
            r'"pipelines/'
            + re.escape(current_name_to_use)
            + r'"\s*:\s*["\']'
            + re.escape(current_package_name)
            + r'["\']',
            f'"{new_pipeline_id}": "{new_package_name}"',
            content,
        )
        content = re.sub(
            r"'pipelines/"
            + re.escape(current_name_to_use)
            + r"'\s*:\s*['\"]"
            + re.escape(current_package_name)
            + r"['\"]",
            f"'{new_pipeline_id}': '{new_package_name}'",
            content,
        )

    # Update wheel file paths
    if current_package_name and new_package_name:
        content = re.sub(
            r'dbfs:/[^"\']*' + re.escape(current_package_name) + r'[^"\']*\.whl',
            lambda m: m.group(0).replace(current_package_name, new_package_name),
            content,
        )
    # Also update if path contains current pipeline name
    content = re.sub(
        r'dbfs:/[^"\']*' + re.escape(current_name_to_use) + r'[^"\']*\.whl',
        lambda m: m.group(0).replace(current_name_to_use, new_name_to_use),
        content,
    )

    content = re.sub(
        r'["\']' + re.escape(current_pipeline_id) + r'["\']',
        f'"{new_pipeline_id}"',
        content,
    )
    content = re.sub(
        r'["\']' + re.escape(f"pipelines/{current_name_to_use}") + r'["\']',
        f'"{new_pipeline_id}"',
        content,
    )

    content = re.sub(
        r'["\']' + re.escape(current_name_to_use) + r'["\']',
        f'"{new_name_to_use}"',
        content,
    )

    content = re.sub(
        r"\b" + re.escape(current_name_to_use) + r"\b",
        new_name_to_use,
        content,
    )

    if content != original_content:
        try:
            _write_file_with_retry(file_path, content, encoding="utf-8")
            log(f"Updated job Python file: {file_path}")
        except Exception as e:
            raise FileOperationError(
                f"Failed to write job Python file: {file_path}\n"
                f"  Error: {str(e)}\n"
                f"  ACTION: Check file permissions and ensure the file is not locked."
            ) from e


def update_json_file(
    json_file_path: str,
    current_pipeline_id: str,
    new_pipeline_id: str,
    current_name: str = None,
    new_name: str = None,
    current_package_name: str = None,
    new_package_name: str = None,
) -> None:
    try:
        original_content = _read_file_with_retry(json_file_path)
        # Parse JSON to validate it's valid, but we'll update via string replacement
        data = json.loads(original_content)
        original_line_ending = _detect_line_ending(original_content)
    except json.JSONDecodeError as e:
        raise ValidationError(
            f"Invalid JSON in file: {json_file_path}\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check if the file is corrupted. You may need to restore it from version control."
        ) from e
    except Exception as e:
        raise FileOperationError(
            f"Failed to read JSON file: {json_file_path}\n"
            f"  Error: {str(e)}\n"
            f"  ACTION: Check file permissions and ensure the file is accessible."
        ) from e

    updated = False
    content = original_content
    current_pipeline_name = current_pipeline_id.split("/")[-1] if "/" in current_pipeline_id else current_pipeline_id
    new_pipeline_name = new_pipeline_id.split("/")[-1] if "/" in new_pipeline_id else new_pipeline_id

    # Use provided names if available, otherwise derive from pipeline ID
    current_name_to_use = current_name if current_name else current_pipeline_name
    new_name_to_use = new_name if new_name else new_pipeline_name

    content = re.sub(
        r'"pipelineId"\s*:\s*"(' + re.escape(current_pipeline_id) + r')"',
        lambda m: m.group(0).replace(current_pipeline_id, new_pipeline_id),
        content,
    )

    content = re.sub(
        r'"pipelineId"\s*:\s*\{\s*"type"\s*:\s*"literal"\s*,\s*"value"\s*:\s*"('
        + re.escape(current_pipeline_id)
        + r')"\s*\}',
        lambda m: m.group(0).replace(current_pipeline_id, new_pipeline_id),
        content,
    )

    content = re.sub(
        r'"nodeName"\s*:\s*"(' + re.escape(current_name_to_use) + r')"',
        lambda m: m.group(0).replace(current_name_to_use, new_name_to_use),
        content,
    )

    content = re.sub(
        r'"task_key"\s*:\s*"(' + re.escape(current_name_to_use) + r')"',
        lambda m: m.group(0).replace(current_name_to_use, new_name_to_use),
        content,
    )

    content = re.sub(
        r'"task_key"\s*:\s*"([^"]*' + re.escape(current_name_to_use) + r'[^"]*)"',
        lambda m: m.group(0).replace(current_name_to_use, new_name_to_use),
        content,
    )

    # Update package_name in python_wheel_task - preserve spacing
    if current_package_name and new_package_name:
        content = re.sub(
            r'"package_name"\s*:\s*"(' + re.escape(current_package_name) + r')"',
            lambda m: m.group(0).replace(current_package_name, new_package_name),
            content,
        )

    # Update whl paths (in "whl" field) - be careful to only update in whl context
    if current_package_name and new_package_name:
        content = re.sub(
            r'"whl"\s*:\s*"([^"]*' + re.escape(current_package_name) + r'[^"]*)"',
            lambda m: m.group(0).replace(current_package_name, new_package_name),
            content,
        )

    # Update path field in PipelineComponent
    if current_package_name and new_package_name:
        content = re.sub(
            r'"path"\s*:\s*"([^"]*' + re.escape(current_package_name) + r'[^"]*)"',
            lambda m: m.group(0).replace(current_package_name, new_package_name),
            content,
        )

    # Update path field with pipeline name
    content = re.sub(
        r'"path"\s*:\s*"([^"]*' + re.escape(current_name_to_use) + r'[^"]*)"',
        lambda m: m.group(0).replace(current_name_to_use, new_name_to_use),
        content,
    )

    # Update PipelineComponent.pipelineId in libraries
    content = re.sub(
        r'"PipelineComponent"\s*:\s*\{[^}]*"pipelineId"\s*:\s*"(' + re.escape(current_pipeline_id) + r')"',
        lambda m: m.group(0).replace(current_pipeline_id, new_pipeline_id),
        content,
    )

    # Update prophecy.packages.path (JSON string within JSON)
    # This is tricky - we need to parse the JSON string, update it, then put it back
    def update_packages_path_in_content(content_str):
        def replace_packages_path(match):
            full_match = match.group(0)
            packages_str = match.group(1)
            try:
                # Unescape the JSON string
                packages_str_unescaped = packages_str.replace('\\"', '"').replace("\\\\", "\\")
                packages = json.loads(packages_str_unescaped)
                path_updated = False

                # Update pipeline ID keys
                if current_pipeline_id in packages:
                    packages[new_pipeline_id] = packages.pop(current_pipeline_id)
                    path_updated = True

                # Update package names in paths
                if current_package_name and new_package_name:
                    for key in list(packages.keys()):
                        if current_package_name in packages[key]:
                            packages[key] = packages[key].replace(current_package_name, new_package_name)
                            path_updated = True

                # Update pipeline names in paths
                for key in list(packages.keys()):
                    if current_name_to_use in packages[key] and new_name_to_use not in packages[key]:
                        packages[key] = packages[key].replace(current_name_to_use, new_name_to_use)
                        path_updated = True

                if path_updated:
                    # Re-escape and return
                    new_packages_str = json.dumps(packages)
                    new_packages_str_escaped = new_packages_str.replace('"', '\\"').replace("\\", "\\\\")
                    return full_match.replace(packages_str, new_packages_str_escaped)
            except:
                # Fallback: simple string replacement
                if current_pipeline_id in packages_str:
                    return full_match.replace(current_pipeline_id, new_pipeline_id)
            return full_match

        # Match "prophecy.packages.path": "escaped_json_string"
        content_str = re.sub(r'"prophecy\.packages\.path"\s*:\s*"([^"]+)"', replace_packages_path, content_str)
        return content_str

    content = update_packages_path_in_content(content)

    # Check if content changed
    if content != original_content:
        try:
            _write_file_with_retry(json_file_path, content, preserve_line_ending=original_line_ending)
            log(f"Updated JSON file: {json_file_path}")
        except Exception as e:
            raise FileOperationError(
                f"Failed to write JSON file: {json_file_path}\n"
                f"  Error: {str(e)}\n"
                f"  ACTION: Check file permissions and ensure the file is not locked."
            ) from e


def sync_pipeline(
    project_path: str, pipeline_id: Optional[str] = None, pipeline_name: Optional[str] = None, unsafe: bool = False
) -> None:
    current_pipeline_id, current_directory_name, target_pipeline_name = validate_sync(
        project_path, pipeline_id, pipeline_name
    )

    log(f"Starting pipeline sync: {current_pipeline_id} (dir: {current_directory_name}) -> {target_pipeline_name}")

    if unsafe:
        log("UNSAFE MODE: Will rename package names, app names, and all identifiers.")
        print(
            "WARNING: This will update all package imports, directory structures, and identifiers throughout the codebase."
        )
        print("It can corrupt the project, please review the changes carefully before pushing")
    else:
        log("SAFE MODE: Only pipeline name/ID will be changed. Package names and app names remain unchanged.")

    new_pipeline_id = f"pipelines/{target_pipeline_name}"

    try:
        rename_pipeline_directory(project_path, current_directory_name, target_pipeline_name)
    except (FileNotFoundError, PermissionError) as e:
        raise FileOperationError(str(e)) from e

    new_pipeline_code_path = os.path.join(project_path, "pipelines", target_pipeline_name, "code")
    new_workflow_path = os.path.join(new_pipeline_code_path, ".prophecy", "workflow.latest.json")

    if unsafe:
        try:
            current_identifiers = get_pipeline_identifiers(new_workflow_path)
        except (FileNotFoundError, ValidationError) as e:
            raise ValidationError(str(e)) from e

        is_valid, warning = validate_package_scoping(
            project_path, target_pipeline_name, current_identifiers["package_name"]
        )
        if not is_valid:
            raise ValidationError(f"Package scoping validation failed: {warning}")
        if warning:
            log(f"Warning: {warning}")

        new_identifiers = derive_new_identifiers(target_pipeline_name)

        log(f"Current package: {current_identifiers['package_name']}")
        log(f"New package: {new_identifiers['package_name']}")

        try:
            rename_package_directories(
                new_pipeline_code_path, current_identifiers["package_name"], new_identifiers["package_name"]
            )
        except PermissionError as e:
            raise FileOperationError(str(e)) from e

        try:
            update_workflow_json(new_workflow_path, current_identifiers, new_identifiers)
            update_python_files(
                new_pipeline_code_path, current_identifiers, new_identifiers, current_pipeline_id, new_pipeline_id
            )
        except (FileOperationError, ValidationError) as e:
            raise ValidationError(str(e)) from e
    else:
        try:
            update_workflow_json_uri_only(new_workflow_path, current_pipeline_id, new_pipeline_id)
            update_python_files_pipeline_id_only(
                new_pipeline_code_path,
                current_pipeline_id,
                new_pipeline_id,
                current_directory_name,
                target_pipeline_name,
            )
        except (FileOperationError, ValidationError) as e:
            raise ValidationError(str(e)) from e

    try:
        update_pbt_project_yml(project_path, current_pipeline_id, new_pipeline_id, target_pipeline_name)
    except (FileNotFoundError, ValidationError, FileOperationError) as e:
        raise ValidationError(str(e)) from e

    current_package_name_for_jobs = None
    new_package_name_for_jobs = None

    if unsafe:
        current_package_name_for_jobs = current_identifiers["package_name"].split(".")[-1]
        new_package_name_for_jobs = new_identifiers["package_name"].split(".")[-1]
    else:
        setup_py_path = os.path.join(new_pipeline_code_path, "setup.py")
        if os.path.exists(setup_py_path):
            try:
                setup_content = _read_file_with_retry(setup_py_path)
                name_match = re.search(r"name\s*=\s*['\"]([^'\"]+)['\"]", setup_content)
                if name_match:
                    package_name = name_match.group(1)
                    current_package_name_for_jobs = package_name
                    new_package_name_for_jobs = package_name
            except Exception:
                pass

    try:
        update_job_json_files(
            project_path,
            current_pipeline_id,
            new_pipeline_id,
            current_directory_name,
            target_pipeline_name,
            current_package_name_for_jobs,
            new_package_name_for_jobs,
        )
    except (FileOperationError, ValidationError) as e:
        raise ValidationError(str(e)) from e

    # Validate that current name is no longer present
    is_valid, files_with_current_name = validate_current_name_removed(
        project_path, target_pipeline_name, current_pipeline_id
    )
    if not is_valid:
        print(
            f"\n[bold yellow]WARNING: The current pipeline name '{current_directory_name}' still appears in the following files:[/bold yellow]"
        )
        for file_path in files_with_current_name[:10]:  # Show first 10 files
            print(f"  - {file_path}")
        if len(files_with_current_name) > 10:
            print(f"  ... and {len(files_with_current_name) - 10} more files")
        print(
            "[bold yellow]ACTION: Review these files manually to ensure all references are updated correctly.[/bold yellow]\n"
        )

    log(
        f"Pipeline sync completed successfully: {current_pipeline_id} -> {new_pipeline_id} (name: {target_pipeline_name})"
    )
