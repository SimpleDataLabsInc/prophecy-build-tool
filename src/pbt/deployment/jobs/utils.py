import re
from typing import Set
from ...entities.project import is_cross_project_pipeline


def extract_dependency_project_ids(obj: dict, dependency_project_ids: Set[str] = None) -> Set[str]:
    """
    Recursively search for pipelineId keys in prophecy-job.json and extract project IDs.

    Args:
        obj: Parsed prophecy-job.json as a dictionary or any nested structure
        dependency_project_ids: Set to accumulate found project IDs (created if None)

    Returns:
        Set of dependency project IDs found in the JSON
    """
    if dependency_project_ids is None:
        dependency_project_ids = set()

    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "pipelineId":
                # Handle both string and dict formats for pipelineId
                pipeline_id = value
                if isinstance(pipeline_id, dict) and pipeline_id.get("type") == "literal":
                    pipeline_id = pipeline_id.get("value")

                if isinstance(pipeline_id, str):
                    result = is_cross_project_pipeline(pipeline_id)
                    if result[0] is not None:  # project_id found
                        project_id, _, _ = result
                        dependency_project_ids.add(project_id)
            else:
                # Continue recursive search
                extract_dependency_project_ids(value, dependency_project_ids)
    elif isinstance(obj, list):
        for item in obj:
            extract_dependency_project_ids(item, dependency_project_ids)

    return dependency_project_ids


def _extract_package_name_and_version(whl_path):
    """
    Extract package name and version from the whl file path.
    Example input:
    dbfs:/FileStore/prophecy/artifacts/dev/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/customers_orders-1.0-py3-none-any.whl
    Output: ('customers_orders', '1.0')
    """
    match = re.search(r"pipeline/(.*?)-([\d.]+)-py3-none-any\.whl", whl_path)
    if match:
        package_name = match.group(1)
        package_version = match.group(2)
        return package_name, package_version
    return None, None


def modify_databricks_json_for_private_artifactory(data, artifactory=None):
    """
    Modify 'whl' entries in the 'request' section to 'pypi' entries.

    Args:
        data (dict): Parsed JSON data.

    Returns:
        dict: Modified JSON data.
    """
    for task in data["request"]["tasks"]:
        libraries = task.get("libraries", [])

        # Filter out 'whl' entries and replace with 'pypi'
        new_libraries = []
        for lib in libraries:
            if "whl" in lib:
                whl_path = lib["whl"]
                package_name, package_version = _extract_package_name_and_version(whl_path)

                if package_name and package_version:
                    # Replace 'whl' with 'pypi' package
                    if artifactory:
                        new_libraries.append(
                            {
                                "pypi": {
                                    "package": f"{package_name}=={package_version}",
                                    "repo": f"{artifactory.rstrip('/')}/simple",  # adding as pip uses simple api
                                    # for downloading packages
                                }
                            }
                        )
                    else:
                        new_libraries.append({"pypi": {"package": f"{package_name}=={package_version}"}})
                else:
                    new_libraries.append(lib)  # If extraction fails, keep the original
            else:
                new_libraries.append(lib)

        task["libraries"] = new_libraries

    return data
