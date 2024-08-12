import os
import re
from packaging import version as packaging_version
import semver
import glob
from ..utility import custom_print as log


def update_all_versions(project_path, project_language, orig_project_version, new_version, force):
    # check this version against base branch if not "force". error if it is not greater
    if not force:
        orig_version = orig_project_version
        if semver.parse_version_info(new_version) <= semver.parse_version_info(orig_version):
            log(f"new version {new_version} is not later than {orig_version}")
            raise ValueError(f"new version {new_version} is not later than {orig_version}")

    def _replace_in_files(matching_regex: str, replacement_string: str, files: list):
        # NOTE: use this pattern matching rather than opening/rewriting files as yaml parsing may shuffle
        #  line order and cause unnecessary changes.
        for file in files:
            pattern = re.compile(matching_regex, re.MULTILINE)
            with open(file, 'r') as fd:
                content = fd.read()
            # only replace the first instance of the version encountered. otherwise we risk
            # replacing other versions (especially found in pom.xml)
            new_content = pattern.sub(replacement_string, content, count=1)
            with open(file, 'w') as fd:
                fd.write(new_content)

    # PBT project
    pbt_project_file = os.path.join(project_path, "pbt_project.yml")
    _replace_in_files(r"^version: .*$", f"version: {new_version}", [pbt_project_file])

    # replace version in language specific files:
    if project_language == 'python':
        matching_regex = r"^\s*version\s*=\s*.*$"
        replacement_string = f"    version = '{new_version}',"
        filename_to_find = "setup.py"
    elif project_language == 'scala':
        matching_regex = r"^\s*<version>.*</version>"
        replacement_string = f"    <version>{new_version}</version>"
        filename_to_find = "pom.xml"
    elif project_language == 'sql':
        matching_regex = r"^version: .*$"
        replacement_string = f"version: \"{new_version}\""
        filename_to_find = "dbt_project.yml"
    else:
        raise ValueError("bad project language: ", project_language)

    files_to_fix = glob.glob(os.path.join(project_path, '**', filename_to_find),
                             recursive=True)
    _replace_in_files(matching_regex, replacement_string, files_to_fix)


def get_bumped_version(original_version, bump_type, project_language):
    try:
        v = semver.parse_version_info(original_version)
    except ValueError:
        log(f"Error bumping: Unable to parse version {original_version}. "
            f"Must Use Semantic Versioning")
        exit(1)

    if project_language == 'python':
        # add an extra check for python as python packages should conform to PEP440 standard and be parseable by
        # the `packaging` package
        try:
            packaging_version.parse(original_version)
        except packaging_version.InvalidVersion:
            log(f"Error bumping: Unable to parse version {original_version}. "
                f"Use PEP440")
            exit(1)

    if bump_type == 'major':
        v = v.bump_major()
    elif bump_type == 'minor':
        v = v.bump_minor()
    elif bump_type == 'patch':
        v = v.bump_patch()
    elif bump_type == 'build':
        v = v.bump_build()
    elif bump_type == 'prerelease':
        v = v.bump_prerelease()

    return str(v)
