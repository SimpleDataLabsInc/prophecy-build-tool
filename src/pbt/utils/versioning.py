import os
import re
from packaging.version as packaging_version
import semver
import glob
from ..utility import custom_print as log


def update_all_versions(project_path, project_language, orig_project_version, new_version, force):
    # check this version against base branch if not "force". error if it is not greater
    if not force:
        orig_version = orig_project_version
        if parse_version(new_version) <= parse_version(orig_version):
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


def get_bumped_version_python(original_version, bump_type):
    try:
        v = parse_version(original_version)
    except InvalidVersion:
        log(f"Error bumping: Unable to parse version {original_version}. "
            f"Use PEP440")
        exit(1)

    if bump_type == 'major':
        major = v.major + 1
        minor = 0
        micro = 0
    elif bump_type == 'minor':
        major = v.major
        minor = v.minor + 1
        micro = 0
    elif bump_type == 'patch':
        major = v.major
        minor = v.minor
        micro = v.micro + 1
    # Version is a final class and we cannot inherit/extend so we steal the string building logic from its
    #  __str__() implementation. this follows pep440:
    parts = []
    if v.epoch != 0:
        parts.append(f"{v.epoch}!")
    parts.append(f"{major}.{minor}.{micro}")
    if v.pre is not None:
        parts.append("".join(str(x) for x in v.pre))
    if v.post is not None:
        parts.append(f".post{v.post}")
    if v.dev is not None:
        parts.append(f".dev{v.dev}")
    if v.local is not None:
        parts.append(f"+{v.local}")
    new_version = "".join(parts)

    return new_version


def get_bumped_version_scala(original_version, bump_type):
    try:
        v = semver.parse_version_info(original_version)
    except InvalidVersion:
        log(f"Error bumping: Unable to parse version {original_version}. "
            f"Must Use Semantic Versioning")
        exit(1)

    if bump_type == 'major':
        v.bump_major()
    elif bump_type == 'minor':
        v.bump_minor()
    elif bump_type == 'patch':
        v.bump_patch()
    elif bump_type == 'build':
        v.bump_build()
    elif bump_type == 'prerelease':
        v.bump_prerelease()

    return str(v)


def get_bumped_version_sql(original_version, bump_type):
    # tODo check if dbt has a standard, otherwise use python
    return get_bumped_version_scala(original_version, bump_type)


def get_bumped_version(original_version, bump_type, project_language):
    # replace version in language specific files:

    try:
        v = semver.parse_version_info(original_version)
    except InvalidVersion:
        log(f"Error bumping: Unable to parse version {original_version}. "
            f"Must Use Semantic Versioning")
        exit(1)

    if project_language == 'python':
        try:
            packaging_version.parse(original_version)
        except InvalidVersion:
            log(f"Error bumping: Unable to parse version {original_version}. "
                f"Use PEP440")
            exit(1)

    if bump_type == 'major':
        v.bump_major()
    elif bump_type == 'minor':
        v.bump_minor()
    elif bump_type == 'patch':
        v.bump_patch()
    elif bump_type == 'build':
        v.bump_build()
    elif bump_type == 'prerelease':
        v.bump_prerelease()

    return str(v)

    if project_language == 'python':
        new_version = get_bumped_version_python(original_version, bump_type)
    elif project_language == 'scala':
        new_version = get_bumped_version_scala(original_version, bump_type)
    elif project_language == 'sql':
        new_version = get_bumped_version_sql(original_version, bump_type)
    else:
        raise ValueError("bad project language: ", project_language)

    return new_version
