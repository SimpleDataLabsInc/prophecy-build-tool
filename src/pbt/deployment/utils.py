import os


def get_maven_opts():
    """Get Maven options from environment with default fallbacks.

    This function merges MAVEN_OPTS from environment variables with specific default values:
    - -Xmx1024m (heap size)
    - -XX:MaxMetaspaceSize=512m (metaspace size)
    - -Xss32m (stack size)

    Environment variables take precedence, and missing defaults are appended.

    Returns:
        str: Merged Maven options string
    """
    existing_opts = os.environ.get("MAVEN_OPTS", "").strip()

    if not existing_opts:
        return "-Xmx1024m -XX:MaxMetaspaceSize=512m -Xss32m -Dfile.encoding=UTF-8"

    # Check which specific default options are missing and append them
    result = existing_opts

    # Add default heap size if not present
    if not any(opt.startswith("-Xmx") for opt in existing_opts.split()):
        result += " -Xmx1024m"

    # Add default metaspace size if not present
    if "-XX:MaxMetaspaceSize=" not in existing_opts:
        result += " -XX:MaxMetaspaceSize=512m"

    # Add default stack size if not present
    if not any(opt.startswith("-Xss") for opt in existing_opts.split()):
        result += " -Xss32m"

    return result
