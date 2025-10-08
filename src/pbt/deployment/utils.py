import os


def get_maven_opts():
    """Get Maven options from environment with default fallbacks.

    This function merges MAVEN_OPTS from environment variables with default values.
    Environment variables take precedence, and missing defaults are appended.

    Returns:
        str: Merged Maven options string
    """
    default_opts = "-Xmx1024m -XX:MaxMetaspaceSize=512m -Xss32m"
    existing_opts = os.environ.get("MAVEN_OPTS", "").strip()

    if not existing_opts:
        return default_opts

    # Start with existing options, then add missing defaults
    result_parts = existing_opts.split()
    default_parts = default_opts.split()

    # Extract keys from existing options
    existing_keys = set()
    for part in result_parts:
        if part.startswith("-Xmx"):
            existing_keys.add("-Xmx")
        elif part.startswith("-Xss"):
            existing_keys.add("-Xss")
        elif part.startswith("-XX:"):
            key = part.split("=")[0]
            existing_keys.add(key)
        else:
            existing_keys.add(part)

    # Add missing defaults
    for part in default_parts:
        if part.startswith("-Xmx") and "-Xmx" not in existing_keys:
            result_parts.append(part)
        elif part.startswith("-Xss") and "-Xss" not in existing_keys:
            result_parts.append(part)
        elif part.startswith("-XX:"):
            key = part.split("=")[0]
            if key not in existing_keys:
                result_parts.append(part)

    return " ".join(result_parts)
