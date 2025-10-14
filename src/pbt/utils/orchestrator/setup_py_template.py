# fmt: off
import os
from setuptools import setup

# Base requirements (hardcoded)
install_requires = [
    "dbt-core~=1.9.1",
    "dbt-postgres~=1.9.0",
    "dbt-bigquery~=1.9.0",
    "dbt-databricks~=1.9.0",
    "dbt-snowflake~=1.9.0",
    "dbt-trino~=1.9.0",
    "dbt-redshift~=1.9.0",
    "fastapi[all]",
    "uvicorn",
    "pydantic",
    "grpcio",
    "grpcio-tools",
    "sqlglot",
    "jinja2>=3.1.2,<4.0.0",
    "pyyaml>=6.0,<7.0",
    "networkx>=3.1,<4.0",
    "python-dotenv>=1.0.0,<2.0.0",
    "colorlog>=6.7.0,<7.0.0",
    "structlog>=23.1.0,<24.0.0",
    "supervisor-stdout>=0.1.1",
    "googleapis-common-protos>=1.60.0",
    "grpcio-reflection",
    "dbt-duckdb~=1.9.0",
    "s5cmd",
    "opentelemetry-api",
    "opentelemetry-sdk",
    "opentelemetry-exporter-zipkin",
    "opentelemetry-exporter-otlp",
    "opentelemetry-distro[otlp]",
    "opentelemetry-instrumentation-grpc",
]

# Read additional dependencies from requirements-pipeline.txt if it exists
requirements_file = "{requirements_path}"
if os.path.exists(requirements_file):
    with open(requirements_file, "r") as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if line and not line.startswith("#"):
                # Avoid duplicates
                if line not in install_requires:
                    install_requires.append(line)

setup(
    name="{package_name}",
    version="{version}",
    packages=["{package_name}", "data"],  # Two packages
    package_data={{
        "data": [
            "{project_name}/**/*",
            "{project_name}/**/.*",
            "{project_name}/**/.*/**/*",
        ],  # Include all project files
    }},
    include_package_data=True,
    install_requires=install_requires,
    entry_points={{
        "console_scripts": [
            "main = {package_name}.__main__:main",
        ],
    }},
    python_requires=">=3.7",
    description="Prophecy Pipeline Orchestration: {pipeline_name}",
    zip_safe=False,
)
# fmt: on
