import os
import json


def pytest_configure():
    conftest_directory = os.path.dirname(os.path.abspath(__file__))
    ivysettings_file = os.path.join(conftest_directory, "ivySettings.xml")
    if not os.path.exists(ivysettings_file):
        raise FileNotFoundError(f"missing ivysettings file for parallel processing of tests: {ivysettings_file}")
    # Set it as an environment variable
    spark_config = json.loads(os.environ.get("SPARK_CONFIG_JSON", "{}"))
    spark_config["spark.jars.ivySettings"] = ivysettings_file
    os.environ["SPARK_CONFIG_JSON"] = json.dumps(spark_config)
