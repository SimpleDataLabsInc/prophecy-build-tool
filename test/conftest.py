import os
import json


def pytest_configure():
    # add ivysettings to sparkconfig to allow concurrency on spark processes.
    conftest_directory = os.path.dirname(os.path.abspath(__file__))
    ivysettings_file = os.path.join(conftest_directory, "ivySettings.xml")
    if not os.path.exists(ivysettings_file):
        raise FileNotFoundError(f"missing ivysettings file for parallel processing of tests: {ivysettings_file}")

    # Set it as an environment variable
    spark_config = json.loads(os.environ.get("SPARK_CONFIG_JSON", "{}"))
    spark_config["spark.jars.ivySettings"] = ivysettings_file
    os.environ["SPARK_CONFIG_JSON"] = json.dumps(spark_config)

    # point maven opts to save ivysettings file so that the scala-maven-plugin will pass this location to sbt correctly
    os.environ["MAVEN_OPTS"] = " ".join(
        os.environ.get("MAVEN_OPTS", ""), f"-Dsbt.ivy.home={os.path.basename(ivysettings_file)}"
    )
