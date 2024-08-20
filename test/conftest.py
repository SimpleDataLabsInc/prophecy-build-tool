# conftest.py

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session', autouse=True)
def setup_session():
    # Code to run before all tests
    print("\nCreate a single spark session so that any following tests do not use a sparksession which is used by "
          "another which would get destroyed. ")

    sparkSession = (
        SparkSession.builder.master("local")
        .appName("init")
        .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
        .config("spark.port.maxRetries", "100")
    )

    yield

    print("\ntestconf teardown")
