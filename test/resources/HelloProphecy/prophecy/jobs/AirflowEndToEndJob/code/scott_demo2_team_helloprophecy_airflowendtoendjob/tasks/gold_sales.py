from scott_demo2_team_helloprophecy_airflowendtoendjob.utils import *

@task_wrapper(task_id = "gold_sales")
def gold_sales(ti=None, params=None, **context):
    from datetime import timedelta
    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator # noqa

    return DatabricksSubmitRunOperator(  # noqa
        task_id = "gold_sales",
        json = {
          "task_key": "gold_sales", 
          "new_cluster": {
            "node_type_id": "i3.xlarge", 
            "spark_version": "16.4.x-scala2.12", 
            "runtime_engine": "PHOTON", 
            "num_workers": 0.0, 
            "data_security_mode": "DATA_SECURITY_MODE_DEDICATED", 
            "custom_tags": {}, 
            "spark_conf": {
              "spark.prophecy.metadata.job.uri": "__PROJECT_ID_PLACEHOLDER__/jobs/AirflowEndToEndJob", 
              "spark.prophecy.metadata.is.interactive.run": "false", 
              "spark.prophecy.metadata.fabric.id": "16433", 
              "spark.prophecy.tasks": "H4sIAAAAAAAAAKuuBQBDv6ajAgAAAA==", 
              "spark.prophecy.metadata.url": "__PROPHECY_URL_PLACEHOLDER__", 
              "spark.prophecy.metadata.user.id": "2169", 
              "spark.prophecy.project.id": "__PROJECT_ID_PLACEHOLDER__", 
              "spark.prophecy.execution.metrics.disabled": "true", 
              "spark.databricks.isv.product": "prophecy", 
              "spark.prophecy.metadata.job.branch": "__PROJECT_RELEASE_VERSION_PLACEHOLDER__", 
              "spark.prophecy.execution.service.url": "wss://app.prophecy.io/execution/eventws"
            }, 
            "is_single_node": True, 
            "aws_attributes": {
              "first_on_demand": 1.0, 
              "availability": "SPOT_WITH_FALLBACK", 
              "zone_id": "auto", 
              "spot_bid_price_percent": 100.0
            }, 
            "spark_env_vars": {"PYSPARK_PYTHON" : "/databricks/python3/bin/python3"}, 
            "kind": "CLASSIC_PREVIEW", 
            "enable_elastic_disk": False
          }, 
          "python_wheel_task": {
            "package_name": "gold_sales", 
            "entry_point": "main", 
            "parameters": ["-i", "default", "-O", "{}"]
          }, 
          "libraries": [{"maven" : {"coordinates" : "io.prophecy:prophecy-libs_2.12:3.5.0-9.2.0"}},                          {"pypi" : {"package" : "prophecy-libs==2.1.16"}},                          {
                           "whl": "/Volumes/pbt_testing/default/prophecy//prophecy/artifacts/saas/app/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/gold_sales-1.0-py3-none-any.whl"
                         }]
        },
        databricks_conn_id = "pbt-only-cicd-databricks"
    )
