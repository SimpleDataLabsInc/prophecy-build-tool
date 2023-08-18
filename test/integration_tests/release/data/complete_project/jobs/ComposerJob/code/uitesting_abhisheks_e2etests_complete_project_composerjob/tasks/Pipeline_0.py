def Pipeline_0():
    settings = {}
    from datetime import timedelta
    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator # noqa

    return DatabricksSubmitRunOperator(  # noqa
        task_id = "Pipeline_0",
        json = {
          "task_key": "Pipeline_0", 
          "new_cluster": {
            "node_type_id": "i3.xlarge", 
            "spark_version": "12.2.x-scala2.12", 
            "num_workers": 1, 
            "spark_conf": {
              "spark.prophecy.execution.metrics.component-metrics.table": "prophecy.component_runs_manual", 
              "spark.prophecy.metadata.job.uri": "__PROJECT_ID_PLACEHOLDER__/jobs/ComposerJob", 
              "spark.prophecy.execution.metrics.interims.table": "prophecy.interims_manual", 
              "spark.prophecy.metadata.is.interactive.run": "false", 
              "spark.prophecy.metadata.fabric.id": "5295", 
              "spark.prophecy.metadata.url": "__PROPHECY_URL_PLACEHOLDER__", 
              "spark.prophecy.execution.metrics.pipeline-metrics.table": "prophecy.pipeline_runs_manual", 
              "spark.prophecy.packages.path": "{\"pipelines/OnlyOne\":\"dbfs:/FileStore/prophecy/artifacts/prophecy/uitesting/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/OnlyOne.jar\"}", 
              "spark.prophecy.deployment.id": "__PROJECT_ID_PLACEHOLDER__",
              "spark.prophecy.execution.metrics.disabled": False, 
              "spark.databricks.isv.product": "prophecy", 
              "spark.prophecy.metadata.job.branch": "__PROJECT_RELEASE_VERSION_PLACEHOLDER__", 
              "spark.prophecy.execution.service.url": "wss://execution.dp.uitesting.prophecy.io/eventws"
            }, 
            "driver_node_type_id": "i3.xlarge"
          }, 
          "spark_jar_task": {
            "main_class_name": "io.prophecy.pipelines.onlyone.Main", 
            "parameters": ["-i", "default", "-O", "{}"]
          }, 
          "libraries": [{"maven" : {"coordinates" : "io.prophecy:prophecy-libs_2.12:3.3.0-7.1.6"}},                          {"pypi" : {"package" : "prophecy-libs==1.5.10"}},                          {
                           "jar": "dbfs:/FileStore/prophecy/artifacts/prophecy/uitesting/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/OnlyOne.jar"
                         }]
        },
        databricks_conn_id = "dev_databricks_connection",
        **settings
    )
