import json

from src.pbt.deployment.jobs.utils import modify_databricks_json_for_private_artifactory

input_db_json = """
{
  "fabric_id" : "1",
  "components" : [ {
    "PipelineComponent" : {
      "path" : "dbfs:/FileStore/prophecy/artifacts/dev/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/customers_orders-1.0-py3-none-any.whl",
      "nodeName" : "customer_orders",
      "id" : "qFATU76qKR0jve4cqmCO1$$havVHr9lcuPnlvBsDFsbC",
      "language" : "python",
      "pipelineId" : "pipelines/customers_orders"
    }
  }, {
    "PipelineComponent" : {
      "path" : "dbfs:/FileStore/prophecy/artifacts/dev/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/join_agg_sort-1.0-py3-none-any.whl",
      "nodeName" : "join_agg",
      "id" : "GfTz7Z0PB1NSomLlG5Ut0$$94lvOhiJ2eP6hBPlC55wB",
      "language" : "python",
      "pipelineId" : "pipelines/join_agg_sort"
    }
  } ],
  "request" : {
    "format" : "MULTI_TASK",
    "name" : "WheelTestJob",
    "job_clusters" : [ {
      "job_cluster_key" : "WheelTestJob_default_small",
      "new_cluster" : {
        "spark_version" : "12.2.x-scala2.12",
        "node_type_id" : "i3.xlarge",
        "num_workers" : 0,
        "custom_tags" : {
          "ResourceClass" : "SingleNode"
        },
        "enable_elastic_disk" : false,
        "spark_conf" : {
          "spark.master" : "local[*, 4]",
          "spark.databricks.cluster.profile" : "singleNode",
          "spark.prophecy.metadata.fabric.id" : "1",
          "spark.prophecy.metadata.job.uri" : "__PROJECT_ID_PLACEHOLDER__/jobs/WheelTestJob",
          "spark.prophecy.metadata.is.interactive.run" : "false",
          "spark.prophecy.project.id" : "__PROJECT_ID_PLACEHOLDER__",
          "spark.prophecy.metadata.user.id" : "3",
          "spark.prophecy.tasks" : "H4sIAAAAAAAAAKtWSi4tLsnPTS2Kzy9KSS0qVrJSKsgsSM3JzEst1ofJFcMkdZSy8jPz4hPT01HUwQTji/OLSpRqAQklsuVVAAAA",
          "spark.prophecy.metadata.job.branch" : "__PROJECT_RELEASE_VERSION_PLACEHOLDER__",
          "spark.prophecy.metadata.url" : "__PROPHECY_URL_PLACEHOLDER__",
          "spark.prophecy.execution.metrics.disabled" : "true",
          "spark.prophecy.execution.service.url" : "wss://ashishk3s.dev.cloud.prophecy.io/execution/eventws",
          "spark.databricks.isv.product" : "prophecy"
        },
        "spark_env_vars" : {
          "PYSPARK_PYTHON" : "/databricks/python3/bin/python3"
        },
        "runtime_engine" : "STANDARD",
        "aws_attributes" : {
          "first_on_demand" : 1,
          "availability" : "SPOT_WITH_FALLBACK",
          "zone_id" : "auto",
          "spot_bid_price_percent" : 100
        },
        "data_security_mode" : "NONE"
      }
    } ],
    "email_notifications" : { },
    "tasks" : [ {
      "task_key" : "customer_orders",
      "job_cluster_key" : "WheelTestJob_default_small",
      "python_wheel_task" : {
        "package_name" : "customers_orders",
        "entry_point" : "main",
        "parameters" : [ "-i", "default", "-O", "{}" ]
      },
      "libraries" : [ {
        "maven" : {
          "coordinates" : "io.prophecy:prophecy-libs_2.12:3.3.0-8.3.0-SNAPSHOT",
          "repo" : "https://s01.oss.sonatype.org/content/repositories/snapshots/"
        }
      }, {
        "pypi" : {
          "package" : "prophecy-libs==1.9.16"
        }
      }, {
        "whl" : "dbfs:/FileStore/prophecy/artifacts/dev/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/customers_orders-1.0-py3-none-any.whl"
      } ],
      "email_notifications" : { }
    }, {
      "task_key" : "join_agg",
      "depends_on" : [ {
        "task_key" : "customer_orders"
      } ],
      "job_cluster_key" : "WheelTestJob_default_small",
      "python_wheel_task" : {
        "package_name" : "join_agg_sort",
        "entry_point" : "main",
        "parameters" : [ "-i", "default", "-O", "{}" ]
      },
      "libraries" : [ {
        "maven" : {
          "coordinates" : "io.prophecy:prophecy-libs_2.12:3.3.0-8.3.0-SNAPSHOT",
          "repo" : "https://s01.oss.sonatype.org/content/repositories/snapshots/"
        }
      }, {
        "pypi" : {
          "package" : "prophecy-libs==1.9.16"
        }
      }, {
        "whl" : "dbfs:/FileStore/prophecy/artifacts/dev/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/join_agg_sort-1.0-py3-none-any.whl"
      } ],
      "email_notifications" : { }
    } ],
    "max_concurrent_runs" : 1
  },
  "cluster_mode" : {
    "clusterMode" : "Single"
  },
  "secret_scope" : "prophecy_jobs_3",
  "sorted_processes" : [ "qFATU76qKR0jve4cqmCO1$$havVHr9lcuPnlvBsDFsbC", "GfTz7Z0PB1NSomLlG5Ut0$$94lvOhiJ2eP6hBPlC55wB" ]
}
"""


def test_artifactory_databricks_json_update():
    # Input JSON string
    expected_data = """{
    "fabric_id": "1",
    "components": [
        {
            "PipelineComponent": {
                "path": "dbfs:/FileStore/prophecy/artifacts/dev/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/customers_orders-1.0-py3-none-any.whl",
                "nodeName": "customer_orders",
                "id": "qFATU76qKR0jve4cqmCO1$$havVHr9lcuPnlvBsDFsbC",
                "language": "python",
                "pipelineId": "pipelines/customers_orders"
            }
        },
        {
            "PipelineComponent": {
                "path": "dbfs:/FileStore/prophecy/artifacts/dev/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/join_agg_sort-1.0-py3-none-any.whl",
                "nodeName": "join_agg",
                "id": "GfTz7Z0PB1NSomLlG5Ut0$$94lvOhiJ2eP6hBPlC55wB",
                "language": "python",
                "pipelineId": "pipelines/join_agg_sort"
            }
        }
    ],
    "request": {
    "format": "MULTI_TASK",
    "name": "WheelTestJob",
    "job_clusters": [
        {
            "job_cluster_key": "WheelTestJob_default_small",
            "new_cluster": {
                "spark_version": "12.2.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 0,
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                },
                "enable_elastic_disk": false,
                "spark_conf": {
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode",
                    "spark.prophecy.metadata.fabric.id": "1",
                    "spark.prophecy.metadata.job.uri": "__PROJECT_ID_PLACEHOLDER__/jobs/WheelTestJob",
                    "spark.prophecy.metadata.is.interactive.run": "false",
                    "spark.prophecy.project.id": "__PROJECT_ID_PLACEHOLDER__",
                    "spark.prophecy.metadata.user.id": "3",
                    "spark.prophecy.tasks": "H4sIAAAAAAAAAKtWSi4tLsnPTS2Kzy9KSS0qVrJSKsgsSM3JzEst1ofJFcMkdZSy8jPz4hPT01HUwQTji/OLSpRqAQklsuVVAAAA",
                    "spark.prophecy.metadata.job.branch": "__PROJECT_RELEASE_VERSION_PLACEHOLDER__",
                    "spark.prophecy.metadata.url": "__PROPHECY_URL_PLACEHOLDER__",
                    "spark.prophecy.execution.metrics.disabled": "true",
                    "spark.prophecy.execution.service.url": "wss://ashishk3s.dev.cloud.prophecy.io/execution/eventws",
                    "spark.databricks.isv.product": "prophecy"
                },
                "spark_env_vars": {
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                },
                "runtime_engine": "STANDARD",
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "auto",
                    "spot_bid_price_percent": 100
                },
                "data_security_mode": "NONE"
            }
        }
    ],
    "email_notifications": {},
    "tasks": [
        {
            "task_key": "customer_orders",
            "job_cluster_key": "WheelTestJob_default_small",
            "python_wheel_task": {
                "package_name": "customers_orders",
                "entry_point": "main",
                "parameters": [
                    "-i",
                    "default",
                    "-O",
                    "{}"
                ]
            },
            "libraries": [
                {
                    "maven": {
                        "coordinates": "io.prophecy:prophecy-libs_2.12:3.3.0-8.3.0-SNAPSHOT",
                        "repo": "https://s01.oss.sonatype.org/content/repositories/snapshots/"
                    }
                },
                {
                    "pypi": {
                        "package": "prophecy-libs==1.9.16"
                    }
                },
                {
                    "pypi": {
                        "package": "customers_orders==1.0"
                    }
                }
            ],
            "email_notifications": {}
        },
        {
            "task_key": "join_agg",
            "depends_on": [
                {
                    "task_key": "customer_orders"
                }
            ],
            "job_cluster_key": "WheelTestJob_default_small",
            "python_wheel_task": {
                "package_name": "join_agg_sort",
                "entry_point": "main",
                "parameters": [
                    "-i",
                    "default",
                    "-O",
                    "{}"
                ]
            },
            "libraries": [
                {
                    "maven": {
                        "coordinates": "io.prophecy:prophecy-libs_2.12:3.3.0-8.3.0-SNAPSHOT",
                        "repo": "https://s01.oss.sonatype.org/content/repositories/snapshots/"
                    }
                },
                {
                    "pypi": {
                        "package": "prophecy-libs==1.9.16"
                    }
                },
                {
                    "pypi": {
                        "package": "join_agg_sort==1.0"
                    }
                }
            ],
            "email_notifications": {}
        }
    ],
    "max_concurrent_runs": 1
    },
    "cluster_mode": {
        "clusterMode": "Single"
    },
    "secret_scope": "prophecy_jobs_3",
    "sorted_processes": [
        "qFATU76qKR0jve4cqmCO1$$havVHr9lcuPnlvBsDFsbC",
        "GfTz7Z0PB1NSomLlG5Ut0$$94lvOhiJ2eP6hBPlC55wB"
    ]
}
"""
    # Step 1: Parse the JSON string into a Python dictionary
    data_dict = json.loads(input_db_json)
    # Step 2: Modify the 'whl' entries in the 'request' section
    modified_data = modify_databricks_json_for_private_artifactory(data_dict)
    modified_json = json.dumps(modified_data, indent=4)
    assert json.loads(modified_json) == json.loads(expected_data)


def test_artifactory_databricks_json_update_with_custom_artifactory():
    # Input JSON string
    expected_data = """{
    "fabric_id": "1",
    "components": [
        {
            "PipelineComponent": {
                "path": "dbfs:/FileStore/prophecy/artifacts/dev/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/customers_orders-1.0-py3-none-any.whl",
                "nodeName": "customer_orders",
                "id": "qFATU76qKR0jve4cqmCO1$$havVHr9lcuPnlvBsDFsbC",
                "language": "python",
                "pipelineId": "pipelines/customers_orders"
            }
        },
        {
            "PipelineComponent": {
                "path": "dbfs:/FileStore/prophecy/artifacts/dev/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/join_agg_sort-1.0-py3-none-any.whl",
                "nodeName": "join_agg",
                "id": "GfTz7Z0PB1NSomLlG5Ut0$$94lvOhiJ2eP6hBPlC55wB",
                "language": "python",
                "pipelineId": "pipelines/join_agg_sort"
            }
        }
    ],
    "request": {
        "format": "MULTI_TASK",
        "name": "WheelTestJob",
        "job_clusters": [
            {
                "job_cluster_key": "WheelTestJob_default_small",
                "new_cluster": {
                    "spark_version": "12.2.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 0,
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "enable_elastic_disk": false,
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.prophecy.metadata.fabric.id": "1",
                        "spark.prophecy.metadata.job.uri": "__PROJECT_ID_PLACEHOLDER__/jobs/WheelTestJob",
                        "spark.prophecy.metadata.is.interactive.run": "false",
                        "spark.prophecy.project.id": "__PROJECT_ID_PLACEHOLDER__",
                        "spark.prophecy.metadata.user.id": "3",
                        "spark.prophecy.tasks": "H4sIAAAAAAAAAKtWSi4tLsnPTS2Kzy9KSS0qVrJSKsgsSM3JzEst1ofJFcMkdZSy8jPz4hPT01HUwQTji/OLSpRqAQklsuVVAAAA",
                        "spark.prophecy.metadata.job.branch": "__PROJECT_RELEASE_VERSION_PLACEHOLDER__",
                        "spark.prophecy.metadata.url": "__PROPHECY_URL_PLACEHOLDER__",
                        "spark.prophecy.execution.metrics.disabled": "true",
                        "spark.prophecy.execution.service.url": "wss://ashishk3s.dev.cloud.prophecy.io/execution/eventws",
                        "spark.databricks.isv.product": "prophecy"
                    },
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "runtime_engine": "STANDARD",
                    "aws_attributes": {
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "zone_id": "auto",
                        "spot_bid_price_percent": 100
                    },
                    "data_security_mode": "NONE"
                }
            }
        ],
        "email_notifications": {},
        "tasks": [
            {
                "task_key": "customer_orders",
                "job_cluster_key": "WheelTestJob_default_small",
                "python_wheel_task": {
                    "package_name": "customers_orders",
                    "entry_point": "main",
                    "parameters": [
                        "-i",
                        "default",
                        "-O",
                        "{}"
                    ]
                },
                "libraries": [
                    {
                        "maven": {
                            "coordinates": "io.prophecy:prophecy-libs_2.12:3.3.0-8.3.0-SNAPSHOT",
                            "repo": "https://s01.oss.sonatype.org/content/repositories/snapshots/"
                        }
                    },
                    {
                        "pypi": {
                            "package": "prophecy-libs==1.9.16"
                        }
                    },
                    {
                        "pypi": {
                            "package": "customers_orders==1.0",
                            "repo": "https://prophecyio.jfrog.io/artifactory/api/pypi/pypi-local/simple"
                        }
                    }
                ],
                "email_notifications": {}
            },
            {
                "task_key": "join_agg",
                "depends_on": [
                    {
                        "task_key": "customer_orders"
                    }
                ],
                "job_cluster_key": "WheelTestJob_default_small",
                "python_wheel_task": {
                    "package_name": "join_agg_sort",
                    "entry_point": "main",
                    "parameters": [
                        "-i",
                        "default",
                        "-O",
                        "{}"
                    ]
                },
                "libraries": [
                    {
                        "maven": {
                            "coordinates": "io.prophecy:prophecy-libs_2.12:3.3.0-8.3.0-SNAPSHOT",
                            "repo": "https://s01.oss.sonatype.org/content/repositories/snapshots/"
                        }
                    },
                    {
                        "pypi": {
                            "package": "prophecy-libs==1.9.16"
                        }
                    },
                    {
                        "pypi": {
                            "package": "join_agg_sort==1.0",
                            "repo":"https://prophecyio.jfrog.io/artifactory/api/pypi/pypi-local/simple"
                        }
                    }
                ],
                "email_notifications": {}
            }
        ],
        "max_concurrent_runs": 1
    },
    "cluster_mode": {
        "clusterMode": "Single"
    },
    "secret_scope": "prophecy_jobs_3",
    "sorted_processes": [
        "qFATU76qKR0jve4cqmCO1$$havVHr9lcuPnlvBsDFsbC",
        "GfTz7Z0PB1NSomLlG5Ut0$$94lvOhiJ2eP6hBPlC55wB"
    ]
}
"""
    # Step 1: Parse the JSON string into a Python dictionary
    data_dict = json.loads(input_db_json)
    # Step 2: Modify the 'whl' entries in the 'request' section
    modified_data = modify_databricks_json_for_private_artifactory(
        data_dict, artifactory="https://prophecyio.jfrog.io/artifactory/api/pypi/pypi-local"
    )
    modified_json = json.dumps(modified_data, indent=4)
    assert json.loads(modified_json) == json.loads(expected_data)
