import json
import re


def test_artifactory_databricks_json_update():
    def extract_package_name_and_version(whl_path):
        """
        Extract package name and version from the whl file path.
        Example input:
        dbfs:/FileStore/prophecy/artifacts/dev/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/customers_orders-1.0-py3-none-any.whl
        Output: ('customers_orders', '1.0')
        """
        match = re.search(r'pipeline/(.*?)-([\d\.]+)-py3-none-any\.whl', whl_path)
        if match:
            package_name = match.group(1)
            package_version = match.group(2)
            return package_name, package_version
        return None, None

    def modify_whl_in_request(data):
        """
        Modify 'whl' entries in the 'request' section to 'pypi' entries.

        Args:
            data (dict): Parsed JSON data.

        Returns:
            dict: Modified JSON data.
        """
        for task in data['request']['tasks']:
            libraries = task.get('libraries', [])

            # Filter out 'whl' entries and replace with 'pypi'
            new_libraries = []
            for lib in libraries:
                if 'whl' in lib:
                    whl_path = lib['whl']
                    package_name, package_version = extract_package_name_and_version(whl_path)

                    if package_name and package_version:
                        # Replace 'whl' with 'pypi' package
                        new_libraries.append({
                            "pypi": {
                                "package": f"{package_name}=={package_version}"
                            }
                        })
                    else:
                        new_libraries.append(lib)  # If extraction fails, keep the original
                else:
                    new_libraries.append(lib)

            task['libraries'] = new_libraries

        return data

    # Input JSON string
    data = """
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
    data_dict = json.loads(data)

    # Step 2: Modify the 'whl' entries in the 'request' section
    modified_data = modify_whl_in_request(data_dict)

    # Step 3: Convert the modified dictionary back to a JSON string
    modified_json = json.dumps(modified_data, indent=4)

    # Output the modified JSON
    print(modified_json)
    assert json.loads(modified_data) == json.loads(expected_data)
