# prophecy-build-tool

**Prophecy** is designed to enable all users to be productive with data engineering. It also replaces legacy ETL
products.
To learn more about Prophecy visit https://docs.prophecy.io/.

**Prophecy-build-tool** (PBT) allows you to quickly build projects generated by Prophecy (your standard Spark Scala and
PySpark pipelines) to integrate them with your own CI / CD (e.g. Github Actions), build system (e.g. Jenkins), and
orchestration (e.g. Databricks Workflows).

For the latest information on how to use Prophecy-build-tool, please visit the Prophecy Build Tool documentation [here](https://docs.prophecy.io/deployment/prophecy-build-tool/).

## Installation

To install PBT, simply run:

```shell
pip3 install prophecy-build-tool
```

## Running

To build and deploy your Prophecy project containing Python/Scala projects and Databricks Jobs run

```shell
pbt deploy --path /path/to/your/prophecy_project/
```

Sample output:

```shell
Prophecy-build-tool v1.0.0

Found 1 pipelines: customers_orders (python)
Found 1 jobs: daily_report

Building 1 pipeline 🚰

  Building pipeline pipelines/customers_orders [1/1]
    running build
    running build_py

✅ Build complete!

Deploying 1 job ⏱

  Deploying job jobs/daily_report [1/1]
    Uploading cs-1.0-py3-none-any.whl to dbfs:/FileStore/prophecy/artifacts/...
    Updating an existing job: daily_report

✅ Deployment complete!
```

### Build
To just build your Prophecy project containing Python/Scala projects and Databricks Jobs run

```shell
pbt build --path /path/to/your/prophecy_project/
```


### Deploy
To build and Deploy all jobs in your project, run deploy command
```shell
pbt deploy --path /path/to/your/prophecy_project/
```

It's also possible to only deploy jobs associated with given **fabrics**, you can provide fabrics id's (comma separated)
to just deploy only those jobs.

```shell
pbt deploy --fabric-ids 3,7 --path /path/to/your/prophecy_project/
```

By default, `deploy` command builds all pipelines and then deploys them, if you want to skip building all pipelines 
( this could be useful, if you are running a `deploy` command  after running `deploy` or `build` previously.)

```shell
pbt deploy --skip-builds --path /path/to/your/prophecy_project/
```



### Test
To run all unit tests in your Prophecy project containing Python/Scala projects and Databricks Jobs run

```shell
pbt test --path /path/to/your/prophecy_project/
```


Sample output:
```shell
Prophecy-build-tool v1.0.1

Found 1 jobs: daily
Found 1 pipelines: customers_orders (python)

  Unit Testing pipeline pipelines/customers_orders [1/1]

    ============================= test session starts ==============================
    platform darwin -- Python 3.8.9, pytest-7.1.2, pluggy-1.0.0 -- /Library/Developer/CommandLineTools/usr/bin/python
    cachedir: .pytest_cache
    metadata: None
    rootdir: /Users/kiran/proj2/pipelines/customers_orders/code
    plugins: html-3.1.1, metadata-2.0.2
    collecting ... collected 1 item

    test/TestSuite.py::CleanupTest::test_unit_test_0 PASSED                  [100%]
    
    ============================== 1 passed in 17.42s ==============================

✅ Unit test for pipeline: pipelines/customers_orders succeeded.
```


### Validate
To quickly validate if all your pipelines are not broken 

```shell
pbt validate --path /path/to/your/prophecy_project/
```

Sample output:
```shell
Prophecy-build-tool v1.0.3.4

Project name: resources.HelloWorld
Found 1 jobs: default_schedule
Found 4 pipelines: customers_orders (python), report_top_customers (python), join_agg_sort (python), farmers-markets-irs (python)

Validating 4 pipelines 

  Validating pipeline pipelines/customers_orders [1/4]

 Pipeline is validated: customers_orders

  Validating pipeline pipelines/report_top_customers [2/4]

 Pipeline is validated: report_top_customers

  Validating pipeline pipelines/join_agg_sort [3/4]

 Pipeline is validated: join_agg_sort

  Validating pipeline pipelines/farmers-markets-irs [4/4]

 Pipeline is validated: farmers-markets-irs
```



## Integrating with Github actions

Prophecy-built-tool (PBT) can be integrated with CI/CD (eg: Github Actions). A sample Github Actions .yml file that builds, tests and deploys the project on every code push is mentioned below:
```yaml
name: Example CI

on: [push]

env:
  DATABRICKS_HOST: "https://<<databricks-host>>.databricks.com"
  DATABRICKS_TOKEN: "<<databricks-token>>"
  FABRIC_NAME: "fabric-name"

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3
      - name: Set up JDK 17
        uses: actions/setup-java@v3
        with:
          java-version: '17'
          distribution: 'adopt'
      - name: Set up Python 3.x
        uses: actions/setup-python@v4
        with:
          python-version: '3.x'
      # Install all python dependencies 
      # prophecy-libs not included here because prophecy-build-tool 
      # takes care of it by reading each pipeline's setup.py
      - name: Install dependencies
        run: |
          python3 -m pip install --upgrade pip
          pip3 install build pytest wheel pytest-html pyspark prophecy-build-tool
      - name: Run PBT build
        run: pbt build --path .
      - name: Run PBT test
        run: pbt test --path .
      - name: Run PBT deploy
        run: pbt deploy --path .

```

