def DBT_1():
    settings = {}
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    envs = {}
    envs["DBT_PROFILES_DIR"] = "/usr/local/airflow/dags"

    return BashOperator(
        task_id = "DBT_1",
        bash_command = "set -euxo pipefail; tmpDir=`mktemp -d`; git clone https://github.com/abhisheks-prophecy/sql_snowflake_public_parent --branch main --single-branch $tmpDir; cd $tmpDir/; dbt run --profile run_profile_snowflake; dbt test --profile run_profile_snowflake; ",
        env = envs,
        append_env = True,
        **settings
    )
