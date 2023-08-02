def DBT_1():
    settings = {}
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    envs = {}
    envs["DBT_PROFILE_SECRET"] = "sBhJHMg-N93f4yYJBxzHe"
    envs["GIT_TOKEN_SECRET"] = "d9HL4N7ibK32n423N92XYw_"
    envs["GIT_ENTITY"] = "branch"
    envs["GIT_ENTITY_VALUE"] = "main"
    envs["GIT_SSH_URL"] = "https://github.com/abhisheks-prophecy/sql_snowflake_public_parent"
    envs["GIT_SUB_PATH"] = ""

    return BashOperator(
        task_id = "DBT_1",
        bash_command = f"$PROPHECY_HOME/run_dbt.sh \"dbt run --profile run_profile; dbt test --profile run_profile; \"",
        env = envs,
        append_env = True,
        **settings
    )
