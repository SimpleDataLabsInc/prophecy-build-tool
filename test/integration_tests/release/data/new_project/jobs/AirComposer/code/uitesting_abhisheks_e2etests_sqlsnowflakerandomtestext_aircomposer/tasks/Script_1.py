def Script_1():
    settings = {}
    from datetime import timedelta
    from airflow.operators.python import PythonOperator

    return PythonOperator(
        task_id = "Script_1",
        python_callable = lambda *args, **kwargs: exec("print(\"hello\")"),
        **settings
    )
