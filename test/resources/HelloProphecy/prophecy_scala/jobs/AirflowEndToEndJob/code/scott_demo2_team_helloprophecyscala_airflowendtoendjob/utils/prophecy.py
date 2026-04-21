from airflow.decorators import task

db_pipeline_id_to_path_dict = {
    "pipelines/gold_sales": "/Volumes/pbt_testing/default/prophecy//prophecy/artifacts/saas/app/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/gold_sales_2.12.jar", 
    "pipelines/gold_top_customers": "/Volumes/pbt_testing/default/prophecy//prophecy/artifacts/saas/app/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/gold_top_customers_2.12.jar", 
    "pipelines/raw_bronze": "/Volumes/pbt_testing/default/prophecy//prophecy/artifacts/saas/app/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/raw_bronze_2.12.jar", 
    "pipelines/silver_customers_orders": "/Volumes/pbt_testing/default/prophecy//prophecy/artifacts/saas/app/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/silver_customers_orders_2.12.jar", 
    "pipelines/silver_zip_codes": "/Volumes/pbt_testing/default/prophecy//prophecy/artifacts/saas/app/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/silver_zip_codes_2.12.jar"
}


def task_wrapper(task_id, **task_kwargs):

    def decorator(func):

        @task(task_id = task_id, **task_kwargs)
        def wrapper(*args, **context):
            ## running the actual method.
            return func(*args, **context).execute(context)

        return wrapper

    return decorator

pipeline_package_name = {
    "pipelines/silver_customers_orders": "io.prophecy.pipelines.silver_customers_orders.Main", 
    "pipelines/silver_zip_codes": "io.prophecy.pipelines.silver_zip_codes.Main", 
    "pipelines/gold_sales": "io.prophecy.pipelines.gold_sales.Main", 
    "pipelines/gold_top_customers": "io.prophecy.pipelines.gold_top_customers.Main", 
    "pipelines/raw_bronze": "io.prophecy.pipelines.raw_bronze.Main"
}
