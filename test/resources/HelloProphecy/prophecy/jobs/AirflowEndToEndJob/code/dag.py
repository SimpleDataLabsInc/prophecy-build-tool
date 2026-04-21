import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from scott_demo2_team_helloprophecy_airflowendtoendjob.tasks import (
    gold_sales,
    gold_top_customers,
    raw_bronze,
    silver_customers,
    silver_zip_codes
)
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "scott_demo2_team_HelloProphecy_AirflowEndToEndJob", 
    schedule_interval = None, 
    default_args = {"owner" : "Prophecy", "ignore_first_depends_on_past" : True, "do_xcom_push" : True}, 
    start_date = pendulum.today('UTC'), 
    end_date = pendulum.datetime(2026, 11, 29, tz = "UTC"), 
    catchup = False
) as dag:
    raw_bronze_op = raw_bronze()
    silver_zip_codes_op = silver_zip_codes()
    silver_customers_op = silver_customers()
    gold_sales_op = gold_sales()
    gold_top_customers_op = gold_top_customers()
    raw_bronze_op >> silver_zip_codes_op
    silver_zip_codes_op >> silver_customers_op
    silver_customers_op >> gold_sales_op
    gold_sales_op >> gold_top_customers_op
