import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from t1podpe4id95d5catqdq2a_.tasks import DBT_0
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "T1pOdPE4Id95D5CaTQdQ2A_", 
    schedule_interval = "0 0 1 11 *", 
    default_args = {"owner" : "Prophecy", "ignore_first_depends_on_past" : True, "do_xcom_push" : True, "pool" : "n7pJN9mh"}, 
    start_date = pendulum.today('UTC'), 
    end_date = pendulum.datetime(2024, 7, 17, tz = "UTC"), 
    catchup = True
) as dag:
    DBT_0_op = DBT_0()
