import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from ut2c4qu9rltiy3q4lcnkjq_.tasks import DBT_1, Email_1, OnlyOne
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "ut2c4qU9RLTIy3q4LCnkjQ_", 
    schedule_interval = "0 0/1 * * *", 
    default_args = {"owner" : "Prophecy", "ignore_first_depends_on_past" : True, "do_xcom_push" : True, "pool" : "n7pJN9mh"}, 
    start_date = pendulum.today('UTC'), 
    end_date = pendulum.datetime(2024, 7, 17, tz = "UTC"), 
    catchup = True
) as dag:
    OnlyOne_op = OnlyOne()
    Email_1_op = Email_1()
    DBT_1_op = DBT_1()
    OnlyOne_op >> Email_1_op
    Email_1_op >> DBT_1_op
