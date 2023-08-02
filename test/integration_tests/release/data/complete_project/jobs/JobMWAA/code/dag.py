import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from uitesting_abhisheks_e2etests_complete_project_jobmwaa.tasks import DBT_1, Pipeline_0
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "uitesting_abhisheks_e2etests_complete_project_JobMWAA", 
    schedule_interval = "0 0/1 * * *", 
    default_args = {"owner" : "Prophecy", "ignore_first_depends_on_past" : True, "do_xcom_push" : True}, 
    start_date = pendulum.today('UTC'), 
    catchup = True, 
    tags = []
) as dag:
    Pipeline_0_op = Pipeline_0()
    DBT_1_op = DBT_1()
    Pipeline_0_op >> DBT_1_op
