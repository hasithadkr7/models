import datetime as dt
import json
import logging

from curw.workflow.airflow import utils as af_utils
from curw.workflow.airflow.dags.docker_impl import utils as airflow_docker_utils
from curw.workflow.airflow.dags.production_2 import utils
from curw.workflow.airflow.extensions.operators.curw_dag_run_operator import dict_merge
from curw.workflow.airflow.extensions.operators.curw_dag_run_operator import CurwDagRunOperator
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


class RainfallMasterDagException(Exception):
    pass


default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'rainfall-master-dag-v1',
    default_args=default_args,
    description='Run 6 dag runs. dag_run.config not applicable',
    schedule_interval=None)


run_wps = CurwDagRunOperator(
    task_id='run_wps',
    trigger_dag_id='wrf-dag-v1',
    dag_run_conf=utils.get_dag_run_config(mode='wps'),
    run_id= '{{}}'
)