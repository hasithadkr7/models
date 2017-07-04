import datetime as dt

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from curwrf.workflow.airflow.dags import utils as dag_utils

WRF_DAG_NAME = 'wrf_run'

wrf_config = dag_utils.set_initial_parameters()

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# initiate the DAG
dag = DAG(
    WRF_DAG_NAME,
    default_args=default_args,
    description='Running WRF instantaneously',
    schedule_interval=None)

initialize_params = PythonOperator(
    task_id='initialize-params',
    python_callable=dag_utils.set_initial_parameters,
    provide_context=True,
    op_args=[],
    default_args=default_args,
    dag=dag,
)

gfs_data_download = SubDagOperator(
    task_id='gfs_download',
    subdag=dag_utils.get_gfs_download_subdag(WRF_DAG_NAME, 'gfs_download', default_args),
    default_args=default_args,
    dag=dag,
)
initialize_params.set_downstream(gfs_data_download)

