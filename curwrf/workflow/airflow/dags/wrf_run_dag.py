import datetime as dt

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from curwrf.workflow.airflow.dags import utils as dag_utils
from curwrf.wrf.execution.tasks import run_wps

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

pre_wps = PythonOperator(
    task_id='pre_wps',
    python_callable=run_wps.preprocess,
    provide_context=True,
    op_args=[],
    default_args=default_args,
    dag=dag,
)

pre_ungrib = PythonOperator(
    task_id='pre_ungrib',
    python_callable=run_wps.pre_ungrib,
    provide_context=True,
    op_args=[],
    default_args=default_args,
    dag=dag,
)

ungrib = PythonOperator(
    task_id='ungrib',
    python_callable=run_wps.ungrib,
    provide_context=True,
    op_args=[],
    default_args=default_args,
    dag=dag,
)

pre_geogrid = PythonOperator(
    task_id='pre_geogrid',
    python_callable=run_wps.pre_geogrid,
    provide_context=True,
    op_args=[],
    default_args=default_args,
    dag=dag,
)

geogrid = PythonOperator(
    task_id='geogrid',
    python_callable=run_wps.geogrid,
    provide_context=True,
    op_args=[],
    default_args=default_args,
    dag=dag,
)

pre_metgrid = PythonOperator(
    task_id='pre_metgrid',
    python_callable=run_wps.pre_metgrid,
    provide_context=True,
    op_args=[],
    default_args=default_args,
    dag=dag,
)

metgrid = PythonOperator(
    task_id='metgrid',
    python_callable=run_wps.metgrid,
    provide_context=True,
    op_args=[],
    default_args=default_args,
    dag=dag,
)

initialize_params >> [pre_wps, gfs_data_download]

pre_wps >> pre_geogrid >> geogrid

pre_ungrib << [gfs_data_download, pre_wps]
pre_ungrib >> ungrib

pre_metgrid << [geogrid, ungrib]

pre_metgrid >> metgrid