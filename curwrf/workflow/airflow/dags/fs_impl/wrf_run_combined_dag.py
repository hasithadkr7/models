import datetime as dt
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from curwrf.workflow.airflow.dags import utils as dag_utils
from curwrf.workflow.airflow.extensions import tasks
from curwrf.workflow.airflow.extensions.operators import CurwPythonOperator

wrf_dag_name = 'wrf_run'
wrf_config_key_prefix = 'wrf_config'
wrf_home_key_prefix = 'wrf_home'
wrf_start_date_key_prefix = 'wrf_start_date'
queue = 'wrf_fs_impl_queue'
schedule_interval = None

test_mode = True

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
    'queue': queue,
}

# initiate the DAG
dag = DAG(
    wrf_dag_name,
    default_args=default_args,
    description='Running 4 WRFs simultaneously',
    schedule_interval=schedule_interval)

initialize_params = SubDagOperator(
    task_id='initialize_params',
    subdag=dag_utils.get_initial_parameters_subdag(wrf_dag_name, 'initialize_params', 4, default_args,
                                                   wrf_home_key_prefix, wrf_start_date_key_prefix,
                                                   wrf_config_key_prefix),
    default_args=default_args,
    dag=dag,
)

gfs_data_download = SubDagOperator(
    task_id='gfs_download',
    subdag=dag_utils.get_gfs_download_subdag(wrf_dag_name, 'gfs_download', default_args,
                                             wrf_config_key=wrf_config_key_prefix + '0', test_mode=test_mode),
    default_args=default_args,
    dag=dag,
)

ungrib = CurwPythonOperator(
    task_id='ungrib',
    curw_task=tasks.Ungrib,
    init_args=[wrf_config_key_prefix + '0'],
    provide_context=True,
    default_args=default_args,
    dag=dag,
    test_mode=test_mode
)

geogrid = CurwPythonOperator(
    task_id='geogrid',
    curw_task=tasks.Geogrid,
    init_args=[wrf_config_key_prefix + '0'],
    provide_context=True,
    default_args=default_args,
    dag=dag,
    test_mode=test_mode
)

metgrid = CurwPythonOperator(
    task_id='metgrid',
    curw_task=tasks.Metgrid,
    init_args=[wrf_config_key_prefix + '0'],
    provide_context=True,
    default_args=default_args,
    dag=dag,
    test_mode=test_mode
)

real_wrf = SubDagOperator(
    task_id='real_wrf',
    subdag=dag_utils.get_wrf_run_subdag(wrf_dag_name, 'real_wrf', 4, default_args, wrf_config_key_prefix,
                                        test_mode=test_mode),
    default_args=default_args,
    dag=dag,
)

initialize_params >> [gfs_data_download, geogrid, ungrib]

gfs_data_download >> ungrib

metgrid << [geogrid, ungrib]

metgrid >> real_wrf
