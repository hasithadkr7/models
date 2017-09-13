import datetime as dt
import airflow
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from curwrf.workflow.airflow.extensions import tasks, subdags
from curwrf.workflow.airflow.extensions.operators import CurwPythonOperator
from curwrf.workflow.airflow.dags import utils as dag_utils


wrf_dag_name = 'wrf_docker_run'
wrf_config_key_prefix = 'wrf_config'
wrf_home_key_prefix = 'wrf_home'
wrf_start_date_key_prefix = 'wrf_start_date'
queue = 'wrf_fs_impl_queue'
schedule_interval = '@once'

test_mode = False

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
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
    description='Running 4 WRFs simultaneously using docker',
    schedule_interval=schedule_interval)

initialize_params = PythonOperator(
            task_id='init-params',
            python_callable=dag_utils.set_initial_parameters_fs,
            provide_context=True,
            op_args=[wrf_home_key_prefix, wrf_start_date_key_prefix, wrf_config_key_prefix, True],
            default_args=default_args,
            dag=dag,
        )

wps = DockerOperator(
    task_id='wps',
    command=''
)

gfs_data_download = SubDagOperator(
    task_id='gfs_download',
    subdag=subdags.get_gfs_download_subdag(wrf_dag_name, 'gfs_download', default_args,
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
    subdag=subdags.get_wrf_run_subdag(wrf_dag_name, 'real_wrf', 4, default_args, wrf_config_key_prefix,
                                      test_mode=test_mode),
    default_args=default_args,
    dag=dag,
)

initialize_params >> [gfs_data_download, geogrid, ungrib]

gfs_data_download >> ungrib

metgrid << [geogrid, ungrib]

metgrid >> real_wrf
