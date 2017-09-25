import datetime as dt
import airflow
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from curwrf.workflow.airflow.extensions import tasks, subdags
from curwrf.workflow.airflow.extensions.operators import CurwPythonOperator
from curwrf.workflow.airflow.dags import utils as dag_utils

wrf_dag_name = 'docker_wrf_run'
wrf_config_key = 'docker_wrf_config'
namelist_wps_key = 'docker_namelist_wps'
namelist_input_key = 'docker_namelist_input'
wrf_home_key = 'wrf_home'
wrf_start_date_key = 'wrf_start_date'
queue = 'wrf_docker_queue'
schedule_interval = '@once'

git_path = '/opt/git/models'

test_mode = False


def get_docker_cmd(git, wrf_config, mode, nl_wps, nl_input):
    # python3 run_wrf.py -wrf_config="$CURW_wrf_config" -mode="$CURW_mode" -nl_wps="$CURW_nl_wps" -nl_input="$CURW_nl_input"

    return 'python3 %s/run_wrf.py -wrf_config=\"%s\" -mode=\"%s\" -nl_wps=\"%s\" -nl_input=\"%s\"' % (
        git, wrf_config, mode, nl_wps, nl_input)


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
    op_args=[wrf_home_key, wrf_start_date_key, wrf_config_key, True],
    default_args=default_args,
    dag=dag,
)

wps = DockerOperator(
    task_id='wps',
    command=get_docker_cmd(git_path, '{{ var.json.%s }}' % wrf_config_key, 'wps',
                           '{{ var.value.%s }}' % namelist_wps_key, '{{ var.value.%s }}' % namelist_input_key)
)
