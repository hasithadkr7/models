import datetime as dt
import random
import string

from airflow.operators.dummy_operator import DummyOperator

from airflow.models import Variable, Connection

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from curw.workflow.airflow.dags.docker_impl import utils as docker_utils
from curw.workflow.airflow.extensions.operators.curw_docker_operator import CurwDockerOperator

wrf_dag_name = 'docker_wrf_run_v4'
namelist_wps_key = 'docker_namelist_wps'
namelist_input_key = 'docker_namelist_input'

parallel_runs = 5

db_config_path_key = 'db_config_path'
gcs_json_path_key = 'gcs_json_path'
namelists_path_key = 'namelists_path'

# wrf_home_key = 'docker_wrf_home'
# wrf_start_date_key = 'docker_wrf_start_date'

queue = 'wrf_docker_queue'
schedule_interval = '0 18 * * *'

wrf_image = 'nirandaperera/curw-wrf-391'
extract_image = 'nirandaperera/curw-wrf-391-extract'
geog_dir = "/mnt/disks/workspace1/wrf-data/geog"
curw_nfs = 'curwsl_nfs_1'
curw_archive = 'curwsl_archive_1'

docker_volumes = ['%s:/wrf/geog' % geog_dir]
gcs_volumes = ' -v %s:/wrf/output -v %s:/wrf/archive' % (curw_nfs, curw_archive)

test_mode = False

nl_inp_keys = ["nl_inp_SIDAT", "nl_inp_C", "nl_inp_H", "nl_inp_NW", "nl_inp_SW", "nl_inp_W"]
nl_wps_key = "nl_wps"
curw_gcs_key_path = 'curw_gcs_key_path'

curw_db_config_path = 'curw_db_config_path'
wrf_config_key = 'docker_wrf_config'

airflow_vars = docker_utils.check_airflow_variables(nl_inp_keys.extend([nl_wps_key, curw_gcs_key_path]),
                                                    ignore_error=True)

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
    description='Running 6 WRFs simultaneously using docker',
    schedule_interval=schedule_interval)


def initialize_config(**context):
    config = context['templates_dict']['wrf_config']
    kv = context['templates_dict']['kv_dict']
    config.update(kv)

    context['ti'].xcom_push[]


init = DummyOperator()

for i in range(parallel_runs):
    pass

wps = CurwDockerOperator(
    task_id='wps',
    image=wrf_image,
    command=docker_utils.get_docker_cmd(run_id, '{{ var.json.%s }}' % wrf_config_key, 'wps',
                                        '{{ var.value.%s }}' % namelist_wps_key,
                                        '{{ var.value.%s }}' % namelist_input_key),
    cpus=1,
    volumes=docker_volumes,
    auto_remove=True,
    dag=dag
)

wrf = CurwDockerOperator(
    task_id='wrf',
    image=wrf_image,
    command=docker_utils.get_docker_cmd(run_id, '{{ var.json.%s }}' % wrf_config_key, 'wrf',
                                        '{{ var.value.%s }}' % namelist_wps_key,
                                        '{{ var.value.%s }}' % namelist_input_key),
    cpus=2,
    volumes=docker_volumes,
    auto_remove=True,
    dag=dag
)
