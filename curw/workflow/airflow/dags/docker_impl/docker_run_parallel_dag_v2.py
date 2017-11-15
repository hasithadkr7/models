import datetime as dt
import random
import string

from airflow.models import Variable

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from curw.workflow.airflow.dags import utils as dag_utils
from curw.workflow.airflow.extensions.operators.curw_docker_operator import CurwDockerOperator

wrf_dag_name = 'docker_wrf_run_v4'
wrf_config_key = 'docker_wrf_config'
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

image = 'nirandaperera/curw-wrf-391'
geog_dir = "/mnt/disks/workspace1/wrf-data/geog"
curw_nfs = 'curwsl_nfs_1'
curw_archive = 'curwsl_archive_1'

docker_volumes = ['%s:/wrf/geog' % geog_dir]
gcs_volumes = ' -v %s:/wrf/output -v %s:/wrf/archive' % (curw_nfs, curw_archive)

test_mode = False


def get_run_id(run_name, suffix=None):
    return run_name + '_' + '{{ execution_date.strftime(\'%%Y-%%m-%%d_%%H:%%M\') }}' + (
        ('_' + suffix) if suffix else '')


def id_generator(size=4, chars=string.ascii_uppercase + string.digits + string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))


def get_docker_cmd(run_id, wrf_config, mode, nl_wps, nl_input):
    cmd = '/wrf/run_wrf.sh -i \"%s\" -c \"%s\" -m \"%s\" -x \"%s\" -y \"%s\"' % (
        run_id, wrf_config, mode, nl_wps, nl_input)
    return cmd


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
    description='Running 5 WRFs simultaneously using docker',
    schedule_interval=schedule_interval)

namelist_tar = Variable.get(namelists_path_key)


for i in range()
wps = CurwDockerOperator(
    task_id='wps',
    image=image,
    command=get_docker_cmd(run_id, '{{ var.json.%s }}' % wrf_config_key, 'wps', '{{ var.value.%s }}' % namelist_wps_key,
                           '{{ var.value.%s }}' % namelist_input_key),
    cpus=1,
    volumes=docker_volumes,
    auto_remove=True,
    dag=dag
)

wrf = CurwDockerOperator(
    task_id='wrf',
    image=image,
    command=get_docker_cmd(run_id, '{{ var.json.%s }}' % wrf_config_key, 'wrf', '{{ var.value.%s }}' % namelist_wps_key,
                           '{{ var.value.%s }}' % namelist_input_key),
    cpus=2,
    volumes=docker_volumes,
    auto_remove=True,
    dag=dag
)

initialize_params >> wps >> wrf
