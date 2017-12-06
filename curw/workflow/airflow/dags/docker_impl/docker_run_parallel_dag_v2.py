import datetime as dt
import json

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from curw.workflow.airflow.dags.docker_impl import utils as docker_utils
from curw.workflow.airflow.extensions.operators.curw_docker_operator import CurwDockerOperator

wrf_dag_name = 'docker_wrf_run_v4'
queue = 'wrf_docker_queue'
schedule_interval = None

parallel_runs = 6

# docker images
wrf_image = 'nirandaperera/curw-wrf-391'
extract_image = 'nirandaperera/curw-wrf-391-extract'

# volumes and mounts
curw_nfs = 'curwsl_nfs_1'
curw_archive = 'curwsl_archive_1'
geog_dir = "/mnt/disks/workspace1/wrf-data/geog"
docker_volumes = ['%s:/wrf/geog' % geog_dir]
gcs_volumes = ['%s:/wrf/output' % curw_nfs, '%s:/wrf/archive' % curw_archive]

test_mode = False

# namelist keys
nl_wps_key = "nl_wps"
nl_inp_keys = ["nl_inp_SIDAT", "nl_inp_C", "nl_inp_H", "nl_inp_NW", "nl_inp_SW", "nl_inp_W"]

curw_gcs_key_path = 'curw_gcs_key_path'

curw_db_config_path = 'curw_db_config_path'

wrf_config_key = 'docker_wrf_config'
wrf_pool = 'parallel_wrf_runs'
run_id_prefix = 'wrf-doc'
run_id_suffix = docker_utils.id_generator(size=4)

nl_inp_keys.extend([nl_wps_key, curw_gcs_key_path])
airflow_vars = docker_utils.check_airflow_variables(nl_inp_keys, ignore_error=True)

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

    context['ti'].xcom_push(key='wrf_config', value=json.dumps(config, sort_keys=True))


init = PythonOperator(
    task_id='init-config',
    python_callable=initialize_config,
    provide_context=True,
    templates_dict={
        'wrf_config': '{{ var.json.%s }}' % wrf_config_key,
        'kv_dict': {
            'start_date': docker_utils.get_exec_date_template(),
            'run_id': docker_utils.get_run_id(run_id_prefix, suffix=run_id_suffix),
            'wps_run_id': docker_utils.get_run_id(run_id_prefix, suffix=run_id_suffix),
        }
    },
    dag=dag,
)

wps = CurwDockerOperator(
    task_id='wps',
    image=wrf_image,
    command=docker_utils.get_docker_cmd(docker_utils.get_run_id(run_id_prefix, suffix=run_id_suffix),
                                        '{{ task_instance.xcom_pull(task_ids=\'init-config\', key=\'wrf_config\') }}',
                                        'wps',
                                        airflow_vars[nl_wps_key],
                                        airflow_vars[nl_inp_keys[0]],
                                        docker_utils.read_file(airflow_vars[curw_gcs_key_path], ignore_errors=True),
                                        gcs_volumes),
    cpus=1,
    volumes=docker_volumes,
    auto_remove=True,
    priviliedged=True,
    dag=dag,
    pool=wrf_pool,
)

wrf = CurwDockerOperator(
    task_id='wrf',
    image=wrf_image,
    command=docker_utils.get_docker_cmd(docker_utils.get_run_id(run_id_prefix + '0', suffix=run_id_suffix),
                                        '{{ task_instance.xcom_pull(task_ids=\'init-config\', key=\'wrf_config\') }}',
                                        'wrf',
                                        airflow_vars[nl_wps_key],
                                        airflow_vars[nl_inp_keys[0]],
                                        docker_utils.read_file(airflow_vars[curw_gcs_key_path], ignore_errors=True),
                                        gcs_volumes),
    cpus=2,
    volumes=docker_volumes,
    auto_remove=True,
    priviliedged=True,
    dag=dag,
    pool=wrf_pool,
)

init >> wps >> wrf
