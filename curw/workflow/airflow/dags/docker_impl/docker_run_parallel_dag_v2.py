import ast
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


def initialize_config(config_key='wrf_config', configs_prefix='wrf_config_', **context):
    print(context)
    print(context['templates_dict'])
    config = ast.literal_eval(context['templates_dict'][config_key])
    for k, v in context['templates_dict'].items():
        if k.startswith(configs_prefix):
            config[k.replace(configs_prefix, '')] = v

    context['ti'].xcom_push(key='wrf_config', value=json.dumps(config, sort_keys=True))


def generate_random_run_id(prefix, random_str_len=4, **context):
    return '_'.join(
        [prefix, context['execution_date'].strftime('%Y-%m-%d_%H:%M'), docker_utils.id_generator(size=random_str_len)])


generate_run_id = PythonOperator(
    task_id='gen-run-id',
    python_callable=generate_random_run_id,
    op_args=[run_id_prefix],
    provide_context=True,
    dag=dag
)

init_config = PythonOperator(
    task_id='init-config',
    python_callable=initialize_config,
    provide_context=True,
    op_args=['wrf_config', 'wrf_config_'],
    templates_dict={
        'wrf_config': '{{ var.json.%s }}' % wrf_config_key,
        'wrf_config_start_date': '{{ execution_date.strftime(\'%Y-%m-%d_%H:%M\') }}',
        'wrf_config_run_id': '{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
        'wrf_config_wps_run_id': '{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
    },
    dag=dag,
)

wps = CurwDockerOperator(
    task_id='wps',
    image=wrf_image,
    command=docker_utils.get_docker_cmd('{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
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


generate_run_id_wrf0 = PythonOperator(
    task_id='gen-run-id-wrf0',
    python_callable=generate_random_run_id,
    op_args=[run_id_prefix + '0'],
    provide_context=True,
    dag=dag
)


wrf = CurwDockerOperator(
    task_id='wrf',
    image=wrf_image,
    command=docker_utils.get_docker_cmd('{{ task_instance.xcom_pull(task_ids=\'gen-run-id-wrf0\') }}',
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

generate_run_id >> init_config >> wps >> generate_run_id_wrf0 >> wrf
