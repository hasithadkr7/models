import datetime as dt
import json
import logging
import os

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from curw.rainfall.wrf import utils
from curw.workflow.airflow.dags.docker_impl import utils as docker_utils
from curw.workflow.airflow.extensions.operators.curw_docker_operator import CurwDockerOperator

wrf_dag_name = 'docker_wrf_run_prod_cts_v1'
queue = 'docker_prod_queue'
schedule_interval = '0 18 * * *'

parallel_runs = 6
priorities = [1000, 1, 1, 1, 1, 1]

docker_url = 'tcp://10.128.0.5:2375'

# docker images
wrf_image = 'nirandaperera/curw-wrf-391'
extract_image = 'nirandaperera/curw-wrf-391-extract'

# volumes and mounts
curw_nfs = 'curwsl_nfs_1'
curw_archive = 'curwsl_archive_1'
geog_dir = "/mnt/disks/workspace1/wrf-data/geog"
config_dir = "/mnt/disks/workspace1/wrf-config"
docker_volumes = ['%s:/wrf/geog' % geog_dir, '%s:/wrf/config' % config_dir]
gcs_volumes = ['%s:/wrf/output' % curw_nfs, '%s:/wrf/archive' % curw_archive]

test_mode = False

# namelist keys
nl_wps_key = "nl_wps"
nl_inp_keys = ["nl_inp_SIDAT", "nl_inp_C", "nl_inp_H", "nl_inp_NW", "nl_inp_SW", "nl_inp_W"]

curw_gcs_key_path = 'curw_gcs_key_path'

curw_db_config_path = 'curw_db_config_path'

wrf_config_key = 'docker_wrf_config'
local_wrf_config_path = '/mnt/disks/workspace1/wrf-config/local-wrf-config.json'
wrf_pool = 'parallel_wrf_runs'
run_id_prefix = 'wrf-doc'

nl_inp_keys.extend([nl_wps_key, curw_gcs_key_path, curw_db_config_path])
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
    description='Running 6 WRFs simultaneously using docker continuously',
    schedule_interval=schedule_interval)


def initialize_config(local_config_path, run_id_task_name, configs_prefix='wrf_config_',
                      config_output_dir='/wrf/config', **context):
    run_id = context['task_instance'].xcom_pull(task_ids=run_id_task_name)

    with open(local_config_path, 'r') as local_wc:
        config = json.load(local_wc)

    for k, v in context['templates_dict'].items():
        if k.startswith(configs_prefix):
            config[k.replace(configs_prefix, '')] = v

    out_wc_path = os.path.join(os.path.dirname(local_wrf_config_path), run_id + '-wrf-config.json')

    try:
        with open(out_wc_path, 'w') as out_wc:
            json.dump(config, out_wc, sort_keys=True)
    except PermissionError as e:
        logging.error('Unable to write wrf_config ' + str(e))

    logging.info('Initialized wrf_config:\n' + json.dumps(config))

    xcom_val = os.path.join(config_output_dir, os.path.basename(out_wc_path))
    logging.info('Returned xcom: ' + xcom_val)
    # context['ti'].xcom_push(key='config', value=xcom_val)
    return xcom_val


def generate_random_run_id(prefix, random_str_len=4, **context):
    run_id = '_'.join(
        [prefix, context['execution_date'].strftime('%Y-%m-%d_%H:%M'), docker_utils.id_generator(size=random_str_len)])
    logging.info('Generated run_id: ' + run_id)
    return run_id


def clean_up_wrf_config(init_task_id, **context):
    run_id = context['task_instance'].xcom_pull(task_ids=init_task_id)
    wc_file = os.path.basename(run_id)
    wc_dir = os.path.dirname(local_wrf_config_path)
    wc_path = os.path.join(wc_dir, wc_file)

    with open(wc_path) as wc:
        wrf_config = json.loads(wc)
        metgrid_path = os.path.join(wrf_config.get('nfs_dir'), 'metgrid', run_id + '_metgrid.zip')
        if utils.file_exists_nonempty(metgrid_path):
            logging.info('Cleaning up metgrid ' + metgrid_path)
            os.remove(metgrid_path)
        else:
            logging.info('No metgrid file')

    logging.info('Cleaning up ' + wc_path)
    os.remove(wc_path)


def check_data_push_callable(task_ids, **context):
    exec_date = context['execution_date']
    if exec_date.hour == 18 and exec_date.minute == 0:
        return task_ids[0]
    else:
        return task_ids[1]


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
    op_args=[local_wrf_config_path, 'gen-run-id', 'wrf_config_', '/wrf/config'],
    templates_dict={
        'wrf_config_start_date': '{{ execution_date.strftime(\'%Y-%m-%d_%H:%M\') }}',
        'wrf_config_run_id': '{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
        'wrf_config_wps_run_id': '{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
    },
    dag=dag,
)

clean_up = PythonOperator(
    task_id='cleanup-config',
    python_callable=clean_up_wrf_config,
    provide_context=True,
    op_args=['init-config'],
    dag=dag,
)

wps = CurwDockerOperator(
    task_id='wps',
    image=wrf_image,
    docker_url=docker_url,
    command=docker_utils.get_docker_cmd('{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
                                        '{{ task_instance.xcom_pull(task_ids=\'init-config\') }}',
                                        'wps',
                                        airflow_vars[nl_wps_key],
                                        airflow_vars[nl_inp_keys[0]],
                                        airflow_vars[curw_gcs_key_path],
                                        gcs_volumes),
    cpus=1,
    volumes=docker_volumes,
    auto_remove=True,
    priviliedged=True,
    dag=dag,
    pool=wrf_pool,
)

generate_run_id >> init_config >> wps

select_wrf = DummyOperator(task_id='select-wrf', dag=dag)

for i in range(parallel_runs):
    generate_run_id_wrf = PythonOperator(
        task_id='gen-run-id-wrf%d' % i,
        python_callable=generate_random_run_id,
        op_args=[run_id_prefix + str(i)],
        provide_context=True,
        dag=dag,
        priority_weight=priorities[i]
    )

    wrf = CurwDockerOperator(
        task_id='wrf%d' % i,
        image=wrf_image,
        docker_url=docker_url,
        command=docker_utils.get_docker_cmd('{{ task_instance.xcom_pull(task_ids=\'gen-run-id-wrf%d\') }}' % i,
                                            '{{ task_instance.xcom_pull(task_ids=\'init-config\') }}',
                                            'wrf',
                                            airflow_vars[nl_wps_key],
                                            airflow_vars[nl_inp_keys[i]],
                                            airflow_vars[curw_gcs_key_path],
                                            gcs_volumes),
        cpus=4,
        volumes=docker_volumes,
        auto_remove=True,
        priviliedged=True,
        dag=dag,
        pool=wrf_pool,
        priority_weight=priorities[i]
    )

    extract_wrf = CurwDockerOperator(
        task_id='wrf%d-extract' % i,
        image=extract_image,
        docker_url=docker_url,
        command=docker_utils.get_docker_extract_cmd('{{ task_instance.xcom_pull(task_ids=\'gen-run-id-wrf%d\') }}' % i,
                                                    '{{ task_instance.xcom_pull(task_ids=\'init-config\') }}',
                                                    airflow_vars[curw_db_config_path],
                                                    airflow_vars[curw_gcs_key_path],
                                                    gcs_volumes,
                                                    overwrite=False),
        cpus=4,
        volumes=docker_volumes,
        auto_remove=True,
        priviliedged=True,
        dag=dag,
        pool=wrf_pool,
        priority_weight=priorities[i]
    )

    extract_wrf_no_data_push = CurwDockerOperator(
        task_id='wrf%d-extract-no-data-push' % i,
        image=extract_image,
        docker_url=docker_url,
        command=docker_utils.get_docker_extract_cmd('{{ task_instance.xcom_pull(task_ids=\'gen-run-id-wrf%d\') }}' % i,
                                                    '{{ task_instance.xcom_pull(task_ids=\'init-config\') }}',
                                                    '{}',
                                                    airflow_vars[curw_gcs_key_path],
                                                    gcs_volumes,
                                                    overwrite=False),
        cpus=4,
        volumes=docker_volumes,
        auto_remove=True,
        priviliedged=True,
        dag=dag,
        pool=wrf_pool,
        priority_weight=priorities[i]
    )

    check_data_push = BranchPythonOperator(
        task_id='check-data-push' + str(i),
        python_callable=check_data_push_callable,
        provide_context=True,
        op_args=[['wrf%d-extract' % i, 'wrf%d-extract-no-data-push' % i]],
        dag=dag,
        priority_weight=priorities[i]
    )

    join_branch = DummyOperator(
        task_id='join-branch' + str(i),
        trigger_rule='one_success',
        dag=dag,
        priority_weight=priorities[i]
    )

    wps >> generate_run_id_wrf >> wrf >> check_data_push
    check_data_push >> extract_wrf >> join_branch
    check_data_push >> extract_wrf_no_data_push >> join_branch
    join_branch >> select_wrf

select_wrf >> clean_up
