import datetime as dt
import json
import logging

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from curw.workflow.airflow.dags.docker_impl import utils as airflow_docker_utils
from curw.container.docker.rainfall import utils as docker_rf_utils
from curw.workflow.airflow.dags.production_2 import WrfDefaults
from curw.workflow.airflow.extensions.operators.curw_docker_operator import CurwDockerOperator
from curw.workflow.airflow import utils as af_utils

"""
Configurations
--------------

run_id
model run ID.  Format: [model tag]_[execution timestamp]_[random chars] Ex: wrf0_2017-05-26_00:00_0000/

wrf_config
WRF config json object. dict. Refer [1]

mode 
Mode. [wps/wrf/all] str Default:all 

namelist_wps_b64
namelist.wps content. Can use place-holders for templating. Check the namelist.wps file in the GIT 
repository.  BASE64 encoded str

namelist_input_b64 
namelist.input content. Can use place-holders for templating. Check the namelist.input file in the GIT repository.  
BASE64 encoded str 

gcs_key_b64
GCS service account key file path. str/ key content.  dict 

gcs_vol_mounts
GCS bucket volume mounts. bucket_name:mount_path Ex: ["curwsl_nfs_1:/wrf/output", "curwsl_archive_1:/wrf/archive"]. 
array[str] 

vol_mounts
File system volume mounts. source_dir:mount_path Ex: ["/mnt/disks/curwsl_nfs_1:/wrf/output"]. 
array[str] 

"""

DOCKER_URL = 'tcp://192.168.2.100:2375'
WRF_DOCKER_IMAGE = 'nirandaperera/curw-wrf-391'

VOL_MOUNTS = [
    "/samba/wrf-static-data/geog:/wrf/geog",
    "/samba/archive:/wrf/output",
    "/samba/archive:/wrf/archive"
]

WRF_POOL = 'parallel_wrf_runs'


class WrfDagException(Exception):
    pass


default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
    'catchup': False,
}

#
# def generate_run_id(prefix, **context):
#     run_id = prefix + '_' + context['next_execution_date'] if context['next_execution_date'] else context[
#         'execution_date']
#     logging.info('Generated run_id: ' + run_id)
#     return run_id

dag = DAG(
    'wrf-dag-v1',
    default_args=default_args,
    schedule_interval=None)


def get_dag_run_conf(**context):
    dr_conf = WrfDefaults.DAG_RUN_CONFIG
    if context and context['dag_run']:
        logging.info('dagrun: %s' % context['dag_run'])
        if context['dag_run'].conf:
            logging.info('dagrun conf %s' % context['dag_run'].conf)
            dr_conf.update(context['dag_run'].conf)
        else:
            logging.warning('dag_run.conf is missing. Using the default dag_run.config')
        logging.info('dag_run_conf returned: %s' % json.dumps(dr_conf))
    else:
        logging.warning(
            'context was not passed to the method or dag_run is missing in the context. '
            'Using the default dag_run.config..;')

    if not dr_conf['wrf_config']['start_date']:
        sd = context['next_execution_date'] if context['next_execution_date'] else context['execution_date']
        dr_conf['wrf_config']['start_date'] = sd.strftime('%Y-%m-%d_%H:%M')
        logging.info('Added dag_run.config.wrf_config.start_date: %s' % dr_conf['wrf_config']['start_date'])

    wc_str = json.dumps(dr_conf['wrf_config'])
    logging.info('Current wrf_config: %s' % wc_str)

    wrf_config_b64 = af_utils.get_base64_encoded_str(wc_str)
    dr_conf['wrf_config_b64'] = wrf_config_b64
    logging.info('Added dag_run.config.wrf_config_b64 : %s' % wrf_config_b64)

    return dr_conf


dag_run_conf = PythonOperator(
    task_id='dag_run_conf',
    python_callable=get_dag_run_conf,
    provide_context=True,
    dag=dag,
)

run_wrf = CurwDockerOperator(
    task_id='run_wrf',
    image=WRF_DOCKER_IMAGE,
    docker_url=DOCKER_URL,
    command=airflow_docker_utils.get_docker_cmd(
        run_id='{{ run_id }}',
        wrf_config='{{ ti.xcom_pull(task_ids=\'dag_run_conf\').wrf_config_b64 }}',
        mode='{{ti.xcom_pull(task_ids=\'dag_run_conf\').mode',
        nl_wps='{{ti.xcom_pull(task_ids=\'dag_run_conf\').namelist_wps_b64 }}',
        nl_input='{{ ti.xcom_pull(task_ids=\'dag_run_conf\').namelist_input_b64 }}'
    ),
    cpus=1,
    volumes=VOL_MOUNTS,
    auto_remove=True,
    privileged=True,
    dag=dag,
    pool=WRF_POOL,
    retries=3,
    retry_delay=dt.timedelta(minutes=10),
)

dag_run_conf >> dag_run_conf

# def get_mode(**context):
#     if context and context['templates_dict'] and context['templates_dict']['mode']:
#         m = context['templates_dict']['mode'].lower()
#         if m == 'wps':
#             return 'run_wps'
#         elif m == 'wrf':
#             return 'run_wrf'
#         elif m == 'all':
#             return 'run_all'
#         else:
#             raise WrfDagException('Unknown type: %s' % m)
#     else:
#         logging.warning(
#             'context was not passed to the method or dag_run is missing in the context. ')
#
#     return 'run_all'
