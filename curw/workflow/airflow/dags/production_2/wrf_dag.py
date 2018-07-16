import datetime as dt
import json
import logging

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from curw.workflow.airflow.dags.production_2 import WrfDefaults

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


def get_dag_run_conf(**kwargs):
    dr_conf = WrfDefaults.DAG_RUN_CONFIG
    if kwargs and kwargs['dag_run']:
        logging.info('dagrun: %s' % kwargs['dag_run'])
        if kwargs['dag_run'].conf:
            logging.info('dagrun conf %s' % kwargs['dag_run'].conf)
            dr_conf.update(kwargs['dag_run'].conf)
        else:
            logging.warning('dag_run.conf is missing. Using the default dag_run.config')
        logging.info('dag_run_conf returned: %s' % json.dumps(dr_conf))
    else:
        logging.error('context was not passed to the method or dag_run is missing in the context')
    return dr_conf


dag_run_conf = PythonOperator(
    task_id='get_dag_conf',
    python_callable=get_dag_run_conf,
    provide_context=True,
    dag=dag,
)
