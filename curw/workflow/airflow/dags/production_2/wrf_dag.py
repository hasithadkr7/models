import datetime as dt
import json
import logging
import os

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from curw.rainfall.wrf import utils
from curw.container.docker.rainfall import utils as docker_rf_utils
from curw.workflow.airflow.dags.docker_impl import utils as airflow_docker_utils
from curw.workflow.airflow.extensions.operators.curw_docker_operator import CurwDockerOperator

"""
Configurations
--------------

run_id
model run ID.  Format: [model tag]_[execution timestamp]_[random chars] Ex: wrf0_2017-05-26_00:00_0000/

wrf_config_b64
WRF config json object. BASE64 encoded str. Refer [1]

mode 
Mode. [wps/wrf/all] str Default:all 

namelist_wps_b64
namelist.wps content. Can use place-holders for templating. Check the namelist.wps file in the GIT 
repository.  BASE64 encoded str

namelist_input_b64 
namelist.input content. Can use place-holders for templating. Check the namelist.input file in the GIT repository.  
BASE64 encoded str 

gcs_key_b64
GCS service account key file path. str/ key content.  BASE64 encoded str 

gcs_vol_mounts
GCS bucket volume mounts. bucket_name:mount_path Ex: ["curwsl_nfs_1:/wrf/output", "curwsl_archive_1:/wrf/archive"]. 
array[str] 

"""

wrf_config = {
    "wrf_home": "/wrf",
    "gfs_dir": "/wrf/gfs",
    "nfs_dir": "/wrf/output",
    "geog_dir": "/wrf/geog",
    "archive_dir": "/wrf/archive",
    "procs": 4,
    "period": 3,
}


def generate_random_run_id(prefix, random_str_len=4, **context):
    run_id = '_'.join(
        [prefix, airflow_docker_utils.get_start_date_from_context(context).strftime('%Y-%m-%d_%H:%M'),
         airflow_docker_utils.id_generator(size=random_str_len)])
    logging.info('Generated run_id: ' + run_id)
    return run_id


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

dag = DAG(
    'wrf-dag-v1',
    default_args=default_args,
    schedule_interval=None)
