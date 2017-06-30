import datetime as dt
import json
import logging
import os

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from curwrf.workflow.airflow.dags import utils as dag_utils
from curwrf.wrf import constants
from curwrf.wrf.execution import executor as wrf_exec

WRF_DAG_NAME = 'wrf_run'
wrf_home = None
wrf_config = None

# set wrf_home --> wrf_home Var > WRF_HOME env var > wrf_home default
try:
    wrf_home = Variable.get('wrf_home')
except KeyError:
    try:
        wrf_home = os.environ['WRF_HOME']
    except KeyError:
        wrf_home = constants.DEFAULT_WRF_HOME

# set wrf_config --> wrf_config Var (YAML format) > get_wrf_config(wrf_home)
try:
    wrf_config_dict = json.loads(str(Variable.get('wrf_config')))
    print(wrf_config_dict)
    wrf_config = wrf_exec.get_wrf_config(wrf_config_dict.pop('wrf_home'), **wrf_config_dict)
except KeyError as e:
    logging.info('Key Error: ' + str(e))
    wrf_config = wrf_exec.get_wrf_config(wrf_home)

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# initiate the DAG
dag = DAG(
    WRF_DAG_NAME,
    default_args=default_args,
    description='Running WRF instantaneously',
    schedule_interval=None)

start = DummyOperator(
    task_id='start',
    default_args=default_args,
    dag=dag,
)

gfs_data_download = SubDagOperator(
    task_id='gfs_download',
    subdag=dag_utils.get_gfs_download_subdag(WRF_DAG_NAME, 'gfs_download', default_args, wrf_home, wrf_config),
    default_args=default_args,
    dag=dag,
)
start.set_downstream(gfs_data_download)

