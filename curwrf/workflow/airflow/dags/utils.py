import datetime
import json
import os

import logging
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

from curwrf.wrf import constants
from curwrf.wrf.execution.executor import WrfConfig
from curwrf.wrf.execution.tasks import download_inventory
from curwrf.wrf.execution import executor as wrf_exec


def get_gfs_download_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )

    wrf_config = WrfConfig(configs=Variable.get('wrf_config', deserialize_json=True))

    period = wrf_config.get('period')
    step = wrf_config.get('gfs_step')

    for i in range(0, period * 24 + 1, step):
        PythonOperator(
            python_callable=download_inventory.download_i_th_inventory,
            task_id='%s-task-%s' % (child_dag_name, i),
            op_args=[i, wrf_config],
            provide_context=True,
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag


def set_initial_parameters(**kwargs):
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
        wrf_config_dict = Variable.get('wrf_config', deserialize_json=True)
        logging.info('wrf_config Variable: ' + str(wrf_config_dict))
        wrf_config = WrfConfig(configs=wrf_config_dict)
    except KeyError as e:
        logging.warning('Key Error: ' + str(e))
        wrf_config = wrf_exec.get_wrf_config(wrf_home)

    # set start_date --> wrf_start_date var > execution_date param in the workflow > today
    start_date = None
    try:
        start_date = Variable.get('wrf_start_date')
    except KeyError as e1:
        logging.warning('wrf_start_date variable is not available. execution_date will be used - %s' % str(e1))
        logging.info(kwargs)
        try:
            start_date = kwargs['execution_date'].strftime('%Y-%m-%d_%H:%M')
        except KeyError as e2:
            logging.warning('execution_date is not available - %s' % str(e2))

    if start_date is not None:
        wrf_config.set('start_date', start_date)
        Variable.set('wrf_config', wrf_config.to_json_string())
