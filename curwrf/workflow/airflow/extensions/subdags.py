import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from curwrf.workflow.airflow.dags import utils as dag_utils
from curwrf.workflow.airflow.extensions import tasks
from curwrf.workflow.airflow.extensions.operators import CurwPythonOperator
from curwrf.wrf import utils
from curwrf.wrf.execution.executor import WrfConfig
from curwrf.wrf.execution.tasks import download_inventory


def get_gfs_download_subdag(parent_dag_name, child_dag_name, args, wrf_config_key='wrf_config', test_mode=False):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )

    try:
        wrf_config = WrfConfig(configs=Variable.get(wrf_config_key, deserialize_json=True))
    except KeyError as e:
        logging.error('Key error %s' % str(e))
        return dag_subdag

    period = wrf_config.get('period')
    step = wrf_config.get('gfs_step')

    try:
        gfs_date, gfs_cycle, start = utils.get_appropriate_gfs_inventory(wrf_config)
    except KeyError as e:
        # raise WrfRunException(str(e))
        logging.error('Unable to find the key: %s. Returining an empty subdag' % str(e))
        return dag_subdag

    for i in range(int(start), int(start) + period * 24 + 1, step):
        PythonOperator(
            python_callable=download_inventory.download_i_th_inventory,
            task_id='%s-task-%s' % (child_dag_name, i),
            op_args=[i, wrf_config.get('gfs_url'), wrf_config.get('gfs_inv'), gfs_date, gfs_cycle,
                     wrf_config.get('gfs_res'), wrf_config.get('gfs_dir'), wrf_config.get('nfs_dir'), test_mode],
            # provide_context=True,
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag


def get_initial_parameters_subdag(parent_dag_name, child_dag_name, runs, args, wrf_home_key, wrf_start_date_key,
                                  wrf_config_key):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )

    for i in [str(x) for x in range(runs)]:
        PythonOperator(
            task_id='%s-task-%s' % (child_dag_name, i),
            python_callable=dag_utils.set_initial_parameters_fs,
            provide_context=True,
            op_args=[wrf_home_key + i, wrf_start_date_key + i, wrf_config_key + i],
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag


def get_wrf_run_subdag(parent_dag_name, child_dag_name, runs, args, wrf_config_key, test_mode=False):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )

    for i in [str(x) for x in range(runs)]:
        real = CurwPythonOperator(
            task_id='%s-task-%s-%s' % (child_dag_name, 'real', i),
            curw_task=tasks.Real,
            init_args=[wrf_config_key + i],
            provide_context=True,
            default_args=args,
            dag=dag_subdag,
            test_mode=test_mode
        )

        wrf = CurwPythonOperator(
            task_id='%s-task-%s-%s' % (child_dag_name, 'wrf', i),
            curw_task=tasks.Wrf,
            init_args=[wrf_config_key + i],
            provide_context=True,
            default_args=args,
            dag=dag_subdag,
            test_mode=test_mode
        )

        real >> wrf

    return dag_subdag
