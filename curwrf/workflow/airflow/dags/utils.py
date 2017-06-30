from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from curwrf.wrf.execution.tasks import download_inventory


def get_gfs_download_subdag(parent_dag_name, child_dag_name, args, wrf_home, wrf_config):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )

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
