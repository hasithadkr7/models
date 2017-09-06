import airflow
import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


sat_dag_name = 'sat_rf_extraction'
# queue = 'wrf_fs_impl_queue'
schedule_interval = '@hourly'
curw_py_dir = '/opt/git/models'
output_dir = '/mnt/disks/curwsl_nfs/sat'
# curw_py_dir = '/home/curw/git/models'
# output_dir = '/home/curw/temp'

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 6,
    'retry_delay': dt.timedelta(minutes=15),
    # 'queue': queue,
}

# initiate the DAG
dag = DAG(
    sat_dag_name,
    default_args=default_args,
    description='Extracting JAXA satellite rainfall data hourly',
    schedule_interval=schedule_interval)

cmd_str = 'python3 %s/curwrf/wrf/extraction/sat_extractor.py ' \
          '-end {{ macros.datetime.strftime(execution_date + macros.timedelta(hours=1), \'%%Y-%%m-%%d_%%H:%%M\') }} ' \
          '-start {{ execution_date.strftime(\'%%Y-%%m-%%d_%%H:%%M\') }} ' \
          '-output %s' % (curw_py_dir, output_dir)

BashOperator(
    task_id='sat_rf_extraction',
    bash_command=cmd_str,
    default_args=default_args,
    dag=dag
)
