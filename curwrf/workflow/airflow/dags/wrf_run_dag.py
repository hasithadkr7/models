# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](http://pythonhosted.org/airflow/tutorial.html)
"""
import airflow
import datetime as dt
from airflow import DAG, macros
from datetime import timedelta

from curwrf.wrf.execution import executor as wrf_exec
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


def download_single_inventory_sub_dag(parent_name, child_name, url, dest):
    dag = DAG()

    pass


curr_ts = {{macros.datetime.now()}}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    'wrf_run',
    default_args=default_args,
    description='Running WRF',
    schedule_interval=timedelta(days=1))

wrf_config = wrf_exec.get_wrf_config('/home/nira/curw')


t1 = PythonOperator(
    task_id='download_gfs_data',
    python_callable=wrf_exec.download_gfs_data,
    op_args=[start, wrf_config],
    dag=dag)

