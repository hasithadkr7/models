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

import airflow
from airflow import DAG
from datetime import timedelta

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
from curw.workflow.airflow.extensions.operators.curw_dag_run_operator import CurwDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
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
    'prod-wf',
    default_args=default_args,
    schedule_interval=None)

wf1 = CurwDagRunOperator(
    task_id='run-wf1',
    trigger_dag_id='wf1',
    dag=dag
)

wf2 = CurwDagRunOperator(
    task_id='run-wf2',
    trigger_dag_id='wf2',
    dag=dag
)

wf3_config = {
    'a': 'test',
    'b': '{{ts}}',
}

wf3 = CurwDagRunOperator(
    task_id='run-wf3',
    trigger_dag_id='wf3',
    dag_run_conf=wf3_config,
    dag=dag,
    # run_id='wf3-run',
    wait_for_completion=True,
)

wf1 >> wf2 >> wf3
