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

import datetime as dt
import time
from datetime import datetime

from airflow import settings
from airflow.models import BaseOperator, DagBag
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class CurwDagRunOperator(BaseOperator):
    """

    """
    template_fields = ('dag_run_conf', 'run_id', 'trigger_dag_id')
    template_ext = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            run_id=None,
            dag_run_conf=None,
            poll_interval=dt.timedelta(seconds=1),
            wait_for_completion=False,
            *args, **kwargs):
        super(CurwDagRunOperator, self).__init__(*args, **kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.dag_run_conf = dag_run_conf
        self.poll_interval = poll_interval
        self.wait_for_completion = wait_for_completion
        self.run_id = run_id

    def execute(self, context):
        execution_time = datetime.utcnow()
        self.log.info("Execution time %s", execution_time.isoformat())

        if not self.run_id:
            self.run_id = 'curw_trig__' + execution_time.isoformat()
        self.log.info("DagRun Run ID %s", self.run_id)

        dbag = DagBag(settings.DAGS_FOLDER)
        trigger_dag = dbag.get_dag(self.trigger_dag_id)

        dagrun = trigger_dag.create_dagrun(
            run_id=self.run_id,
            state=State.RUNNING,
            execution_date=execution_time,
            conf=self.dag_run_conf,
            external_trigger=True)
        self.log.info("Created DagRun %s \nConfig: %s" % (dagrun, str(self.dag_run_conf)))

        while self.wait_for_completion and dagrun.get_state() is State.RUNNING:
            self.log.info('Waiting for DagRun completion')
            dagrun.update_state()
            time.sleep(self.poll_interval.seconds)

        #
        # dbag = DagBag(settings.DAGS_FOLDER)
        # trigger_dag = dbag.get_dag(self.trigger_dag_id)
        # ed = context['execution_date']
        # trigger_dag.run(
        #     start_date=ed, end_date=ed, donot_pickle=True,
        #     executor=GetDefaultExecutor())
