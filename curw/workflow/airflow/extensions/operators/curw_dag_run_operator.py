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

from datetime import datetime

from airflow.models import BaseOperator, DagBag
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow import settings
import datetime as dt
from airflow.executors import GetDefaultExecutor



class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class CurwDagRunOperator(BaseOperator):
    """

    """
    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            *args, **kwargs):
        super(CurwDagRunOperator, self).__init__(*args, **kwargs)
        self.trigger_dag_id = trigger_dag_id

    def execute(self, context):
        # run_id='trig__' + datetime.utcnow().isoformat()
        #
        # session = settings.Session()
        # dbag = DagBag(settings.DAGS_FOLDER)
        # trigger_dag = dbag.get_dag(self.trigger_dag_id)
        # dr = trigger_dag.create_dagrun(
        #     run_id=run_id,
        #     state=State.RUNNING,
        #     execution_date=context['execution_date'] + dt.timedelta(microseconds=1) if context[
        #         'execution_date'] else None,
        #     external_trigger=True)
        # self.log.info("Creating DagRun %s", dr)
        # session.add(dr)
        # session.commit()
        # session.close()

        dbag = DagBag(settings.DAGS_FOLDER)
        trigger_dag = dbag.get_dag(self.trigger_dag_id)
        ed = context['execution_date']
        trigger_dag.run(
            start_date=ed, end_date=ed, donot_pickle=True,
            executor=GetDefaultExecutor())
