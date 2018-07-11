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
import collections
import datetime as dt
import json
import time
from datetime import datetime

from airflow import settings
from airflow.models import BaseOperator, DagBag
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class CurwDagRunOperator(BaseOperator):
    """

    :param trigger_dag_id:
    :type str
    :param run_id:
    :type str
    :param dag_run_conf:
    :type typing.Union[str, dict]
    :param poll_interval:
    :type timedelta
    :param wait_for_completion::
    :type bool
    """
    template_fields = ('dag_run_conf_templates_dict', 'run_id', 'trigger_dag_id')
    template_ext = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            run_id=None,
            dag_run_conf=None,
            dag_run_conf_templates_dict=None,
            poll_interval=dt.timedelta(seconds=1),
            wait_for_completion=False,
            *args, **kwargs):
        super(CurwDagRunOperator, self).__init__(*args, **kwargs)
        self.trigger_dag_id = trigger_dag_id

        self.dag_run_conf = {}
        if dag_run_conf:
            if isinstance(dag_run_conf, str):
                self.dag_run_conf = json.loads(dag_run_conf)
            elif isinstance(dag_run_conf, dict):
                self.dag_run_conf = dag_run_conf
            else:
                raise CurwDagRunOperatorException('Unsupported dag_run_conf type %s' % type(dag_run_conf))

        self.poll_interval = poll_interval
        self.wait_for_completion = wait_for_completion
        self.run_id = run_id
        self.dag_run_conf_templates_dict = dag_run_conf_templates_dict

    def execute(self, context):
        execution_time = datetime.utcnow()
        self.log.info("Execution time %s", execution_time.isoformat())

        if not self.run_id:
            self.log.warning('Run ID not set. Auto-generating...')
            self.run_id = 'curw_trig__' + execution_time.isoformat()
        self.log.info("DagRun Run ID %s", self.run_id)

        if self.dag_run_conf_templates_dict:
            self.log.info('Using the templates_dict for dag_run_conf...')
            self.dag_run_conf = self._dict_merge(self.dag_run_conf, self.dag_run_conf_templates_dict)
            self.log.info('Templated dag_run_conf:\n%s' % str(self.dag_run_conf))

        dbag = DagBag(settings.DAGS_FOLDER)
        trigger_dag = dbag.get_dag(self.trigger_dag_id)

        self.log.info("Creating DagRun %s \nConfig: %s" % (self.run_id, str(self.dag_run_conf)))
        try:
            dagrun = trigger_dag.create_dagrun(
                run_id=self.run_id,
                state=State.RUNNING,
                execution_date=execution_time,
                conf=self.dag_run_conf,
                external_trigger=True)
        except Exception as e:
            raise CurwDagRunOperatorException(
                'Unable to create DagRun %s_%s' % (self.trigger_dag_id, self.run_id)) from e

        attempt = 1
        while self.wait_for_completion and (dagrun.get_state() == State.RUNNING):
            self.log.info('Waiting %d s for DagRun completion. Attempt %d' % (self.poll_interval.seconds, attempt))
            dagrun.refresh_from_db()
            attempt += 1
            time.sleep(self.poll_interval.seconds)

        return self.run_id

    def _dict_merge(self, dct, merge_dct, add_keys=True):
        """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
        updating only top-level keys, dict_merge recurses down into dicts nested
        to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
        ``dct``.

        This version will return a copy of the dictionary and leave the original
        arguments untouched.

        The optional argument ``add_keys``, determines whether keys which are
        present in ``merge_dict`` but not ``dct`` should be included in the
        new dict.

        Args:
            dct (dict) onto which the merge is executed
            merge_dct (dict): dct merged into dct
            add_keys (bool): whether to add new keys

        Returns:
            dict: updated dict
        """
        dct = dct.copy()
        if not add_keys:
            merge_dct = {
                k: merge_dct[k]
                for k in set(dct).intersection(set(merge_dct))
            }

        for k, v in merge_dct.items():
            if (k in dct and isinstance(dct[k], dict)
                    and isinstance(merge_dct[k], collections.Mapping)):
                dct[k] = self._dict_merge(dct[k], merge_dct[k], add_keys=add_keys)
            else:
                dct[k] = merge_dct[k]

        return dct


class CurwDagRunOperatorException(Exception):
    pass
