import random
import string

import logging

from airflow.models import Variable


def get_run_id(run_name, suffix=None):
    return run_name + '_' + '{{ execution_date.strftime(\'%%Y-%%m-%%d_%%H:%%M\') }}' + (
        ('_' + suffix) if suffix else '')


def id_generator(size=4, chars=string.ascii_uppercase + string.digits + string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))


def get_docker_cmd(run_id, wrf_config, mode, nl_wps, nl_input):
    cmd = '/wrf/run_wrf.sh -i \"%s\" -c \"%s\" -m \"%s\" -x \"%s\" -y \"%s\"' % (
        run_id, wrf_config, mode, nl_wps, nl_input)
    return cmd


def check_airflow_variables(var_list, ignore_error=False, deserialize_json=False):
    out = {}
    for v in var_list:
        try:
            out[v] = Variable.get(v, deserialize_json=deserialize_json)
        except KeyError as e:
            logging.error('Key %s not found!' % v)
            out[v] = None
            if not ignore_error:
                raise e
    return out


def initialize_wrf_config(wrf_config, **kwargs):

