import argparse
import os
import random
import string


def get_env_vars(prefix):
    return {k.replace(prefix, ''): v for (k, v) in os.environ.items() if prefix in k}


def get_var(var, env_vars, args, default=None):
    val = env_vars.pop(var, None)
    if val is None:
        return args.pop(var, default)
    return val


def parse_args():
    parser = argparse.ArgumentParser()
    env_vars = get_env_vars('CURW_')

    parser.add_argument('-run_id', default=env_vars['run_id'] if 'run_id' in env_vars else id_generator())
    parser.add_argument('-mode', default=env_vars['mode'] if 'mode' in env_vars else 'wps')
    parser.add_argument('-nl_wps', default=env_vars['nl_wps'] if 'nl_wps' in env_vars else None)
    parser.add_argument('-nl_input', default=env_vars['nl_input'] if 'nl_input' in env_vars else None)
    parser.add_argument('-wrf_config', default=env_vars['wrf_config'] if 'wrf_config' in env_vars else '{}')

    return parser.parse_args()


def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


class CurwDockerRainfallException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)