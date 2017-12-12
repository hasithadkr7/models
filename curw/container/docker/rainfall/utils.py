import argparse
import ast
import os
import random
import string

import logging

import json


def get_env_vars(prefix):
    return {k.replace(prefix, ''): v for (k, v) in os.environ.items() if prefix in k}


def get_var(var, env_vars, args, default=None):
    val = env_vars.pop(var, None)
    if val is None:
        return args.pop(var, default)
    return val


def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def get_config_dict(config_str):
    try:
        wrf_config_eval = ast.literal_eval(config_str)
        if isinstance(wrf_config_eval, dict):
            logging.info('Using config content')
            wrf_config_dict = wrf_config_eval
        else:
            logging.error('Unable to load config from content')
            raise Exception
    except (SyntaxError, ValueError):
        if os.path.isfile(config_str):
            logging.info('Using config path')
            with open(config_str, 'r') as f:
                wrf_config_dict = json.load(f)
        else:
            logging.error('Unable to load config from path')
            raise Exception
    except Exception:
        raise CurwDockerRainfallException('Unknown config: ' + config_str)

    return wrf_config_dict


class CurwDockerRainfallException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)