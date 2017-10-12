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


def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


class CurwDockerRainfallException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)