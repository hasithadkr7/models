import argparse
import json
import logging
import os
import random
import string

from curwrf.wrf import utils
from curwrf.wrf.execution import executor


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-run_id', default=id_generator())
    parser.add_argument('-wrf_home', default='/wrf')
    parser.add_argument('-mode', default='wps')
    parser.add_argument('-nl_wps', default=None)
    parser.add_argument('-nl_input', default=None)
    parser.add_argument('-output_dir', default='/wrf/output')
    parser.add_argument('-geog_dir', default='/wrf/geog')
    parser.add_argument('-start', default=None)
    parser.add_argument('-period', default=3.0, type=float)
    parser.add_argument('-gfs_dir', default='/wrf/gfs')

    return parser.parse_args()


def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def run_wrf(wrf_config):
    logging.info('Running WRF')

    logging.info('Replacing the namelist input file')
    executor.replace_namelist_input(wrf_config)

    logging.info('Running WRF...')
    executor.run_em_real(wrf_config)


def run_wps(wrf_config):
    logging.info('Downloading GFS data')
    executor.download_gfs_data(wrf_config)

    logging.info('Replacing the namelist wps file')
    executor.replace_namelist_wps(wrf_config)

    logging.info('Running WPS...')
    executor.run_wps(wrf_config)


def get_env_vars(prefix):
    return {k.replace(prefix, ''): v for (k, v) in os.environ.items() if prefix in k}


class CurwDockerException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    # args = parse_args()
    env_vars = get_env_vars('CURW_')

    run_id = env_vars.pop('run_id', id_generator())
    logging.info('**** WRF RUN **** Run ID: ' + run_id)

    mode = env_vars.pop('mode').strip().lower()
    nl_wps = env_vars.pop('nl_wps', None)
    nl_input = env_vars.pop('nl_input', None)
    wrf_config_dict = json.loads(env_vars.pop('wrf_config', '{}'))

    config = executor.get_wrf_config(**wrf_config_dict)
    config.set('run_id', run_id)

    wrf_home = config.get('wrf_home')

    if nl_wps is not None:
        logging.info('Reading namelist wps')
        nl_wps_path = os.path.join(wrf_home, 'namelist.wps')
        content = nl_wps.replace('\\n', '\n')
        logging.info('namelist.wps content: \n%s' % content)
        with open(nl_wps_path, 'w') as f:
            f.write(content)
            f.write('\n')
        config.set('namelist_wps', nl_wps_path)

    if nl_input is not None:
        logging.info('Reading namelist input')
        nl_input_path = os.path.join(wrf_home, 'namelist.input')
        content = nl_input.replace('\\n', '\n')
        logging.info('namelist.input content: \n%s' % content)
        with open(nl_input_path, 'w') as f:
            f.write(content)
            f.write('\n')
        config.set('namelist_input', nl_input_path)

    logging.info('WRF config: %s' % config.to_json_string())

    logging.info('Backup the output dir')
    utils.backup_dir(os.path.join(config.get('nfs_dir'), run_id, 'results'))

    if mode == 'wps':
        logging.info('Running WPS')
        run_wps(config)
    elif mode == 'wrf':
        logging.info('Running WRF')
        run_wrf(config)
    elif mode == "all":
        logging.info("Running both WPS and WRF")
        run_wps(config)
        run_wrf(config)
    else:
        raise CurwDockerException('Unknown mode ' + mode)
