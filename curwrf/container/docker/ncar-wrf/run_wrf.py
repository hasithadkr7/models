import argparse
import logging
import os

from curwrf.workflow.airflow.extensions.tasks import Ungrib, Geogrid, Metgrid, Real, Wrf
from curwrf.wrf.execution import executor


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-wrf_home', default='/wrf')
    parser.add_argument('-mode', default='wps')
    parser.add_argument('-nl_wps', default=None)
    parser.add_argument('-nl_input', default=None)
    parser.add_argument('-output_dir', default='/wrf/output')
    parser.add_argument('-geog_dir', default='/wrf/geog')
    parser.add_argument('-start', default=None)
    parser.add_argument('-period', default=3)
    parser.add_argument('-gfs_dir', default='/wrf/gfs')

    return parser.parse_args()


def run_wrf(wrf_config):
    logging.info('Adding the WRF tasks')
    wrf_tasks = [Real(config=wrf_config), Wrf(config=wrf_config)]

    run_curw_tasks(wrf_tasks)


def run_wps(wrf_config):
    logging.info('Downloading GFS data')
    executor.download_gfs_data(wrf_config)

    logging.info('Adding the WPS tasks')
    wps_tasks = [Ungrib(config=wrf_config), Geogrid(config=wrf_config), Metgrid(config=wrf_config)]

    run_curw_tasks(wps_tasks)


def run_curw_tasks(tasks):
    for task in tasks:
        name = task.__class__.__name__
        logging.info('Starting ' + name)

        logging.info('Running %s: Pre-process')
        task.pre_process()

        logging.info('Running %s: Process')
        task.pre_process()

        logging.info('Running %s: Process')
        task.post_process()

        logging.info(name + ' completed!')


class CurwDockerException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)


if __name__ == "__main__":
    args = parse_args()

    config = executor.get_wrf_config(wrf_home=args.wrf_home, start_date=args.start, output_dir=args.output_dir,
                                     period=int(args.period), gfs_dir=args.gfs_dir, geog_dir=args.geog_dir)

    if args.nl_wps is not None:
        logging.info('Reading namelist wps')
        nl_wps = os.path.join(args.wrf_home, 'namelist.wps')
        content = args.nl_wps.replace('\\n', '\n')
        logging.info('namelist.wps content: \n%s' % content)
        with open(nl_wps, 'w') as f:
            f.write(content.encode())
        config.set('namelist_wps', nl_wps)

    if args.nl_input is not None:
        logging.info('Reading namelist input')
        nl_input = os.path.join(args.wrf_home, 'namelist.input')
        content = args.nl_wps.replace('\\n', '\n')
        logging.info('namelist.input content: \n%s' % content)
        with open(nl_input, 'w') as f:
            f.write(content)
        config.set('namelist_input', nl_input)

    logging.info('WRF config: %s' % config.to_string())

    if args.start is None:
        raise CurwDockerException('start_date is None')

    mode = args.mode.strip().lower()
    if mode == 'wps':
        logging.info('Running WPS')
        # run_wps(config)
    elif mode == 'wrf':
        logging.info('Running WRF')
        # run_wrf(config)
    else:
        raise CurwDockerException('Unknown mode ' + mode)
