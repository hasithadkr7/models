import argparse
import datetime as dt
import glob
import logging
import logging.config
import re
import shlex
import shutil
import time
import ntpath


import os
import subprocess

import constants as constants


def parse_args():
    parser = argparse.ArgumentParser(description='Run all stages of WRF')
    parser.add_argument('-wrf', default=constants.DEFAULT_WRF_HOME, help='WRF home', dest='wrf_home')
    parser.add_argument('-start', default=dt.datetime.today().strftime('%Y-%m-%d'),
                        help='Start date with format %%Y-%%m-%%d', dest='start_date')
    parser.add_argument('-end', default=(dt.datetime.today() + dt.timedelta(days=1)).strftime('%Y-%m-%d'),
                        help='End date with format %%Y-%%m-%%d', dest='end_date')
    parser.add_argument('-period', default=3, help='Period of days for each run', type=int)
    return parser.parse_args()


def set_logging_config(log_home):
    logging_config = dict(
        version=1,
        formatters={
            'f': {'format': '%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s'}
        },
        handlers={
            'h': {'class': 'logging.StreamHandler',
                  'formatter': 'f',
                  'level': logging.INFO},
            'fh': {'class': 'logging.handlers.TimedRotatingFileHandler',
                   'formatter': 'f',
                   'level': logging.INFO,
                   'filename': os.path.join(log_home, 'wrfrun.log'),
                   'when': 'D',
                   'interval': 1
                   }
        },
        root={
            'handlers': ['h', 'fh'],
            'level': logging.DEBUG,
        },
    )

    logging.config.dictConfig(logging_config)


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_gfs_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'DATA', 'GFS'))


def get_wps_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, constants.DEFAULT_WPS_PATH)


def get_em_real_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, constants.DEFAULT_EM_REAL_PATH)


def get_geog_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, 'DATA', 'geog')


def get_output_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'OUTPUT'))


def get_scripts_run_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'wrf-scripts', 'run'))


def get_logs_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'logs'))


def get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, fcst_id, res, gfs_dir):
    url0 = url.replace('YYYYMMDD', date_str).replace('CC', cycle)
    inv0 = inv.replace('CC', cycle).replace('FFF', fcst_id).replace('RRRR', res)
    dest = os.path.join(gfs_dir, date_str + '.' + inv0)
    return url0 + inv0, dest


def get_gfs_data_dest(inv, date_str, cycle, fcst_id, res, gfs_dir):
    inv0 = inv.replace('CC', cycle).replace('FFF', fcst_id).replace('RRRR', res)
    dest = os.path.join(gfs_dir, date_str + '.' + inv0)
    return dest


def get_gfs_inventory_url_dest_list(url, inv, date, period, step, cycle, res, gfs_dir):
    date_str = date.strftime('%Y%m%d')
    return [get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, str(i).zfill(3), res, gfs_dir) for i in
            range(0, period * 24 + 1, step)]


def get_gfs_inventory_dest_list(inv, date, period, step, cycle, res, gfs_dir):
    date_str = date.strftime('%Y%m%d')
    return [get_gfs_data_dest(inv, date_str, cycle, str(i).zfill(3), res, gfs_dir) for i in
            range(0, period * 24 + 1, step)]


def replace_file_with_values(source, destination, val_dict):
    logging.debug('replace file source ' + source)
    logging.debug('replace file destination ' + destination)
    logging.debug('replace file content dict ' + str(val_dict))

    # pattern = re.compile(r'\b(' + '|'.join(val_dict.keys()) + r')\b')
    pattern = re.compile('|'.join(val_dict.keys()))

    dest = open(destination, 'w')
    out = ''
    with open(source, 'r') as src:
        line = pattern.sub(lambda x: val_dict[x.group()], src.read())
        dest.write(line)
        out += line

    dest.close()
    logging.debug('replace file final content \n' + out)


def cleanup_dir(gfs_dir):
    shutil.rmtree(gfs_dir)
    os.makedirs(gfs_dir)


def delete_files_with_prefix(directory, prefix):
    for filename in glob.glob(os.path.join(directory, prefix)):
        os.remove(filename)


def move_files_with_prefix(directory, prefix, dest):
    create_dir_if_not_exists(dest)
    for filename in glob.glob(os.path.join(directory, prefix)):
        os.rename(filename, os.path.join(dest, ntpath.basename(filename)))


def run_subprocess(cmd, cwd=None):
    logging.info('Running subprocess %s' % cmd)
    start_t = time.time()
    output = ''
    try:
        output = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, cwd=cwd)
    except subprocess.CalledProcessError as e:
        logging.error('Exception in subprocess %s! Error code %d' % (cmd, e.returncode))
        logging.error(e.output)
        raise e
    finally:
        elapsed_t = time.time() - start_t
        logging.info('Subprocess %s finished in %f s' % (cmd, elapsed_t))
        logging.info('stdout and stderr of %s\n%s' % (cmd, output))
    return output


def main():
    wrf_home = "/tmp"
    print get_gfs_dir(wrf_home)
    print get_output_dir(wrf_home)
    print get_scripts_run_dir(wrf_home)
    print get_logs_dir(wrf_home)
    print get_gfs_data_url_dest_tuple(constants.DEFAULT_GFS_DATA_URL, constants.DEFAULT_GFS_DATA_INV,
                                      get_gfs_dir(wrf_home), '20170501', constants.DEFAULT_CYCLE, '001',
                                      constants.DEFAULT_RES)
    print get_gfs_inventory_url_dest_list(constants.DEFAULT_GFS_DATA_URL, constants.DEFAULT_GFS_DATA_INV,
                                          dt.datetime.strptime('2017-05-01', '%Y-%m-%d'),
                                          constants.DEFAULT_PERIOD,
                                          constants.DEFAULT_STEP,
                                          constants.DEFAULT_CYCLE,
                                          constants.DEFAULT_RES,
                                          get_gfs_dir(wrf_home))
    d = {
        'YYYY1': '2016',
        'MM1': '05',
        'DD1': '01'
    }

    print replace_file_with_values('conf/namelist.input', wrf_home + '/namelist.wps', d)


if __name__ == "__main__":
    main()
