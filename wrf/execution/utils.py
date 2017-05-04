import argparse
import datetime as dt
import logging
import logging.config
import constants as constants
import os


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
            'f': {'format': '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
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
    return create_dir_if_not_exists(os.path.join(wrf_home, 'data', 'GFS'))


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


def get_gfs_inventory_dest_list(url, inv, date, period, step, cycle, res, gfs_dir):
    date_str = date.strftime('%Y%m%d')
    return [get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, str(i).zfill(3), res, gfs_dir) for i in
            range(0, period * 24 + 1, step)]


def main():
    wrf_home = "/tmp"
    print get_gfs_dir(wrf_home)
    print get_output_dir(wrf_home)
    print get_scripts_run_dir(wrf_home)
    print get_logs_dir(wrf_home)
    print get_gfs_data_url_dest_tuple(constants.DEFAULT_GFS_DATA_URL, constants.DEFAULT_GFS_DATA_INV,
                                      get_gfs_dir(wrf_home), '20170501', constants.DEFAULT_CYCLE, '001',
                                      constants.DEFAULT_RES)
    print get_gfs_inventory_dest_list(constants.DEFAULT_GFS_DATA_URL, constants.DEFAULT_GFS_DATA_INV,
                                                dt.datetime.strptime('2017-05-01', '%Y-%m-%d'),
                                                constants.DEFAULT_PERIOD,
                                                constants.DEFAULT_STEP,
                                                constants.DEFAULT_CYCLE,
                                                constants.DEFAULT_RES,
                                                get_gfs_dir(wrf_home))


if __name__ == "__main__":
    main()
