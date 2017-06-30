import datetime as dt
import logging
import os
import threading
import time
import numpy as np
import yaml

from urllib.error import HTTPError, URLError

from curwrf.wrf.resources import manager as res_mgr
from curwrf.wrf import constants, utils


def download_single_inventory(url, dest, retries=constants.DEFAULT_RETRIES, delay=constants.DEFAULT_DELAY_S):
    logging.info('Downloading %s : START' % url)
    try_count = 1
    start_time = time.time()
    while try_count <= retries:
        try:
            utils.download_file(url, dest)
            end_time = time.time()
            logging.info('Downloading %s : END Elapsed time: %f' % (url, end_time - start_time))
            return True
        except (HTTPError, URLError) as e:
            logging.error(
                'Error in downloading %s Attempt %d : %s . Retrying in %d seconds' % (url, try_count, e.message, delay))
            try_count += 1
            time.sleep(delay)

    raise UnableToDownloadGfsData(url)


class InventoryDownloadThread(threading.Thread):
    def __init__(self, thread_id, url, dest, retries, delay):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.url = url
        self.dest = dest
        self.retries = retries
        self.delay = delay

    def run(self):
        try:
            logging.debug('Downloading from thread %d: START' % self.thread_id)
            download_single_inventory(self.url, self.dest, self.retries, self.delay)
            logging.debug('Downloading from thread %d: END' % self.thread_id)
        except UnableToDownloadGfsData:
            logging.error('Error in downloading from thread %d' % self.thread_id)


def download_gfs_data(date, wrf_conf):
    logging.info('Downloading GFS data: START')

    if wrf_conf.get('gfs_clean'):
        logging.info('Cleaning the GFS dir: %s' % wrf_conf.get('gfs_dir'))
        utils.cleanup_dir(wrf_conf.get('gfs_dir'))

    inventories = utils.get_gfs_inventory_url_dest_list(date, wrf_conf.get('period'), wrf_conf.get('gfs_url'),
                                                        wrf_conf.get('gfs_inv'), wrf_conf.get('gfs_step'),
                                                        wrf_conf.get('gfs_cycle'), wrf_conf.get('gfs_res'),
                                                        wrf_conf.get('gfs_dir'))
    gfs_threads = wrf_conf.get('gfs_threads')
    logging.info(
        'Following data will be downloaded in %d parallel threads\n%s' % (gfs_threads, '\n'.join(
            ' '.join(map(str, i)) for i in inventories)))

    start_time = time.time()
    inv_count = len(inventories)
    logging.debug('Initializing threads')
    for k in range(0, inv_count, gfs_threads):
        threads = []
        for j in range(0, gfs_threads):
            i = k + j
            if i < inv_count:
                url0 = inventories[i][0]
                dest0 = inventories[i][1]
                thread = InventoryDownloadThread(i, url0, dest0, wrf_conf.get('gfs_retries'), wrf_conf.get('gfs_delay'))
                thread.start()
                threads.append(thread)

        logging.debug('Joining threads')
        for t in threads:
            t.join()

    elapsed_time = time.time() - start_time
    logging.info('Downloading GFS data: END Elapsed time: %f' % elapsed_time)


def check_gfs_data_availability(date, wrf_config):
    logging.info('Checking gfs data availability...')
    inventories = utils.get_gfs_inventory_dest_list(date, wrf_config.get('period'), wrf_config.get('gfs_inv'),
                                                    wrf_config.get('gfs_step'), wrf_config.get('gfs_cycle'),
                                                    wrf_config.get('gfs_res'), wrf_config.get('gfs_dir'))
    missing_inv = []
    for inv in inventories:
        if not os.path.exists(inv):
            missing_inv.append(inv)

    if len(missing_inv) > 0:
        logging.error('Some data unavailable')
        raise GfsDataUnavailable('Some data unavailable', missing_inv)

    logging.info('GFS data available')


def check_geogrid_output(wps_dir):
    for i in range(1, 4):
        if not os.path.exists(os.path.join(wps_dir, 'geo_em.d%02d.nc' % i)):
            return False
    return True


def run_wps(wrf_home, start_date):
    logging.info('Running WPS...')
    wps_dir = utils.get_wps_dir(wrf_home)

    logging.info('Cleaning up files')
    utils.delete_files_with_prefix(wps_dir, 'FILE:*')
    utils.delete_files_with_prefix(wps_dir, 'PFILE:*')
    utils.delete_files_with_prefix(wps_dir, 'met_em*')

    # Linking VTable
    if not os.path.exists(os.path.join(wps_dir, 'Vtable')):
        logging.info('Creating Vtable symlink')
        os.symlink(os.path.join(wps_dir, 'ungrib/Variable_Tables/Vtable.NAM'), os.path.join(wps_dir, 'Vtable'))

    # Running link_grib.csh
    utils.run_subprocess(
        'csh link_grib.csh %s/%s' % (utils.get_gfs_dir(wrf_home), start_date.strftime('%Y%m%d')), cwd=wps_dir)

    # Starting ungrib.exe
    utils.run_subprocess('./ungrib.exe', cwd=wps_dir)

    # Starting geogrid.exe'
    if not check_geogrid_output(wps_dir):
        logging.info('Geogrid output not available')
        utils.run_subprocess('./geogrid.exe', cwd=wps_dir)

    # Starting metgrid.exe'
    utils.run_subprocess('./metgrid.exe', cwd=wps_dir)


def replace_namelist_wps(wrf_config, start_date, end_date):
    logging.info('Replacing namelist.wps...')
    if os.path.exists(wrf_config.get('namelist_wps')):
        wps = wrf_config.get('namelist_wps')
    else:
        wps = res_mgr.get_resource_path(os.path.join('execution', constants.DEFAULT_NAMELIST_WPS_TEMPLATE))
    d = {
        'YYYY1': start_date.strftime('%Y'),
        'MM1': start_date.strftime('%m'),
        'DD1': start_date.strftime('%d'),
        'YYYY2': end_date.strftime('%Y'),
        'MM2': end_date.strftime('%m'),
        'DD2': end_date.strftime('%d'),
        'GEOG': utils.get_geog_dir(wrf_config.get('wrf_home'))
    }
    utils.replace_file_with_values(wps, os.path.join(utils.get_wps_dir(wrf_config.get('wrf_home')), 'namelist.wps'), d)


def replace_namelist_input(wrf_config, start_date, end_date):
    logging.info('Replacing namelist.input ...')
    if os.path.exists(wrf_config.get('namelist_input')):
        f = wrf_config.get('namelist_input')
    else:
        f = res_mgr.get_resource_path(os.path.join('execution', constants.DEFAULT_NAMELIST_INPUT_TEMPLATE))
    d = {
        'YYYY1': start_date.strftime('%Y'),
        'MM1': start_date.strftime('%m'),
        'DD1': start_date.strftime('%d'),
        'YYYY2': end_date.strftime('%Y'),
        'MM2': end_date.strftime('%m'),
        'DD2': end_date.strftime('%d'),
    }
    utils.replace_file_with_values(f, os.path.join(utils.get_em_real_dir(wrf_config.get('wrf_home')), 'namelist.input'),
                                   d)


def run_em_real(wrf_home, start_date, procs):
    logging.info('Running em_real...')
    em_real_dir = utils.get_em_real_dir(wrf_home)

    logging.info('Cleaning up files')
    utils.delete_files_with_prefix(em_real_dir, 'met_em*')
    utils.delete_files_with_prefix(em_real_dir, 'rsl*')

    # Linking met_em.*
    logging.info('Creating met_em.d* symlinks')
    utils.create_symlink_with_prefix(utils.get_wps_dir(wrf_home), 'met_em.d*', em_real_dir)

    # Starting real.exe
    utils.run_subprocess('mpirun -np %d ./real.exe' % procs, cwd=em_real_dir)
    utils.move_files_with_prefix(em_real_dir, 'rsl*', os.path.join(utils.get_logs_dir(wrf_home),
                                                                   'rsl-real-%s' % start_date.strftime('%Y%m%d')))

    # Starting wrf.exe'
    utils.run_subprocess('mpirun -np %d ./wrf.exe' % procs, cwd=em_real_dir)
    utils.move_files_with_prefix(em_real_dir, 'rsl*', os.path.join(utils.get_logs_dir(wrf_home),
                                                                   'rsl-wrf-%s' % start_date.strftime('%Y%m%d')))


def run_wrf(date, wrf_config):
    end = date + dt.timedelta(days=wrf_config.get('period'))

    logging.info('Running WRF from %s to %s...' % (date.strftime('%Y%m%d'), end.strftime('%Y%m%d')))

    wrf_home = wrf_config.get('wrf_home')
    check_gfs_data_availability(date, wrf_config)

    replace_namelist_wps(wrf_config, date, end)
    run_wps(wrf_home, date)

    replace_namelist_input(wrf_config, date, end)
    run_em_real(wrf_home, date, wrf_config.get('procs'))

    logging.info('Moving the WRF files to output directory')
    utils.move_files_with_prefix(utils.get_em_real_dir(wrf_home), 'wrfout_d*', utils.get_output_dir(wrf_home))


def run_all(wrf_conf, start_date, end_date):
    logging.info('Running WRF model from %s to %s' % (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))

    logging.info('WRF conf\n %s' % wrf_conf.to_string())

    dates = np.arange(start_date, end_date, dt.timedelta(days=1)).astype(dt.datetime)

    for date in dates:
        logging.info('Creating GFS context')
        logging.info('Downloading GFS Data for %s period %d' % (date.strftime('%Y-%m-%d'), wrf_conf.get('period')))
        download_gfs_data(date, wrf_conf)

        logging.info('Running WRF %s period %d' % (date.strftime('%Y-%m-%d'), wrf_conf.get('period')))
        run_wrf(date, wrf_conf)


class UnableToDownloadGfsData(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, 'Unable to download %s' % msg)


class GfsDataUnavailable(Exception):
    def __init__(self, msg, missing_data):
        self.msg = msg
        self.missing_data = missing_data
        Exception.__init__(self, 'Unable to download %s' % msg)


class WrfConfig:
    def __init__(self, configs=None):
        if configs is None:
            configs = {}
        self.configs = configs

    def set_all(self, config_dict):
        self.configs.update(config_dict)

    def set(self, key, value):
        self.configs[key] = value

    def get(self, key):
        return self.configs[key]

    def get_with_defaults(self, key, default_val):
        try:
            return self.configs[key]
        except KeyError:
            return default_val

    def get_all(self):
        return self.configs.copy()

    def to_string(self):
        return str(self.configs)


def get_wrf_config(wrf_home, config_file=None, **kwargs):
    """
    precedence = kwargs > wrf_config.yaml > constants
    """
    defaults = {'wrf_home': constants.DEFAULT_WRF_HOME,
                'nfs_dir': constants.DEFAULT_NFS_DIR,
                'period': constants.DEFAULT_PERIOD,
                'namelist_input': constants.DEFAULT_NAMELIST_INPUT_TEMPLATE,
                'namelist_wps': constants.DEFAULT_NAMELIST_WPS_TEMPLATE,
                'procs': constants.DEFAULT_PROCS,
                'gfs_dir': utils.get_gfs_dir(constants.DEFAULT_WRF_HOME, False),
                'gfs_clean': True,
                'gfs_cycle': constants.DEFAULT_CYCLE,
                'gfs_delay': constants.DEFAULT_DELAY_S,
                'gfs_inv': constants.DEFAULT_GFS_DATA_INV,
                'gfs_res': constants.DEFAULT_RES,
                'gfs_retries': constants.DEFAULT_RETRIES,
                'gfs_step': constants.DEFAULT_STEP,
                'gfs_url': constants.DEFAULT_GFS_DATA_URL,
                'gfs_threads': constants.DEFAULT_THREAD_COUNT}

    conf = WrfConfig(defaults)

    if config_file is not None and os.path.exists(config_file):
        with open(config_file, 'r') as f:
            conf_yaml = yaml.safe_load(f)
            conf.set_all(conf_yaml['wrf_config'])

    for key in kwargs:
        conf.set(key, kwargs[key])

    conf.set('wrf_home', wrf_home)
    conf.set('gfs_dir', utils.get_gfs_dir(wrf_home))

    return conf


if __name__ == "__main__":
    pass
