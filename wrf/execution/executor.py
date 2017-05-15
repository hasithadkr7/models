import logging
import os
import threading
import time
import datetime as dt
import pkg_resources
import subprocess
import wget
import sys

from wrf.execution import constants, utils, exceptions


def download_single_inventory(url, dest, retries=constants.DEFAULT_RETRIES, delay=constants.DEFAULT_DELAY_S):
    logging.info('Downloading %s : START' % url)
    try_count = 1
    start_time = time.time()

    while try_count <= retries:
        try:
            wget.download(url, out=dest)
            end_time = time.time()
            logging.info('Downloading %s : END Elapsed time: %f' % (url, end_time - start_time))
            return True
        except:
            logging.error('Error in downloading %s Attempt %d : %s' % (url, try_count, sys.exc_info()[0]))
            logging.info('Retrying in %d seconds' % delay)
            try_count += 1
            time.sleep(delay)

    raise exceptions.UnableToDownloadGfsData(url)


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
        except exceptions.UnableToDownloadGfsData as e:
            logging.error('Error in downloading from thread %d' % self.thread_id)


def download_gfs_data(date, gfs_dir,
                      thread_count=constants.DEFAULT_THREAD_COUNT,
                      retries=constants.DEFAULT_RETRIES,
                      delay=constants.DEFAULT_DELAY_S,
                      url=constants.DEFAULT_GFS_DATA_URL,
                      inv=constants.DEFAULT_GFS_DATA_INV,
                      period=constants.DEFAULT_PERIOD,
                      step=constants.DEFAULT_STEP,
                      cycle=constants.DEFAULT_CYCLE,
                      res=constants.DEFAULT_RES,
                      clean=True):
    logging.info('Downloading GFS data: START')

    if clean:
        logging.info('Cleaning the GFS dir: %s' % gfs_dir)
        utils.cleanup_dir(gfs_dir)

    inventories = utils.get_gfs_inventory_url_dest_list(url, inv, date, period, step, cycle, res, gfs_dir)
    logging.info(
        'Following data will be downloaded with %d parallel threads\n%s' % (
            thread_count, '\n'.join(' '.join(map(str, i)) for i in inventories)))

    start_time = time.time()

    threads = []
    inv_count = len(inventories)
    logging.debug('Initializing threads')
    for i in range(0, inv_count):
        url0 = inventories[i][0]
        dest0 = inventories[i][1]
        thread = InventoryDownloadThread(i, url0, dest0, retries, delay)
        thread.start()
        threads.append(thread)

    logging.debug('Joining threads')
    for t in threads:
        t.join()

    elapsed_time = time.time() - start_time
    logging.info('Downloading GFS data: END Elapsed time: %f' % elapsed_time)


def check_gfs_data_availability(wrf_home, start_date, inv, period, step, cycle, res):
    logging.info('Checking gfs data availability...')
    inventories = utils.get_gfs_inventory_dest_list(inv, start_date, period, step, cycle, res,
                                                    utils.get_gfs_dir(wrf_home))
    missing_inv = []
    for inv in inventories:
        if not os.path.exists(inv):
            missing_inv.append(inv)

    if len(missing_inv) > 0:
        logging.error('Some data unavailable')
        raise exceptions.GfsDataUnavailable('Some data unavailable', missing_inv)

    logging.info('GFS data available')


def run_wps(wrf_home, start_date):
    logging.info('Running WPS...')
    wps_dir = utils.get_wps_dir(wrf_home)

    logging.info('Cleaning up files')
    utils.delete_files_with_prefix(wps_dir, 'FILE:*')
    utils.delete_files_with_prefix(wps_dir, 'PFILE:*')
    utils.delete_files_with_prefix(wps_dir, 'met_em*')

    # Linking VTable
    utils.run_subprocess('ln -sf %s/ungrib/Variable_Tables/Vtable.NAM %s/Vtable' % (wps_dir, wps_dir))

    # Running link_grib.csh
    utils.run_subprocess(
        'csh %s/link_grib.csh %s/%s' % (wps_dir, utils.get_gfs_dir(wrf_home), start_date.strftime('%Y%m%d')))

    # Starting ungrib.exe
    utils.run_subprocess('./ungrib.exe', cwd=wps_dir)

    # Starting geogrid.exe'
    utils.run_subprocess('./geogrid.exe', cwd=wps_dir)

    # Starting metgrid.exe'
    utils.run_subprocess('./metgrid.exe', cwd=wps_dir)


def replace_namelist_wps(wrf_home, start_date, end_date):
    logging.info('Replacing namelist.wps...')
    wps = pkg_resources.resource_filename(__name__, 'conf/namelist.wps')
    d = {
        'YYYY1': start_date.strftime('%Y'),
        'MM1': start_date.strftime('%m'),
        'DD1': start_date.strftime('%d'),
        'YYYY2': end_date.strftime('%Y'),
        'MM2': end_date.strftime('%m'),
        'DD2': end_date.strftime('%d'),
        'GEOG': utils.get_geog_dir(wrf_home)
    }
    utils.replace_file_with_values(wps, os.path.join(wrf_home, utils.get_wps_dir(wrf_home), 'namelist.wps'), d)


def replace_namelist_input(wrf_home, start_date, end_date):
    logging.info('Replacing namelist.input ...')
    f = pkg_resources.resource_filename(__name__, 'conf/namelist.input')
    d = {
        'YYYY1': start_date.strftime('%Y'),
        'MM1': start_date.strftime('%m'),
        'DD1': start_date.strftime('%d'),
        'YYYY2': end_date.strftime('%Y'),
        'MM2': end_date.strftime('%m'),
        'DD2': end_date.strftime('%d'),
    }
    utils.replace_file_with_values(f, os.path.join(wrf_home, utils.get_em_real_dir(wrf_home), 'namelist.input'), d)


def run_em_real(wrf_home, start_date, procs):
    logging.info('Running em_real...')
    em_real_dir = utils.get_em_real_dir(wrf_home)

    logging.info('Cleaning up files')
    utils.delete_files_with_prefix(em_real_dir, 'met_em*')
    utils.delete_files_with_prefix(em_real_dir, 'rsl*')

    # Linking met_em.*
    utils.run_subprocess('ln -sf %s/met_em.d0* %s/' % (utils.get_wps_dir(wrf_home), em_real_dir))

    # Starting real.exe
    utils.run_subprocess('mpirun -np %d ./real.exe' % procs, cwd=em_real_dir)
    utils.move_files_with_prefix(em_real_dir, 'rsl*', os.path.join(utils.get_logs_dir(wrf_home),
                                                                   'rsl-real-%s' % start_date.strftime('%Y%m%d')))

    # Starting wrf.exe'
    utils.run_subprocess('mpirun -np %d ./wrf.exe' % procs, cwd=em_real_dir)
    utils.move_files_with_prefix(em_real_dir, 'rsl*', os.path.join(utils.get_logs_dir(wrf_home),
                                                                   'rsl-wrf-%s' % start_date.strftime('%Y%m%d')))


def run_wrf(wrf_home, start_date, procs=constants.DEFAULT_PROCS,
            inv=constants.DEFAULT_GFS_DATA_INV,
            period=constants.DEFAULT_PERIOD,
            step=constants.DEFAULT_STEP,
            cycle=constants.DEFAULT_CYCLE,
            res=constants.DEFAULT_RES):
    end_date = start_date + dt.timedelta(days=period)

    logging.info('Running WRF from %s to %s...' % (start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')))

    check_gfs_data_availability(wrf_home, start_date, inv, period, step, cycle, res)

    replace_namelist_wps(wrf_home, start_date, end_date)
    run_wps(wrf_home, start_date)

    replace_namelist_input(wrf_home, start_date, end_date)
    run_em_real(wrf_home, start_date, procs)


def wrf_run_all(wrf_home, start_date, end_date, period):
    logging.info('Downloading GFS Data')
    download_gfs_data(start_date, utils.get_gfs_dir(wrf_home), period=period)

    logging.info('Running WRF')
    run_wrf(wrf_home, start_date, period=period)


if __name__ == "__main__":
    pass
