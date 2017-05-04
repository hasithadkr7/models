import logging
import threading
import time
import datetime as dt
import wget

from wrf.execution import constants, utils

logger = logging.getLogger('executor')


def download_single_inventory(url, dest, retries=5, delay=10):
    logger.info('Downloading %s : START' % url)
    try_count = 1
    start_time = time.time()

    while try_count <= retries:
        try:
            wget.download(url, out=dest)
            end_time = time.time()
            logger.info('Downloading %s : END Elapsed time: %f' % (url, end_time - start_time))
            return True
        except IOError as err:
            logger.error('Error in downloading %s Attempt %d : %s' % (url, try_count, str(err)))
            logger.info('Retrying in %d seconds' % delay)
            try_count += 1
            time.sleep(delay)

    logger.error('Unable to download %s' % url)
    return False


class InventoryDownloadThread(threading.Thread):
    def __init__(self, thread_id, url, dest, retries, delay):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.url = url
        self.dest = dest
        self.retries = retries
        self.delay = delay
        self.logger = logging.getLogger('InventoryDownloadThread' + str(thread_id))

    def run(self):
        self.logger.info('Downloading %s: START' % self.url)
        if download_single_inventory(self.url, self.dest, self.retries, self.delay):
            self.logger.info('Downloading %s: END' % self.url)
        else:
            self.logger.error('Error in downloading %s' % self.url)


class WrfExecutor:
    def __init__(self, wrf_home, start_date, end_date, period, meta_data=list()):
        self.wrf_home = wrf_home
        self.start_date = start_date
        self.end_date = end_date
        self.meta_data = meta_data
        self.period = period
        self.logger = logging.getLogger('WrfExecutor')

    def download_gfs_data(self, date, gfs_dir,
                          thread_count=constants.DEFAULT_THREAD_COUNT,
                          retries=constants.DEFAULT_RETRIES,
                          delay=constants.DEFAULT_DELAY_S,
                          url=constants.DEFAULT_GFS_DATA_URL,
                          inv=constants.DEFAULT_GFS_DATA_INV,
                          period=constants.DEFAULT_PERIOD,
                          step=constants.DEFAULT_STEP,
                          cycle=constants.DEFAULT_CYCLE,
                          res=constants.DEFAULT_RES):
        self.logger.info('Downloading GFS data: START')

        inventories = utils.get_gfs_inventory_dest_list(url, inv, date, period, step, cycle, res, gfs_dir)
        self.logger.info(
            'Following data will be downloaded with %d parallel threads\n%s' % (
                thread_count, '\n'.join(' '.join(map(str, i)) for i in inventories)))

        start_time = time.time()

        threads = []
        inv_count = len(inventories)
        self.logger.debug('Initializing threads')
        for i in range(0, inv_count, thread_count):
            for idx in range(thread_count):
                if i + idx < inv_count:
                    url0 = inventories[i + idx][0]
                    dest0 = inventories[i + idx][1]
                    thread = InventoryDownloadThread(i + idx, url0, dest0, retries, delay)
                    threads.append(thread)

        self.logger.debug('Starting threads')
        for t in threads:
            t.start()

        self.logger.debug('Joining threads')
        for t in threads:
            t.join()

        elapsed_time = time.time() - start_time
        self.logger.info('Downloading GFS data: END Elapsed time: %f' % elapsed_time)

    def run_wrf(self):
        self.logger.info('Running WRF')
        # some logic to run wrf
        pass

    def wrf_run_all(self):
        self.logger.info('Running all WRF')
        self.download_gfs_data(self.start_date, utils.get_gfs_dir(self.wrf_home))

        # some logic to download data and run wrf
        pass

        # dates = np.arange(start_date, end_date, dt.timedelta(days=1)).astype(dt.datetime)

        # for date in dates:
        #     date1 = date + dt.timedelta(days=period)
        #     print 'WRF scheduled from %s to %s' % (date.strftime('%Y-%m-%d'), date1.strftime('%Y-%m-%d'))
        #
        #     print 'Downloading data from %s. Running bash file %s' % (date.strftime('%Y%m%d'), get_data_cmd)
        #     process1, elapsed_time = download_gfs_data(get_data_cmd, date)
        #     print 'Data download completed for %s. Elapsed time %f' % (date.strftime('%Y%m%d'), elapsed_time)
        #
        #     print 'Running wrf from %s to %s' % (date.strftime('%Y%m%d'), date1.strftime('%Y%m%d'))
        #     process2, elapsed_time = run_wrf(run_wrf_cmd, date, date1)
        #     print 'WRF run completed from %s to %s. Elapsed time %f' % (
        #         date.strftime('%Y%m%d'), date1.strftime('%Y%m%d'), elapsed_time)






        # last_file_name = gfs_home + '/' + start_date.strftime('%Y%m%d') + '.gfs.t00z.pgrb2.0p50.f075'
        #
        # i = 5
        # while i > 0 and (not os.path.exists(last_file_name) or os.stat(last_file_name).st_size == 0):
        #     print "File %s does not exist. Downloading data" % last_file_name
        #     result = subprocess.call(run_home + '/get-daily-data.bash')
        #     i = i - 1
        #     if os.stat(last_file_name).st_size != 0:
        #         print "data downloaded!"
        #         break
        #     time.sleep(5 * 60)
        #
        # wrf_output_file = wrf_output + '/wrfout_d03_' + start_date.strftime('%Y-%m-%d') + '_00:00:00'
        #
        # i = 5
        # while i > 0 and (not os.path.exists(wrf_output_file) or os.stat(wrf_output_file).st_size == 0):
        #     print "wrf output %s does not exist. Running WRF" % last_file_name
        #     result = subprocess.call(run_home + '/run-wrf.bash')
        #     i = i - 1
        #     if os.stat(wrf_output_file).st_size != 0:
        #         print "WRF run complete!"
        #         break
        #     time.sleep(5 * 60)
