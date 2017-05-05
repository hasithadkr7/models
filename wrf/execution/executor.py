import logging
import threading
import time
import wget
import sys

from wrf.execution import constants, utils


def download_single_inventory(url, dest, retries=5, delay=10):
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

    logging.error('Unable to download %s' % url)
    return False


class InventoryDownloadThread(threading.Thread):
    def __init__(self, thread_id, url, dest, retries, delay):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.url = url
        self.dest = dest
        self.retries = retries
        self.delay = delay

    def run(self):

        logging.debug('Downloading from thread %d: START' % self.thread_id)
        if download_single_inventory(self.url, self.dest, self.retries, self.delay):
            logging.debug('Downloading from thread %d: END' % self.thread_id)
        else:
            logging.debug('Error in downloading from thread %d' % self.thread_id)


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

    inventories = utils.get_gfs_inventory_dest_list(url, inv, date, period, step, cycle, res, gfs_dir)
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


def run_wrf():
    logging.info('Running WRF')
    # some logic to run wrf
    pass


def wrf_run_all(wrf_home, start_date, end_date, period, meta_data=list()):
    logging.info('Running all WRF')
    download_gfs_data(start_date, utils.get_gfs_dir(wrf_home), period=period)

    # some logic to download data and run wrf


if __name__ == "__main__":
    pass
