#!/usr/bin/python

import argparse
import logging
import ntpath
import os

import shutil
from curwrf.wrf import utils
from curwrf.wrf.execution import executor


def parse_args():
    parser = argparse.ArgumentParser(description='Downloading single inventory')
    parser.add_argument('url', dest='url')
    parser.add_argument('dest', dest='url')

    return parser.parse_args()


def download_single_inventory_task(url, dest):
    logging.info('Downloading from %s to %s' % (url, dest))
    executor.download_single_inventory(url, dest, retries=1, delay=0)


def download_i_th_inventory(i, wrf_config, **kwargs):
    logging.info('Downloading %d inventory' % i)
    if kwargs is not None:
        execution_date = kwargs['execution_date']
        logging.info('Execution date %s' % str(execution_date))
    else:
        raise DownloadSingleInventoryTaskException('kwargs is not provided')

    url, dest = utils.get_gfs_data_url_dest_tuple(wrf_config.get('gfs_url'),
                                                  wrf_config.get('gfs_inv'),
                                                  execution_date.strftime('%Y%m%d'),
                                                  wrf_config.get('gfs_cycle'),
                                                  str(i).zfill(3),
                                                  wrf_config.get('gfs_res'),
                                                  wrf_config.get('gfs_dir'))
    dest_nfs = os.path.join(utils.get_nfs_gfs_dir(wrf_config.get('nfs_dir')), ntpath.basename(dest))
    logging.info('URL %s, dest %s, nfs_des %s' % (url, dest, dest_nfs))

    if os.path.exists(dest_nfs) and os.path.isfile(dest_nfs):
        logging.info("File available in NFS. Copying to the GFS dir from NFS")
        shutil.copyfile(dest_nfs, dest)
    else:
        logging.info("File not available in NFS. Downloading...")
        executor.download_single_inventory(url, dest, retries=1, delay=0)
        shutil.copyfile(dest, dest_nfs)


class DownloadSingleInventoryTaskException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)


if __name__ == "__main__":
    args = parse_args()
    if args.url is None or args.dest is None:
        raise DownloadSingleInventoryTaskException(
            'Unable to download data: url=%s dest= %s' % (str(args.url), str(args.dest)))

        # download_single_inventory_task(args.url, args.dest)
