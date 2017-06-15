#!/usr/bin/python

import argparse
import logging

from curwrf.wrf.execution import executor


def parse_args():
    parser = argparse.ArgumentParser(description='Downloading single inventory')
    parser.add_argument('url', dest='url')
    parser.add_argument('dest', dest='url')

    return parser.parse_args()


def download_single_inventory_task(url, dest):
    logging.info('Downloading from %s to %s' %(url, dest))
    executor.download_single_inventory(url, dest, retries=1, delay=0)


class DownloadSingleInventoryTaskException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)


if __name__ == "__main__":
    args = parse_args()
    if args.url is None or args.dest is None:
        raise DownloadSingleInventoryTaskException(
            'Unable to download data: url=%s dest= %s' % (str(args.url), str(args.dest)))

    download_single_inventory_task(args.url, args.dest)
