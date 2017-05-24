#!/usr/bin/env python

import datetime as dt
import logging
import os
import re
import sys
import pandas as pd
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class DataEventHandler(FileSystemEventHandler):
    def __init__(self, out_file):
        self.out_file = out_file

    def on_created(self, event):
        logging.info("File created %s" % event)
        if event.src_path.endswith('.dat') and event.src_path.split('/')[-1].startswith('CR200_'):
            logging.info('CR200_*.dat file created')
            process_sat_file(event.src_path, self.out_file)

    def on_moved(self, event):
        logging.info("File moved/renamed %s" % event)
        if event.dest_path.endswith('.dat') and event.dest_path.split('/')[-1].startswith('CR200_'):
            logging.info('file renamed to CR200_*.dat')
            process_sat_file(event.dest_path, self.out_file)


def process_sat_file(src_file, dest_file):
    names = ["TIMESTAMP", "Rain_Tot"]
    station = re.search('KALU\d*', src_file).group(0)
    data = pd.read_csv(src_file, skiprows=range(4), names=names, sep=',', usecols=(0, 3), dtype=None,
                       converters={0: lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')})
    means = data.groupby(pd.TimeGrouper(freq='H', key='TIMESTAMP')).agg(['sum', 'count']).rename(
        columns={"sum": "rainfall", "count": "samples"})
    means['STATION'] = station

    with open(dest_file, 'a') as f:
        means.to_csv(f, header=False)


def process_old_files(src, dest_file):
    logging.info('Processing the old files in the dir %s' % src)
    file_list = os.listdir(src)

    if os.path.exists(dest_file):
        logging.info('Removing the %s file' % dest_file)
        os.remove(dest_file)

    for f in sorted(file_list):
        if f.endswith('.dat') and f.startswith('CR200_'):
            logging.info('Reading %s' % f)
            process_sat_file(os.path.join(src, f), dest_file)


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    path = argv[1]
    logging.info('data dir %s' % path)
    process_old = bool(argv[2]) if len(argv) > 2 else False
    logging.info('process old %s' % str(process_old))
    dest_file = argv[2] if len(argv) > 3 else 'summary.txt'
    logging.info('dest file %s' % dest_file)

    if process_old:
        logging.info('Processing old files')
        process_old_files(path, dest_file)

    observer = Observer()
    event_handler = DataEventHandler(os.path.join(path, dest_file))
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    observer.join()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
