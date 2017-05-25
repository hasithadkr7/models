#!/bin/python
import datetime as dt

from curw.wrf.execution import executor
from curw.wrf.extraction import extractor
from curw.wrf import utils


def main():
    args = utils.parse_args()

    wrf_home = args.wrf_home
    start_date = dt.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = dt.datetime.strptime(args.end_date, '%Y-%m-%d')

    utils.set_logging_config(utils.get_logs_dir(wrf_home))

    wrf_conf = executor.get_default_wrf_config(wrf_home)

    executor.run_all(wrf_conf, start_date, end_date)

    extractor.extract_all(wrf_home, start_date, end_date)

if __name__ == "__main__":
    main()
