#!/bin/python
import datetime as dt
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import wrf.utils as utils
import wrf.execution.executor as wrf_exec
import wrf.extraction.extractor as wrf_extract


def main():
    args = utils.parse_args()

    wrf_home = args.wrf_home
    start_date = dt.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = dt.datetime.strptime(args.end_date, '%Y-%m-%d')

    utils.set_logging_config(utils.get_logs_dir(wrf_home))

    wrf_conf = utils.get_default_wrf_config(wrf_home)

    wrf_exec.run_all(wrf_conf, start_date, end_date)

    wrf_extract.extract_all(wrf_home, start_date, end_date)

if __name__ == "__main__":
    main()
