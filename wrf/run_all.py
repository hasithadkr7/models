#!/bin/python
import sys
import os
import datetime as dt

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import wrf.execution.utils as utils

import wrf.execution.executor as wrf_exec


def main():
    args = utils.parse_args()

    wrf_home = args.wrf_home
    start_date = dt.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = dt.datetime.strptime(args.end_date, '%Y-%m-%d')
    period = args.period

    utils.set_logging_config(utils.get_logs_dir(wrf_home))

    wrf_exec.wrf_run_all(wrf_home, start_date, end_date, period)


if __name__ == "__main__":
    main()
