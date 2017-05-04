#!/bin/python
import sys
import os
import execution.utils as utils
import datetime as dt
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import wrf.execution.constants as constants
import wrf.execution.utils

from wrf.execution.executor import WrfExecutor


def main():
    args = utils.parse_args()

    wrf_home = args.wrf_home
    start_date = dt.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = dt.datetime.strptime(args.end_date, '%Y-%m-%d')
    period = args.period

    utils.set_logging_config(utils.get_logs_dir(wrf_home))

    wrf_exec = WrfExecutor(wrf_home, start_date, end_date, period)
    wrf_exec.wrf_run_all()


if __name__ == "__main__":
    main()
