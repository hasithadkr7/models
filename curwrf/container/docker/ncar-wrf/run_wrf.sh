#!/usr/bin/env bash

cd /wrf/curwsl
git pull

cd /wrf

python3.6 /wrf/curwsl/curwrf/container/docker/ncar-wrf/run_wrf.py -run_id="$1" -wrf_config="$2" -mode="$3" -nl_wps="$4" -nl_input="$5"