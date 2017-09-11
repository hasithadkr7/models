#!/usr/bin/env bash

cd /wrf/curwsl
git pull

cd /wrf

python3.6 /wrf/curwsl/curwrf/container/docker/ncar-wrf/run_wrf.py