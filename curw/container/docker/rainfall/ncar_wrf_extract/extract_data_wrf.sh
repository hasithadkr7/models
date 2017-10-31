#!/usr/bin/env bash

while getopts ":i:c:d" option
do
 case "${option}"
 in
 i) ID=$OPTARG;;
 c) CONFIG=$OPTARG;;
 d) DB=$OPTARG;;
 esac
done

cd /wrf/curwsl
git pull
cd /wrf

cd /wrf/curwsl_adapter
git pull
cd /wrf

python3.6 /wrf/curwsl/curw/container/docker/rainfall/ncar_wrf_extract/extract_data_wrf.py -run_id="$ID" -wrf_config="$CONFIG" -db_config="$DB"