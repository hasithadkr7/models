#!/usr/bin/env bash

while getopts ":i:c:d:v:" option
do
 case "${option}"
 in
 i) ID=$OPTARG;;
 c) CONFIG=$OPTARG;;
 d) DB=$OPTARG;;
 v) bucket=$(echo $OPTARG | cut -d':' -f1)
    path=$(echo $OPTARG | cut -d':' -f2)
    echo "mounting $bucket to $path"
    gcsfuse ${bucket} ${path} ;;
 esac
done

cd /wrf/curwsl
git pull
cd /wrf

cd /wrf/curwsl_adapter
git pull
cd /wrf

python3.6 /wrf/curwsl/curw/container/docker/rainfall/ncar_wrf_extract/extract_data_wrf.py -run_id="$ID" -wrf_config="$CONFIG" -db_config="$DB"