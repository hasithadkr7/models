#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS=/wrf/gcs.json

while getopts ":i:c:m:x:y:k:v:" option
do
 case "${option}"
 in
 i) ID=$OPTARG;;
 c) CONFIG=$OPTARG;;
 m) MODE=$OPTARG;;
 x) WPS=$OPTARG;;
 y) INPUT=$OPTARG;;
 k) echo $OPTARG > /wrf/gcs.json;;
 v) bucket=$(echo $OPTARG | cut -d':' -f1)
    path=$(echo $OPTARG | cut -d':' -f2)
    echo "mounting $bucket to $path"
    gcsfuse ${bucket} ${path} ;;
 esac
done

cd /wrf/curwsl
git pull
cd /wrf

python3.6 /wrf/curwsl/curw/container/docker/rainfall/ncar_wrf/run_wrf.py -run_id="$ID" -wrf_config="$CONFIG" -mode="$MODE" -nl_wps="$WPS" -nl_input="$INPUT"
