#!/usr/bin/env bash

while getopts ":i:c:m:x:y:" option
do
 case "${option}"
 in
 i) ID="-run_id=$OPTARG";;
 c) CONFIG="-wrf_config=$OPTARG";;
 m) MODE="-mode=$OPTARG";;
 x) WPS="-nl_wps=$OPTARG";;
 y) INPUT="-nl_input=$OPTARG";;
 esac
done

cd /wrf/curwsl
git pull
cd /wrf

options="$ID $CONFIG $MODE $WPS $INPUT"

python3.6 /wrf/curwsl/curwrf/container/docker/ncar-wrf/run_wrf.py ${options}