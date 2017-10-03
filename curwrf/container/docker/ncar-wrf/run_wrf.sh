#!/usr/bin/env bash

while getopts ":i:c:m:x:y:" option
do
 case "${option}"
 in
 i) ID=$OPTARG;;
 c) CONFIG=$OPTARG;;
 m) MODE=$OPTARG;;
 x) WPS=$OPTARG;;
 y) INPUT=$OPTARG;;
 esac
done

cd /wrf/curwsl
git pull
cd /wrf

options="$ID $CONFIG $MODE $WPS $INPUT"

#echo "Options: $options"

python3.6 /wrf/curwsl/curwrf/container/docker/ncar-wrf/run_wrf.py -run_id="$ID" -wrf_config="$CONFIG" -mode="$MODE" -nl_wps="$WPS" -nl_input="$INPUT"
#python3 run_wrf.py -run_id="$ID" -wrf_config="$CONFIG" -mode="$MODE" -nl_wps="$WPS" -nl_input="$INPUT"