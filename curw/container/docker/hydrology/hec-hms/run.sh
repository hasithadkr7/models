#!/usr/bin/env bash

err() { 1>&2 echo "$0: error $@"; return 1; }

export GOOGLE_APPLICATION_CREDENTIALS=/hec-hms/gcs.json

echo "#### Reading running args..."
while getopts ":i:c:k:v:h" option
do
 case "${option}"
 in
 i) ID=$OPTARG;;
 c) CMD=$OPTARG;;
 k) GCS_KEY=$OPTARG
    if [ -f "$GCS_KEY" ]; then
        echo "#### using GCS KEY file location"
        cat "$GCS_KEY" > ${GOOGLE_APPLICATION_CREDENTIALS}
    else
        echo "#### using GCS KEY file content"
        echo "$GCS_KEY" > ${GOOGLE_APPLICATION_CREDENTIALS}
    fi
    ;;
 v) bucket=$(echo "$OPTARG" | cut -d':' -f1)
    path=$(echo "$OPTARG" | cut -d':' -f2)
    echo "#### mounting $bucket to $path"
    mkdir -pf "$path"
    gcsfuse "$bucket" "$path" ;;
 h) echo "Help text goes here!"
    exit 1 ;;
 :) err "Option -$OPTARG requires an argument." || exit 1 ;;
 \?) err "Invalid option: -$OPTARG" || exit 1 ;;
 esac
done

echo "#### Pulling curwsl changes..."
cd /hec-hms || exit & git pull & cd -

echo "#### Running HEC-HMS..."
/hec-hms/Forecast.sh $( echo "$CMD" | base64 --decode )


#Xvfb :1 -screen 0 1024x768x24 &> xvfb.log &
#export DISPLAY=:1.0
#echo "Xvbf is running..."
#
#./hec-hms-421/hec-hms.sh &
#echo "hec-hms is running..."
#
#cd /home/hec-dssvue201
#./hec-dssvue.sh &
##echo "hec-dssvue is running..."
#cd /home
#
#export AIRFLOW_HOME=/home/airflow
#mkdir ./OUTPUT
#
##airflow initdb
##sleep 5
##echo "Starting Airflow Webserver"
##airflow webserver -p 8080 &
#/bin/bash
