#!/usr/bin/env bash

export CURW_wrf_config=$(cat << EOM
{
	"wrf_home": "/wrf",
	"gfs_dir": "/wrf/gfs",
	"nfs_dir": "/wrf/output",
	"period": 0.25,
	"geog_dir": "/wrf/geog",
	"start_date": "2017-09-05_06:00"
}
EOM
)

export CURW_db_config=$(cat << EOM
{
  "host": "localhost",
  "user": "test",
  "password": "password",
  "db": "testdb"
}
EOM
)

export CURW_run_id=WRF_test_run1

python3 ../extract_data_wrf.py
python3 ../extract_data_wrf.py -wrf_config="$CURW_wrf_config" -run_id="$CURW_run_id" -db_config="$CURW_db_config" -overwrite=True




