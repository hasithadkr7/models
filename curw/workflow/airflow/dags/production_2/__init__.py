import json

from curw.rainfall.wrf.resources import manager as res_manager
from curw.workflow.airflow import utils as af_utils


class WrfDefaults:
    RUN_ID = "TEST"

    WRF_CONFIG = {
        "run_id": RUN_ID,
        "wrf_home": "/wrf",
        "gfs_dir": "/wrf/gfs",
        "nfs_dir": "/wrf/output",
        "geog_dir": "/wrf/geog",
        "archive_dir": "/wrf/archive",
        "procs": 4,
        "period": 3,
    }

    WRF_CONFIG_TEMPLATES = {
        "start_date": "{{ execution_date.strftime(\'%Y-%m-%d_%H:%M\')}}",
    }

    NAMELIST_WPS = af_utils.read_file(res_manager.get_resource_path('execution/namelist.wps'))

    NAMELIST_INPUT = af_utils.read_file(res_manager.get_resource_path('execution/namelist.input_SIDAT'))

    VOL_MOUNTS = [
        "/samba/wrf-static-data/geog:/wrf/geog",
        "/samba/data:/wrf/output",
        "/samba/archive:/wrf/archive"
    ]

    DAG_RUN_CONFIG = {
        'run_id': RUN_ID,
        'wrf_config': json.dumps(WRF_CONFIG),
        'mode': 'all',
        'namelist_wps_b64': af_utils.get_base64_encoded_str(NAMELIST_WPS),
        'namelist_input_b64': af_utils.get_base64_encoded_str(NAMELIST_INPUT),
        'gcs_key_b64': '',
        'gcs_vol_mounts': [],
        'vol_mounts': VOL_MOUNTS,
    }
