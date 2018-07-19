import json

from curw.rainfall.wrf.resources import manager as res_manager
from curw.workflow.airflow import utils as af_utils
from curw.workflow.airflow.extensions.operators.curw_dag_run_operator import dict_merge


def get_run_id(run_id='TEST'):
    return run_id


def get_wrf_config(**kwargs):
    wrf_config = {
        "run_id": get_run_id(),
        "wrf_home": "/wrf",
        "gfs_dir": "/wrf/gfs",
        "nfs_dir": "/wrf/output",
        "geog_dir": "/wrf/geog",
        "archive_dir": "/wrf/archive",
        "procs": 4,
        "period": 3,
    }

    return wrf_config.update(kwargs)


def get_wrf_config_templates(**kwargs):
    templates = {
        "start_date": "{{ execution_date.strftime(\'%Y-%m-%d_%H:%M\')}}",
    }
    return templates.update(kwargs)


def get_namelist_wps():
    return af_utils.read_file(res_manager.get_resource_path('execution/namelist.wps'))


def get_namelist_input():
    return af_utils.read_file(res_manager.get_resource_path('execution/namelist.input_SIDAT'))


def get_vol_mounts(*args):
    vols = [
        "/samba/wrf-static-data/geog:/wrf/geog",
        "/samba/data:/wrf/output",
        "/samba/archive:/wrf/archive"
    ]

    return vols.extend(args)


def get_dag_run_config(**kwargs):
    drc = {
        'run_id': get_run_id(),
        'wrf_config': json.dumps(get_wrf_config()),
        'mode': 'all',
        'namelist_wps_b64': af_utils.get_base64_encoded_str(get_namelist_wps()),
        'namelist_input_b64': af_utils.get_base64_encoded_str(get_namelist_input()),
        'gcs_key_b64': '',
        'gcs_vol_mounts': [],
        'vol_mounts': get_vol_mounts(),
    }

    return dict_merge(drc, kwargs, add_keys=False)
