import ast
import glob
import json
import logging
import os
import shutil
import datetime as dt

from curw.container.docker.rainfall import utils as docker_rf_utils
from curw.rainfall.wrf import utils
from curw.rainfall.wrf.execution import executor
from curw.rainfall.wrf.extraction import extractor
from curw.rainfall.wrf.resources import manager as res_mgr


def run_wrf(wrf_config):
    logging.info('Running WRF')

    logging.info('Replacing the namelist input file')
    executor.replace_namelist_input(wrf_config)

    logging.info('Running WRF...')
    executor.run_em_real(wrf_config)


def run_wps(wrf_config):
    logging.info('Downloading GFS data')
    executor.download_gfs_data(wrf_config)

    logging.info('Replacing the namelist wps file')
    executor.replace_namelist_wps(wrf_config)

    logging.info('Running WPS...')
    executor.run_wps(wrf_config)

    logging.info('Cleaning up wps dir...')
    wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))
    shutil.rmtree(wrf_config.get('gfs_dir'))
    utils.delete_files_with_prefix(wps_dir, 'FILE:*')
    utils.delete_files_with_prefix(wps_dir, 'PFILE:*')
    utils.delete_files_with_prefix(wps_dir, 'geo_em.*')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    args = vars(docker_rf_utils.parse_args())

    logging.info('Running arguments:\n%s' % json.dumps(args, sort_keys=True, indent=0))

    run_id = args['run_id']  # env_vars.pop('run_id', id_generator())
    logging.info('**** Extracting data from WRF **** Run ID: ' + run_id)

    wrf_config_dict = ast.literal_eval(args['wrf_config'])

    config = executor.get_wrf_config(**wrf_config_dict)
    config.set('run_id', run_id)

    wrf_home = config.get('wrf_home')
    wrf_output_dir = utils.create_dir_if_not_exists(os.path.join(config.get('nfs_dir'), 'results', run_id, 'wrf'))

    weather_st_file = res_mgr.get_resource_path('extraction/local/kelani_basin_stations.txt')
    kelani_basin_file = res_mgr.get_resource_path('extraction/local/kelani_basin_points_250m.txt')
    kelani_basin_shp_file = res_mgr.get_resource_path('extraction/shp/kelani-upper-basin.shp')
    jaxa_weather_st_file = res_mgr.get_resource_path('extraction/local/jaxa_weather_stations.txt')

    nc_f = glob.glob(os.path.join(wrf_output_dir, '/wrfout_d03_*'))[0]
    date = dt.datetime.strptime(config.get('start_date'), '%Y-%m-%d_%H:%M')
    output_dir = utils.create_dir_if_not_exists(os.path.join(config.get('nfs_dir'), 'results', run_id))

    logging.info('Extracting data from ' + nc_f)

    logging.info('Extract rainfall data for the metro colombo area')
    basin_rf = extractor.extract_metro_colombo(nc_f, date, output_dir)
    logging.info('Basin rainfall' + str(basin_rf))

    logging.info('Extract weather station rainfall')
    extractor.extract_weather_stations(nc_f, date, times, weather_st_file, wrf_output)