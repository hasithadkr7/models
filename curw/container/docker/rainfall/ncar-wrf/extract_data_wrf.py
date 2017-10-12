import argparse
import ast
import datetime as dt
import glob
import json
import logging
import os

from curw.container.docker.rainfall import utils as docker_rf_utils
from curw.rainfall.wrf import utils
from curw.rainfall.wrf.execution import executor
from curw.rainfall.wrf.extraction import extractor
from curw.rainfall.wrf.extraction import utils as ext_utils
from curw.rainfall.wrf.resources import manager as res_mgr


def parse_args():
    parser = argparse.ArgumentParser()
    env_vars = docker_rf_utils.get_env_vars('CURW_')

    parser.add_argument('-run_id',
                        default=env_vars['run_id'] if 'run_id' in env_vars else docker_rf_utils.id_generator())
    parser.add_argument('-db_config', default=env_vars['db_config'] if 'db_config' in env_vars else None)
    parser.add_argument('-wrf_config', default=env_vars['wrf_config'] if 'wrf_config' in env_vars else '{}')

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    args = vars(parse_args())

    logging.info('Running arguments:\n%s' % json.dumps(args, sort_keys=True, indent=0))

    run_id = args['run_id']  # env_vars.pop('run_id', id_generator())
    logging.info('**** Extracting data from WRF **** Run ID: ' + run_id)

    wrf_config_dict = ast.literal_eval(args['wrf_config'])

    config = executor.get_wrf_config(**wrf_config_dict)
    config.set('run_id', run_id)

    wrf_home = config.get('wrf_home')
    wrf_output_dir = utils.create_dir_if_not_exists(os.path.join(config.get('nfs_dir'), 'results', run_id, 'wrf'))

    db_config_dict = ast.literal_eval(args['db_config'])
    db_adapter = ext_utils.get_curw_adapter(mysql_config=db_config_dict)

    nc_f = glob.glob(os.path.join(wrf_output_dir, '/wrfout_d03_*'))[0]
    date = dt.datetime.strptime(config.get('start_date'), '%Y-%m-%d_%H:%M')
    output_dir = utils.create_dir_if_not_exists(os.path.join(config.get('nfs_dir'), 'results', run_id))

    logging.info('Extracting data from ' + nc_f)

    logging.info('Extract rainfall data for the metro colombo area')
    basin_rf = extractor.extract_metro_colombo(nc_f, date, output_dir, curw_db_adapter=db_adapter)
    logging.info('Basin rainfall' + str(basin_rf))

    logging.info('Extract weather station rainfall')
    extractor.extract_weather_stations(nc_f, date, output_dir, curw_db_adapter=db_adapter)

    logging.info('Extract Kelani upper Basin mean rainfall')
    extractor.extract_kelani_upper_basin_mean_rainfall(nc_f, date, output_dir, curw_db_adapter=db_adapter)

    logging.info('Extract Kelani Basin rainfall')
    extractor.extract_kelani_basin_rainfall(nc_f, date, output_dir, avg_basin_rf=basin_rf)

