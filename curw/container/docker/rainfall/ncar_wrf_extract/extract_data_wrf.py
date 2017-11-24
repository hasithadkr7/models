import argparse
import ast
import glob
import json
import logging
import os
import shutil
from tempfile import TemporaryDirectory

from curw.container.docker.rainfall import utils as docker_rf_utils
from curw.rainfall.wrf.execution.executor import get_wrf_config
from curw.rainfall.wrf.extraction import extractor, constants
from curw.rainfall.wrf.extraction import utils as ext_utils


def parse_args():
    parser = argparse.ArgumentParser()
    env_vars = docker_rf_utils.get_env_vars('CURW_')

    def check_key(k, d_val):
        if k in env_vars and not env_vars[k]:
            return env_vars[k]
        else:
            return d_val

    parser.add_argument('-run_id', default=check_key('run_id', docker_rf_utils.id_generator()))
    parser.add_argument('-db_config', default=check_key('db_config', '{}'))
    parser.add_argument('-wrf_config', default=check_key('wrf_config', '{}'))
    parser.add_argument('-overwrite', default=check_key('overwrite', 'False'))

    return parser.parse_args()


def run(run_id, wrf_config_dict, db_config_dict, upsert=False, run_name='Cloud-1'):
    logging.info('**** Extracting data from WRF **** Run ID: ' + run_id)
    run_prefix = run_id.split('_')[0]

    config = get_wrf_config(**wrf_config_dict)
    config.set('run_id', run_id)

    output_dir_base = os.path.join(config.get('nfs_dir'), 'results')
    run_output_dir = os.path.join(output_dir_base, run_id)
    wrf_output_dir = os.path.join(run_output_dir, 'wrf')

    db_adapter = ext_utils.get_curw_adapter(mysql_config=db_config_dict) if db_config_dict else None

    logging.info('Creating temp file space')

    with TemporaryDirectory(prefix='wrfout_') as temp_dir:
        try:
            logging.info('Copying wrfout_* to temp_dir ' + temp_dir)
            d03_nc_f = shutil.copy2(glob.glob(os.path.join(wrf_output_dir, 'wrfout_d03_*'))[0], temp_dir)
            d01_nc_f = shutil.copy2(glob.glob(os.path.join(wrf_output_dir, 'wrfout_d01_*'))[0], temp_dir)

            logging.info('Extracting data from ' + d03_nc_f)
            try:
                logging.info('Extract WRF data points in the Kelani and Kalu basins')
                lon_min, lat_min, lon_max, lat_max = constants.KELANI_KALU_BASIN_EXTENT
                extractor.push_wrf_rainfall_to_db(d03_nc_f, curw_db_adapter=db_adapter, lat_min=lat_min,
                                                  lon_min=lon_min,
                                                  lat_max=lat_max, lon_max=lon_max, run_prefix=run_prefix,
                                                  upsert=upsert)
            except Exception as e:
                logging.error('Extract WRF data points in the Kelani and Kalu basins FAILED: ' + str(e))

            try:
                logging.info('Extract rainfall data for the metro colombo area')
                basin_rf = extractor.extract_metro_colombo(d03_nc_f, run_output_dir, output_dir_base,
                                                           curw_db_adapter=db_adapter, run_prefix=run_prefix,
                                                           run_name=run_name, curw_db_upsert=upsert)
                logging.info('Basin rainfall' + str(basin_rf))
            except Exception as e:
                logging.error('Extract rainfall data for the metro colombo area FAILED: ' + str(e))

            try:
                logging.info('Extract weather station rainfall')
                extractor.extract_weather_stations(d03_nc_f, run_output_dir, curw_db_adapter=db_adapter,
                                                   curw_db_upsert=upsert, run_prefix=run_prefix, run_name=run_name)
            except Exception as e:
                logging.error('Extract weather station rainfall FAILED: ' + str(e))

            try:
                logging.info('Extract Kelani upper Basin mean rainfall')
                extractor.extract_kelani_upper_basin_mean_rainfall(d03_nc_f, run_output_dir, curw_db_adapter=db_adapter,
                                                                   run_prefix=run_prefix, run_name=run_name,
                                                                   curw_db_upsert=upsert)
            except Exception as e:
                logging.error('Extract Kelani upper Basin mean rainfall FAILED: ' + str(e))

            # logging.info('Extract Kelani Basin rainfall')
            # extractor.extract_kelani_basin_rainfall(d03_nc_f, date, output_dir, avg_basin_rf=basin_rf)

            try:
                logging.info('Create plots for D03')
                lon_min, lat_min, lon_max, lat_max = constants.SRI_LANKA_EXTENT
                extractor.create_rf_plots_wrf(d03_nc_f, os.path.join(run_output_dir, 'plots_D03'), output_dir_base,
                                              lat_min=lat_min, lon_min=lon_min, lat_max=lat_max, lon_max=lon_max,
                                              run_prefix=run_prefix)
            except Exception as e:
                logging.error('Create plots for D03 FAILED: ' + str(e))

            logging.info('Extracting data from ' + d01_nc_f)
            try:
                logging.info('Create plots for D01')
                lon_min, lat_min, lon_max, lat_max = constants.SRI_LANKA_D01_EXTENT
                extractor.create_rf_plots_wrf(d01_nc_f, os.path.join(run_output_dir, 'plots_D01'), output_dir_base,
                                              lat_min=lat_min, lon_min=lon_min, lat_max=lat_max, lon_max=lon_max,
                                              run_prefix=run_prefix)
            except Exception as e:
                logging.error('Create plots for D01 FAILED: ' + str(e))

        except Exception as e:
            logging.error('Copying wrfout_* to temp_dir %s FAILED: %s' % (temp_dir, str(e)))

    logging.info('**** Extracting data from WRF **** Run ID: ' + run_id + ' COMPLETED!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    args = vars(parse_args())

    logging.info('Running arguments:\n%s' % json.dumps(args, sort_keys=True, indent=0))

    run(args['run_id'], ast.literal_eval(args['wrf_config']), ast.literal_eval(args['db_config']),
        ast.literal_eval(args['overwrite']))
