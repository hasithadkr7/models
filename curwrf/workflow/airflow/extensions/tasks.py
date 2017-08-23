import json
import logging
import os
import shutil
import numpy as np

from airflow.models import Variable
from mpl_toolkits.basemap import cm, Basemap

from curwrf.wrf import utils
from curwrf.wrf.execution import executor
from curwrf.wrf.execution.executor import WrfConfig
from curwrf.wrf.extraction import utils as ext_utils
from curwrf.workflow.airflow.dags import utils as dag_utils


class CurwTask(object):
    def __init__(self):
        self.config = WrfConfig()

    def pre_process(self, *args, **kwargs):
        pass

    def post_process(self, *args, **kwargs):
        pass

    def process(self, *args, **kwargs):
        pass

    def get_config(self, **kwargs):
        if self.config.is_empty():
            self.set_config(**kwargs)
        return self.config

    def set_config(self, **kwargs):
        raise NotImplementedError('Provide a way to get the config!')


class WrfTask(CurwTask):
    def __init__(self, wrf_config_key='wrf_config'):
        self.wrf_config_key = wrf_config_key
        super(WrfTask, self).__init__()

    def set_config(self, **kwargs):
        wrf_config_json = None
        if 'ti' in kwargs:
            wrf_config_json = kwargs['ti'].xcom_pull(task_ids=None, key=self.wrf_config_key)

        if wrf_config_json is not None:
            logging.info('wrf_config from xcom using %s key: %s' % (self.wrf_config_key, wrf_config_json))
            self.config = WrfConfig(json.loads(wrf_config_json))
        else:
            try:
                self.config = WrfConfig(Variable.get(self.wrf_config_key, deserialize_json=True))
                logging.info(
                    'wrf_config from variable using %s key: %s' % (self.wrf_config_key, self.config.to_json_string))
            except KeyError:
                raise CurwAriflowTasksException('Unable to find WrfConfig')

    def add_config_item(self, key, value):
        self.config.set(key, value)
        Variable.set(self.wrf_config_key, self.config.to_json_string())


class Ungrib(WrfTask):
    def pre_process(self, *args, **kwargs):
        logging.info('Running preprocessing for ungrib...')

        wrf_config = self.get_config(**kwargs)

        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))
        logging.info('WPS dir: %s' % wps_dir)

        logging.info('Cleaning up files')
        utils.delete_files_with_prefix(wps_dir, 'FILE:*')
        utils.delete_files_with_prefix(wps_dir, 'PFILE:*')

        # Linking VTable
        if not os.path.exists(os.path.join(wps_dir, 'Vtable')):
            logging.info('Creating Vtable symlink')
            os.symlink(os.path.join(wps_dir, 'ungrib/Variable_Tables/Vtable.NAM'), os.path.join(wps_dir, 'Vtable'))
        pass

    def process(self, *args, **kwargs):
        logging.info('Running ungrib...')

        wrf_config = self.get_config(**kwargs)
        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

        # Running link_grib.csh
        logging.info('Running link_grib.csh')
        gfs_date, gfs_cycle, start = utils.get_appropriate_gfs_inventory(wrf_config)
        # use get_gfs_data_url_dest_tuple to get
        dest = \
            utils.get_gfs_data_url_dest_tuple(wrf_config.get('gfs_url'), wrf_config.get('gfs_inv'), gfs_date, gfs_cycle,
                                              '', wrf_config.get('gfs_res'), '')[1]

        utils.run_subprocess(
            'csh link_grib.csh %s/%s' % (wrf_config.get('gfs_dir'), dest), cwd=wps_dir)

        utils.run_subprocess('./ungrib.exe', cwd=wps_dir)


class Metgrid(WrfTask):
    def pre_process(self, *args, **kwargs):
        logging.info('Running pre-processing for metgrid...')

        wrf_config = self.get_config(**kwargs)
        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))
        utils.delete_files_with_prefix(wps_dir, 'met_em*')

    def process(self, *args, **kwargs):
        logging.info('Running metgrid...')
        wrf_config = self.get_config(**kwargs)
        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

        utils.run_subprocess('./metgrid.exe', cwd=wps_dir)

    def post_process(self, *args, **kwargs):
        # make a sym link in the nfs dir
        wrf_config = self.get_config(**kwargs)
        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

        nfs_metgrid_dir = os.path.join(wrf_config.get('nfs_dir'), 'metgrid')

        utils.create_dir_if_not_exists(nfs_metgrid_dir)
        # utils.delete_files_with_prefix(nfs_metgrid_dir, 'met_em.d*')
        # utils.create_symlink_with_prefix(wps_dir, 'met_em.d*', nfs_metgrid_dir)
        utils.delete_files_with_prefix(nfs_metgrid_dir, 'met_em.d*')
        utils.move_files_with_prefix(wps_dir, 'met_em.d*', nfs_metgrid_dir)


class Geogrid(WrfTask):
    def process(self, *args, **kwargs):
        logging.info('Running geogrid...')

        wrf_config = self.get_config(**kwargs)
        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

        if not executor.check_geogrid_output(wps_dir):
            logging.info('Running Geogrid.exe')
            utils.run_subprocess('./geogrid.exe', cwd=wps_dir)
        else:
            logging.info('Geogrid output already available')


class Real(WrfTask):
    def pre_process(self, *args, **kwargs):
        wrf_config = self.get_config(**kwargs)
        wrf_home = wrf_config.get('wrf_home')
        nfs_metgrid_dir = os.path.join(wrf_config.get('nfs_dir'), 'metgrid')

        logging.info('Running em_real...')
        em_real_dir = utils.get_em_real_dir(wrf_home)

        logging.info('Cleaning up files')
        utils.delete_files_with_prefix(em_real_dir, 'met_em*')
        utils.delete_files_with_prefix(em_real_dir, 'rsl*')

        # Linking met_em.*
        logging.info('Creating met_em.d* symlinks')
        utils.copy_files_with_prefix(nfs_metgrid_dir, 'met_em.d*', em_real_dir)

    def process(self, *args, **kwargs):
        wrf_home = self.get_config(**kwargs).get('wrf_home')
        em_real_dir = utils.get_em_real_dir(wrf_home)
        procs = self.get_config(**kwargs).get('procs')
        utils.run_subprocess('mpirun -np %d ./real.exe' % procs, cwd=em_real_dir)

    def post_process(self, *args, **kwargs):
        wrf_home = self.get_config(**kwargs).get('wrf_home')
        start_date = self.get_config(**kwargs).get('start_date')
        em_real_dir = utils.get_em_real_dir(wrf_home)

        logging.info('Moving the real logs')
        utils.move_files_with_prefix(em_real_dir, 'rsl*', os.path.join(utils.get_logs_dir(wrf_home),
                                                                       'rsl-real-%s' % start_date))


class Wrf(WrfTask):
    def process(self, *args, **kwargs):
        wrf_home = self.get_config(**kwargs).get('wrf_home')
        em_real_dir = utils.get_em_real_dir(wrf_home)
        procs = self.get_config(**kwargs).get('procs')

        logging.info('Replacing namelist.input place-holders')
        executor.replace_namelist_input(self.get_config(**kwargs))

        utils.run_subprocess('mpirun -np %d ./wrf.exe' % procs, cwd=em_real_dir)

    def post_process(self, *args, **kwargs):
        config = self.get_config(**kwargs)
        wrf_home = config.get('wrf_home')
        em_real_dir = utils.get_em_real_dir(wrf_home)
        start_date = config.get('start_date')

        logging.info('Moving the WRF logs')
        utils.move_files_with_prefix(em_real_dir, 'rsl*',
                                     os.path.join(utils.get_logs_dir(wrf_home), 'rsl-wrf-%s' % start_date))

        logging.info('Moving the WRF files to output directory')
        # move the d03 to nfs
        # ex: /mnt/disks/wrf-mod/nfs/output/wrf0/2017-08-13_00:00/0 .. n
        d03_dir = dag_utils.get_incremented_dir_path(
            os.path.join(config.get('nfs_dir'), 'output', os.path.basename(wrf_home), start_date, '0'))
        self.add_config_item('wrf_output_dir', d03_dir)

        d03_file = os.path.join(em_real_dir, 'wrfout_d03_' + start_date + ':00')
        ext_utils.ncks_extract_variables(d03_file, ['RAINC', 'RAINNC', 'XLAT', 'XLONG', 'Times'], d03_file + '_SL')

        # move the wrfout_SL and the namelist files to the nfs
        utils.create_dir_if_not_exists(d03_dir)
        shutil.move(d03_file + '_SL', d03_dir)
        shutil.copy2(os.path.join(em_real_dir, 'namelist.input'), d03_dir)

        # move the rest to the OUTPUT dir of each run
        # todo: in the docker impl - FIND A BETTER WAY
        archive_dir = dag_utils.get_incremented_dir_path(os.path.join(utils.get_output_dir(wrf_home), start_date))
        utils.move_files_with_prefix(em_real_dir, 'wrfout_d*', archive_dir)


class RainfallExtraction(WrfTask):
    def process(self, *args, **kwargs):
        config = self.get_config(**kwargs)
        start_date = config.get('start_date')
        d03_dir = config.get('wrf_output_dir')
        d03_sl = os.path.join(d03_dir, 'wrfout_d03_' + start_date + ':00_SL')

        # create a temp work dir & get a local copy of the d03.._SL
        temp_dir = utils.create_dir_if_not_exists(os.path.join(config.get('wrf_home'), 'temp'))
        shutil.copy2(d03_sl, temp_dir)

        d03_sl = os.path.join(temp_dir, os.path.basename(d03_sl))

        lat_min = 5.722969
        lon_min = 79.52146
        lat_max = 10.06425
        lon_max = 82.18992

        variables = ext_utils.extract_variables(d03_sl, 'RAINC, RAINNC', lat_min, lat_max, lon_min, lon_max)

        lats = variables['XLAT']
        lons = variables['XLONG']

        # cell size is calc based on the mean between the lat and lon points
        cell_size = np.round(
            np.mean(np.append(lons[1:len(lons)] - lons[0: len(lons) - 1], lats[1:len(lats)] - lats[0: len(lats) - 1])),
            3)
        clevs = np.concatenate(([-1, 0], np.array([pow(2, i) for i in range(0, 9)])))

        basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max,
                          urcrnrlat=lat_max, resolution='h')

        for i in range(1, len(variables['Times'])):
            time = variables['Times'][i]
            logging.info('processing %s', time)

            cum_precip = variables['RAINC'][i] + variables['RAINNC'][i]
            # instantaneous precipitation (hourly)
            inst_precip = cum_precip - (variables['RAINC'][i - 1] + variables['RAINNC'][i - 1])

            inst_file = os.path.join(temp_dir, 'wrf_inst_' + time)
            ext_utils.create_asc_file(np.flip(inst_precip, 0), lats, lons, inst_file + '.asc', cell_size=cell_size)
            ext_utils.create_contour_plot(inst_precip, inst_file + '.png', lat_min, lon_min, lat_max, lon_max,
                                          os.path.basename(inst_file), clevs=clevs, cmap=cm.s3pcpn, basemap=basemap)

            if i == len(variables['Times']) - 1:
                cum_file = os.path.join(temp_dir, 'wrf_cum_' + time)
                ext_utils.create_asc_file(np.flip(cum_precip, 0), lats, lons, cum_file + '.asc', cell_size=cell_size)
                ext_utils.create_contour_plot(cum_precip, cum_file + '.png', lat_min, lon_min, lat_max, lon_max,
                                              os.path.basename(cum_file), clevs=clevs, cmap=cm.s3pcpn, basemap=basemap)

        # move all the data in the tmp dir to the nfs
        utils.move_files_with_prefix(temp_dir, '*.png', d03_dir)
        utils.move_files_with_prefix(temp_dir, '*.asc', d03_dir)
        shutil.rmtree(temp_dir)


def test_rainfall_extraction():
    rf_task = RainfallExtraction()
    rf_task.config = WrfConfig({
        'wrf_home': '/home/curw/Desktop/temp',
        'wrf_output_dir': '/home/curw/Desktop/temp',
        'start_date': '2017-08-13_00:00'
    })

    rf_task.process()


class CurwAriflowTasksException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)
