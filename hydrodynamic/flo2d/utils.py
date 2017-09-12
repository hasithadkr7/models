import numpy as np
import datetime as dt

from sqlalchemy.orm.util import randomize_unitofwork

from curwrf.wrf.extraction import utils as rf_ext_utils
from curwrf.wrf.resources import manager as res_mgr


def create_raincell_from_wrf(run_ts, wrf_out, raincell_points_file, observed_points_polygon, output):
    raincell_points = np.genfromtxt(raincell_points_file, delimiter=',')

    lon_min = np.min(raincell_points, 0)[1]
    lat_min = np.min(raincell_points, 0)[2]
    lon_max = np.max(raincell_points, 0)[1]
    lat_max = np.max(raincell_points, 0)[2]

    rf_vars = ['RAINC', 'RAINNC']

    rf_values = rf_ext_utils.extract_variables(wrf_out, rf_vars, lat_min, lat_max, lon_min, lon_max)

    cum_precip = rf_values[rf_vars[0]]
    for i in range(1, len(rf_vars)):
        cum_precip = cum_precip + rf_values[rf_vars[i]]

    # run_ts = run_ts if isinstance(run_ts, dt.datetime) else dt.datetime.strptime(run_ts, '%Y-%m-%d %H:%M:%S')
    # first_ts = dt.datetime.strptime(rf_values['Times'][0], '%Y-%m-%d %H:%M:%S')

    ts_idx = int(np.argwhere(rf_values['Times'] == run_ts))


    pass


def test_create_raincell_from_wrf():
    create_raincell_from_wrf('2017-08-22_06:00:00',
                             '/home/curw/wrf_compare/mnt/disks/curwsl_nfs/output/wrf0/2017-08-22_00:00/0/wrfout_d03_2017-08-22_00:00:00_SL',
                             res_mgr.get_resource_path('extraction/local/kelani_basin_points_250m.txt'),
                             res_mgr.get_resource_path('extraction/shp/kelani-upper-basin.shp'),
                             '/tmp/raincell'
                             )
