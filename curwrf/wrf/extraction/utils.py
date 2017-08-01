import os

import logging
import numpy as np

from netCDF4._netCDF4 import Dataset


def extract_variables(nc_f, vars, lat_min, lat_max, lon_min, lon_max, lat_var='XLAT', lon_var='XLON', time_var='Times',
                      output=None):
    if not os.path.exists(nc_f):
        raise IOError('File %s not found' % nc_f)

    nc_fid = Dataset(nc_f, 'r')

    times = [''.join(x) for x in nc_fid.variables[time_var][:]]
    lats = nc_fid.variables[lat_var][0, :, 0]
    lons = nc_fid.variables[lon_var][0, 0, :]

    lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
    lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

    vars_dict = {}
    for var in vars.replace(',', ' ').split():
        vars_dict[var] = nc_fid.variables[var][:, lat_inds[0], lon_inds[0]]

    nc_fid.close()

    if output is not None:
        logging.info('%s will be archied to %s' % (nc_f, output))
        nc_format = nc_fid.file_format
        nc_out = Dataset('data/test.nc', 'w', format=nc_format)

        time = nc_out.createDimension('Time', len(times))

    return vars_dict, lats[lat_inds[0]], lons[lon_inds[0]], times


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    lon_min = 79.994117
    lat_min = 6.754167
    lon_max = 80.773182
    lat_max = 7.229167

    f = '/home/nira/curw/OUTPUT/wrfout_d03_2017-04-30_00:00:00'
    extract_variables(f, 'RAINC RAINNC', lat_min, lat_max, lon_min, lon_max)
