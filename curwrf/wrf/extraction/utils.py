import os
import math
import logging
import numpy as np
import matplotlib.pyplot as plt
from netCDF4._netCDF4 import Dataset
from mpl_toolkits.basemap import Basemap, cm

from curwrf.wrf import utils


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

    # todo: complete this function

    return vars_dict, lats[lat_inds[0]], lons[lon_inds[0]], times


def ncks_extract_variables(nc_file, variables, dest):
    v = ','.join(variables)
    logging.info('ncks extraction of %s for %s vars to %s' % (nc_file, v, dest))
    ncks_query = 'ncks -v %s %s %s' % (v, nc_file, dest)
    utils.run_subprocess(ncks_query)


def create_asc_file(data, lats, lons, out_file_path, cell_size=0.1, no_data_val=-99, overwrite=False):
    if not utils.file_exists_nonempty(out_file_path) or overwrite:
        with open(out_file_path, 'wb') as out_file:
            out_file.write(('NCOLS %d\n' % len(lons)).encode())
            out_file.write(('NROWS %d\n' % len(lats)).encode())
            out_file.write(('XLLCORNER %f\n' % lons[0]).encode())
            out_file.write(('YLLCORNER %f\n' % lats[0]).encode())
            out_file.write(('CELLSIZE %f\n' % cell_size).encode())
            out_file.write(('NODATA_VALUE %d\n' % no_data_val).encode())

            np.savetxt(out_file, data, fmt='%g')
    else:
        logging.info('%s already exits' % out_file_path)


def create_contour_plot(data, out_file_path, lat_min, lon_min, lat_max, lon_max, plot_title, basemap=None, clevs=None,
                        cmap=plt.get_cmap('Reds'), overwrite=False):
    """
    create a contour plot using basemap
    :param cmap: color map
    :param clevs: color levels
    :param basemap: creating basemap takes time, hence you can create it outside and pass it over
    :param plot_title:
    :param data: 2D grid data
    :param out_file_path:
    :param lat_min:
    :param lon_min:
    :param lat_max:
    :param lon_max:
    :param overwrite:
    :return:
    """
    if not utils.file_exists_nonempty(out_file_path) or overwrite:
        fig = plt.figure(figsize=(8.27, 11.69))
        ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
        if basemap is None:
            basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max,
                              urcrnrlat=lat_max,
                              resolution='h')
        basemap.drawcoastlines()
        parallels = np.arange(math.floor(lat_min) - 1, math.ceil(lat_max) + 1, 1)
        basemap.drawparallels(parallels, labels=[1, 0, 0, 0], fontsize=10)
        meridians = np.arange(math.floor(lon_min) - 1, math.ceil(lon_max) + 1, 1)
        basemap.drawmeridians(meridians, labels=[0, 0, 0, 1], fontsize=10)

        ny = data.shape[0]
        nx = data.shape[1]
        lons, lats = basemap.makegrid(nx, ny)

        if clevs is None:
            clevs = np.arange(-1, np.max(data) + 1, 1)

        # cs = basemap.contourf(lons, lats, data, clevs, cmap=cm.s3pcpn_l, latlon=True)
        cs = basemap.contourf(lons, lats, data, clevs, cmap=cmap, latlon=True)

        cbar = basemap.colorbar(cs, location='bottom', pad="5%")
        cbar.set_label('mm')

        plt.title(plot_title)
        plt.draw()
        fig.savefig(out_file_path)
        plt.close()
    else:
        logging.info('%s already exists' % out_file_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    lon_min = 79.994117
    lat_min = 6.754167
    lon_max = 80.773182
    lat_max = 7.229167

    f = '/home/nira/curw/OUTPUT/wrfout_d03_2017-04-30_00:00:00'
    extract_variables(f, 'RAINC RAINNC', lat_min, lat_max, lon_min, lon_max)
