import argparse
import datetime as dt
import glob
import logging
import logging.config
import ntpath
import os
import re
import shlex
import shutil
import subprocess
import time
import pkg_resources
import yaml

from shapely.geometry import Point, shape

from curw.wrf import constants


def parse_args():
    parser = argparse.ArgumentParser(description='Run all stages of WRF')
    parser.add_argument('-wrf', default=constants.DEFAULT_WRF_HOME, help='WRF home', dest='wrf_home')
    parser.add_argument('-start', default=dt.datetime.today().strftime('%Y-%m-%d'),
                        help='Start date with format %%Y-%%m-%%d', dest='start_date')
    parser.add_argument('-end', default=(dt.datetime.today() + dt.timedelta(days=1)).strftime('%Y-%m-%d'),
                        help='End date with format %%Y-%%m-%%d', dest='end_date')
    parser.add_argument('-period', default=3, help='Period of days for each run', type=int)
    return parser.parse_args()


def set_logging_config(log_home):
    default_config = dict(
        version=1,
        formatters={
            'f': {'format': '%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s'}
        },
        handlers={
            'h': {'class': 'logging.StreamHandler',
                  'formatter': 'f',
                  'level': logging.INFO},
            'fh': {'class': 'logging.handlers.TimedRotatingFileHandler',
                   'formatter': 'f',
                   'level': logging.INFO,
                   'filename': os.path.join(log_home, 'wrfrun.log'),
                   'when': 'D',
                   'interval': 1
                   }
        },
        root={
            'handlers': ['h', 'fh'],
            'level': logging.INFO,
        },
    )

    path = pkg_resources.resource_filename(__name__, 'logging.yaml')
    value = os.getenv(constants.LOGGING_ENV_VAR, None)

    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        config['handlers']['fh']['filename'] = os.path.join(log_home, 'wrfrun.log')
        logging.config.dictConfig(config)
    else:
        logging.config.dictConfig(default_config)


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_gfs_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'DATA', 'GFS'))


def get_wps_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, constants.DEFAULT_WPS_PATH)


def get_em_real_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, constants.DEFAULT_EM_REAL_PATH)


def get_geog_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, 'DATA', 'geog')


def get_output_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'OUTPUT'))


def get_scripts_run_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'wrf-scripts', 'run'))


def get_logs_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'logs'))


def get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, fcst_id, res, gfs_dir):
    url0 = url.replace('YYYYMMDD', date_str).replace('CC', cycle)
    inv0 = inv.replace('CC', cycle).replace('FFF', fcst_id).replace('RRRR', res)
    dest = os.path.join(gfs_dir, date_str + '.' + inv0)
    return url0 + inv0, dest


def get_gfs_data_dest(inv, date_str, cycle, fcst_id, res, gfs_dir):
    inv0 = inv.replace('CC', cycle).replace('FFF', fcst_id).replace('RRRR', res)
    dest = os.path.join(gfs_dir, date_str + '.' + inv0)
    return dest


def get_gfs_inventory_url_dest_list(date, period, url, inv, step, cycle, res, gfs_dir):
    date_str = date.strftime('%Y%m%d')
    return [get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, str(i).zfill(3), res, gfs_dir) for i in
            range(0, period * 24 + 1, step)]


def get_gfs_inventory_dest_list(date, period, inv, step, cycle, res, gfs_dir):
    date_str = date.strftime('%Y%m%d')
    return [get_gfs_data_dest(inv, date_str, cycle, str(i).zfill(3), res, gfs_dir) for i in
            range(0, period * 24 + 1, step)]


def replace_file_with_values(source, destination, val_dict):
    logging.debug('replace file source ' + source)
    logging.debug('replace file destination ' + destination)
    logging.debug('replace file content dict ' + str(val_dict))

    # pattern = re.compile(r'\b(' + '|'.join(val_dict.keys()) + r')\b')
    pattern = re.compile('|'.join(val_dict.keys()))

    dest = open(destination, 'w')
    out = ''
    with open(source, 'r') as src:
        line = pattern.sub(lambda x: val_dict[x.group()], src.read())
        dest.write(line)
        out += line

    dest.close()
    logging.debug('replace file final content \n' + out)


def cleanup_dir(gfs_dir):
    shutil.rmtree(gfs_dir)
    os.makedirs(gfs_dir)


def delete_files_with_prefix(src_dir, prefix):
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        os.remove(filename)


def move_files_with_prefix(src_dir, prefix, dest_dir):
    create_dir_if_not_exists(dest_dir)
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        os.rename(filename, os.path.join(dest_dir, ntpath.basename(filename)))


def create_symlink_with_prefix(src_dir, prefix, dest_dir):
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        os.symlink(filename, os.path.join(dest_dir, ntpath.basename(filename)))


def run_subprocess(cmd, cwd=None):
    logging.info('Running subprocess %s' % cmd)
    start_t = time.time()
    output = ''
    try:
        output = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, cwd=cwd)
    except subprocess.CalledProcessError as e:
        logging.error('Exception in subprocess %s! Error code %d' % (cmd, e.returncode))
        logging.error(e.output)
        raise e
    finally:
        elapsed_t = time.time() - start_t
        logging.info('Subprocess %s finished in %f s' % (cmd, elapsed_t))
        logging.debug('stdout and stderr of %s\n%s' % (cmd, output))
    return output


def is_inside_polygon(polygons, lat, lon):
    point = Point(lon, lat)
    for i, poly in enumerate(polygons.shapeRecords()):
        polygon = shape(poly.shape.__geo_interface__)
        if point.within(polygon):
            return 1
    return 0


# def namedtuple_with_defaults(typename, field_names, default_values=()):
#     T = namedtuple(typename, field_names)
#     T.__new__.__defaults__ = (None,) * len(T._fields)
#     if isinstance(default_values, collections.Mapping):
#         prototype = T(**default_values)
#     else:
#         prototype = T(*default_values)
#     T.__new__.__defaults__ = tuple(prototype)
#     return T


# def ncdump(nc_fid, verb=True):
#     def print_ncattr(key):
#         try:
#             print "\t\ttype:", repr(nc_fid.variables[key].dtype)
#             for ncattr in nc_fid.variables[key].ncattrs():
#                 print '\t\t%s:' % ncattr, \
#                     repr(nc_fid.variables[key].getncattr(ncattr))
#         except KeyError:
#             print "\t\tWARNING: %s does not contain variable attributes" % key
#
#     # NetCDF global attributes
#     _nc_attrs = nc_fid.ncattrs()
#     if verb:
#         print "NetCDF Global Attributes:"
#         for nc_attr in _nc_attrs:
#             print '\t%s:' % nc_attr, repr(nc_fid.getncattr(nc_attr))
#     _nc_dims = [dim for dim in nc_fid.dimensions]  # list of nc dimensions
#     # Dimension shape information.
#     if verb:
#         print "NetCDF dimension information:"
#         for dim in _nc_dims:
#             print "\tName:", dim
#             print "\t\tsize:", len(nc_fid.dimensions[dim])
#             print_ncattr(dim)
#     # Variable information.
#     _nc_vars = [var for var in nc_fid.variables]  # list of nc variables
#     if verb:
#         print "NetCDF variable information:"
#         for var in _nc_vars:
#             if var not in _nc_dims:
#                 print '\tName:', var
#                 print "\t\tdimensions:", nc_fid.variables[var].dimensions
#                 print "\t\tsize:", nc_fid.variables[var].size
#                 print_ncattr(var)
#     return _nc_attrs, _nc_dims, _nc_vars


def main():
    wrf_home = "/tmp"
    set_logging_config(wrf_home)
    print get_gfs_dir(wrf_home)
    print get_output_dir(wrf_home)
    print get_scripts_run_dir(wrf_home)
    print get_logs_dir(wrf_home)
    print get_gfs_data_url_dest_tuple(constants.DEFAULT_GFS_DATA_URL, constants.DEFAULT_GFS_DATA_INV,
                                      get_gfs_dir(wrf_home), '20170501', constants.DEFAULT_CYCLE, '001',
                                      constants.DEFAULT_RES)
    print get_gfs_inventory_url_dest_list(constants.DEFAULT_GFS_DATA_URL, constants.DEFAULT_GFS_DATA_INV,
                                          dt.datetime.strptime('2017-05-01', '%Y-%m-%d'),
                                          constants.DEFAULT_PERIOD,
                                          constants.DEFAULT_STEP,
                                          constants.DEFAULT_CYCLE,
                                          constants.DEFAULT_RES,
                                          get_gfs_dir(wrf_home))
    d = {
        'YYYY1': '2016',
        'MM1': '05',
        'DD1': '01'
    }

    print replace_file_with_values('resources/namelist.input', wrf_home + '/namelist.wps', d)


if __name__ == "__main__":
    main()
