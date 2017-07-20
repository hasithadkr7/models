import argparse
import datetime as dt
import logging
import math
import multiprocessing
import os
import shutil
import zipfile
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from curwrf.wrf import utils
from joblib import Parallel, delayed
from mpl_toolkits.basemap import Basemap, cm


def extract_jaxa_satellite_data(start_ts_utc, end_ts_utc, output_dir, cleanup=True):
    start = utils.datetime_floor(start_ts_utc, 3600)
    end = utils.datetime_floor(end_ts_utc, 3600)

    lat_min = 5.722969
    lon_min = 79.52146
    lat_max = 10.06425
    lon_max = 82.18992

    login = 'rainmap:Niskur+1404'

    url0 = 'ftp://' + login + '@hokusai.eorc.jaxa.jp/realtime/txt/05_AsiaSS/YYYY/MM/DD/gsmap_nrt.YYYYMMDD.HH00.05_AsiaSS.csv.zip'
    url1 = 'ftp://' + login + '@hokusai.eorc.jaxa.jp/now/txt/05_AsiaSS/gsmap_now.YYYYMMDD.HH00_HH59.05_AsiaSS.csv.zip'

    def get_jaxa_url(ts):
        url_switch = (dt.datetime.utcnow() - ts) > dt.timedelta(hours=5)
        _url = url0 if url_switch else url1
        ph = {'YYYY': ts.strftime('%Y'),
              'MM': ts.strftime('%m'),
              'DD': ts.strftime('%d'),
              'HH': ts.strftime('%H')}
        for k, v in ph.iteritems():
            _url = _url.replace(k, v)
        return _url

    tmp_dir = os.path.join(output_dir, 'tmp_jaxa/')
    if not os.path.exists(tmp_dir):
        os.mkdir(tmp_dir)

    url_dest_list = []
    for timestamp in np.arange(start, end, dt.timedelta(hours=1)).astype(dt.datetime):
        url = get_jaxa_url(timestamp)
        url_dest_list.append((url, os.path.join(tmp_dir, os.path.basename(url)),
                              os.path.join(output_dir, 'jaxa_sat_rf_' + timestamp.strftime('%Y-%m-%d_%H:%M') + '.asc')))

    procs = multiprocessing.cpu_count()

    logging.info('Downloading inventory in parallel')
    utils.download_parallel(url_dest_list, procs)
    logging.info('Downloading inventory complete')

    logging.info('Processing files in parallel')
    Parallel(n_jobs=procs)(
        delayed(process_jaxa_zip_file)(i[1], i[2], lat_min, lon_min, lat_max, lon_max, True) for i in url_dest_list)
    logging.info('Processing files complete')

    logging.info('Processing cumulative')
    process_cumulative_plot(url_dest_list, start_ts_utc, end_ts_utc, output_dir, lat_min, lon_min, lat_max, lon_max)
    logging.info('Processing cumulative complete')

    # clean up temp dir
    if cleanup:
        logging.info('Cleaning up')
        shutil.rmtree(tmp_dir)


def process_cumulative_plot(url_dest_list, start_ts_utc, end_ts_utc, output_dir, lat_min, lon_min, lat_max, lon_max):
    from_to = '%s-%s' % (start_ts_utc.strftime('%Y-%m-%d_%H:%M'), end_ts_utc.strftime('%Y-%m-%d_%H:%M'))
    cum_filename = os.path.join(output_dir, 'jaxa_sat_cum_rf_' + from_to + '.png')

    if not utils.file_exists_nonempty(cum_filename):
        total = None
        for url_dest in url_dest_list:
            if total is None:
                total = np.genfromtxt(url_dest[2] + '.archive', dtype=float)
            else:
                total += np.genfromtxt(url_dest[2] + '.archive', dtype=float)
        title = 'Cumulative rainfall ' + from_to
        clevs = np.concatenate(([-1, 0], np.array([pow(2, i) for i in range(0, 9)])))
        create_contour_plot(total, cum_filename, lat_min, lon_min, lat_max, lon_max, title, clevs=clevs, cmap=cm.s3pcpn)
    else:
        logging.info('%s already exits' % cum_filename)


def process_jaxa_zip_file(zip_file_path, out_file_path, lat_min, lon_min, lat_max, lon_max, archive_data=False):
    sat_zip = zipfile.ZipFile(zip_file_path)
    sat = np.genfromtxt(sat_zip.open(os.path.basename(zip_file_path).replace('.zip', '')), delimiter=',',
                        names=True)
    sat_filt = np.sort(
        sat[(sat['Lat'] <= lat_max) & (sat['Lat'] >= lat_min) & (sat['Lon'] <= lon_max) & (sat['Lon'] >= lon_min)],
        order=['Lat', 'Lon'])
    lats = np.sort(np.unique(sat_filt['Lat']))
    lons = np.sort(np.unique(sat_filt['Lon']))

    data = sat_filt['RainRate'].reshape(len(lats), len(lons))

    if not utils.file_exists_nonempty(out_file_path):
        cell_size = 0.1
        no_data_val = -99
        with open(out_file_path, 'w') as out_file:
            out_file.write('NCOLS %d\n' % len(lons))
            out_file.write('NROWS %d\n' % len(lats))
            out_file.write('XLLCORNER %f\n' % lons[0])
            out_file.write('YLLCORNER %f\n' % lats[0])
            out_file.write('CELLSIZE %f\n' % cell_size)
            out_file.write('NODATA_VALUE %d\n' % no_data_val)

            sat_flipped = np.flip(sat_filt['RainRate'].reshape(len(lats), len(lons)), 0)
            np.savetxt(out_file, sat_flipped, fmt='%g')
    else:
        logging.info('%s already exits' % out_file_path)

    if not utils.file_exists_nonempty(out_file_path + '.png'):
        clevs = np.concatenate(([-1, 0], np.array([pow(2, i) for i in range(0, 9)])))
        title = 'Hourly rainfall ' + os.path.basename(out_file_path).replace('jaxa_sat_rf_', '').replace('.asc', '')
        create_contour_plot(data, out_file_path + '.png', lat_min, lon_min, lat_max, lon_max, title, clevs=clevs,
                            cmap=cm.s3pcpn)
    else:
        logging.info('%s already exits' % (out_file_path + '.png'))

    if archive_data and not utils.file_exists_nonempty(out_file_path + '.archive'):
        np.savetxt(out_file_path + '.archive', data, fmt='%g')
    else:
        logging.info('%s already exits' % (out_file_path + '.archive'))


def create_contour_plot(data, out_file_path, lat_min, lon_min, lat_max, lon_max, plot_title, basemap=None, clevs=None,
                        cmap=plt.get_cmap('Reds')):
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
    :return:
    """
    fig = plt.figure(figsize=(8.27, 11.69))
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    if basemap is None:
        basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max, urcrnrlat=lat_max,
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description='Run all stages of WRF')
    parser.add_argument('-start',
                        default=(dt.datetime.utcnow() - dt.timedelta(days=1, hours=1)).strftime('%Y-%m-%d_%H:%M'),
                        help='Start timestamp UTC with format %%Y-%%m-%%d_%%H:%%M', dest='start_ts')
    parser.add_argument('-end', default=(dt.datetime.utcnow() - dt.timedelta(hours=1)).strftime('%Y-%m-%d_%H:%M'),
                        help='End timestamp UTC with format %%Y-%%m-%%d_%%H:%%M', dest='end_ts')
    parser.add_argument('-output', default=None, help='Output directory of the images', dest='output')
    parser.add_argument('-clean', default=0, help='Cleanup temp directory', dest='clean')
    args = parser.parse_args()

    if args.output is None:
        output = os.path.join(utils.get_output_dir(), 'jaxa_sat')
    else:
        output = args.output

    extract_jaxa_satellite_data(dt.datetime.strptime(args.start_ts, '%Y-%m-%d_%H:%M'),
                                dt.datetime.strptime(args.end_ts, '%Y-%m-%d_%H:%M'),
                                output, bool(int(args.clean)))
