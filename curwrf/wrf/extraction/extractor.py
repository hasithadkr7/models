import csv
import datetime as dt  # Python standard library datetime  module
import logging
import os
import unittest
import zipfile
import multiprocessing
from matplotlib.colors import LinearSegmentedColormap

import numpy as np
import pandas as pd
import shapefile
import shutil
import matplotlib.pyplot as plt
import math

from joblib import Parallel, delayed
from mpl_toolkits.basemap import Basemap, cm
from netCDF4 import Dataset

from curwrf.wrf import utils
from curwrf.wrf.resources import manager as res_mgr


def extract_time_data(nc_f):
    nc_fid = Dataset(nc_f, 'r')
    times_len = len(nc_fid.dimensions['Time'])
    times = [''.join(x) for x in nc_fid.variables['Times'][0:times_len]]
    nc_fid.close()
    return times_len, times


def extract_metro_colombo(nc_f, date, wrf_output):
    times_len, times = extract_time_data(nc_f)

    nc_fid = Dataset(nc_f, 'r')

    lat_min = 41
    lat_max = 47
    lon_min = 11
    lon_max = 17
    cell_size = 0.02723
    no_data_val = -99

    lats = nc_fid.variables['XLAT'][0, lat_min:lat_max + 1, 0]  # extract/copy the data
    lons = nc_fid.variables['XLONG'][0, 0, lon_min:lon_max + 1]

    prcp = nc_fid.variables['RAINC'][:, lat_min:lat_max + 1, lon_min:lon_max + 1] + \
           nc_fid.variables['RAINNC'][:, lat_min:lat_max + 1, lon_min:lon_max + 1] + \
           nc_fid.variables['SNOWNC'][:, lat_min:lat_max + 1, lon_min:lon_max + 1] + \
           nc_fid.variables['GRAUPELNC'][:, lat_min:lat_max + 1, lon_min:lon_max + 1]

    diff = prcp[1:73, :, :] - prcp[0:72, :, :]

    width = len(lons)
    height = len(lats)

    output_dir = wrf_output + '/colombo/created-' + date.strftime('%Y-%m-%d')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    alpha_file_path = wrf_output + '/colombo/alphas.txt'
    basin_rf = 1.0
    with open(alpha_file_path, 'a') as alpha_file:
        basin_rf = np.sum(diff[5:29, :, :]) / float(width * height)
        alpha_file.write('%s %f\n' % (date.strftime('%Y-%m-%d'), basin_rf))

    subsection_file_path = wrf_output + '/colombo/sub-means-' + date.strftime('%Y-%m-%d') + '.txt'
    subsection_file = open(subsection_file_path, 'w')

    for tm in range(0, len(times) - 1):
        output_file_path = output_dir + '/rain-' + times[tm] + '.txt'
        output_file = open(output_file_path, 'w')

        output_file.write('NCOLS %d\n' % width)
        output_file.write('NROWS %d\n' % height)
        output_file.write('XLLCORNER %f\n' % lons[0])
        output_file.write('YLLCORNER %f\n' % lats[0])
        output_file.write('CELLSIZE %f\n' % cell_size)
        output_file.write('NODATA_VALUE %d\n' % no_data_val)

        for y in range(0, height):
            for x in range(0, width):
                output_file.write('%f ' % diff[tm, y, x])
            output_file.write('\n')

        output_file.close()

        # writing subsection file
        sub_divs = [0, 4, 7]
        subsection_file.write(times[tm])
        for j in range(len(sub_divs) - 1):
            for i in range(len(sub_divs) - 1):
                subsection_file.write(
                    ' %f' % np.mean(diff[tm, sub_divs[j]:sub_divs[j + 1], sub_divs[i]: sub_divs[i + 1]]))
        subsection_file.write('\n')

    subsection_file.close()

    nc_fid.close()
    return basin_rf


def extract_weather_stations(nc_f, date, times, weather_stations, wrf_output):
    nc_fid = Dataset(nc_f, 'r')

    with open(weather_stations, 'rb') as csvfile:
        stations = csv.reader(csvfile, delimiter=' ')
        stations_dir = wrf_output + '/RF'
        if not os.path.exists(stations_dir):
            os.makedirs(stations_dir)
        for row in stations:
            print ' '.join(row)
            lon = row[1]
            lat = row[2]

            station_prcp = nc_fid.variables['RAINC'][:, lat, lon] + \
                           nc_fid.variables['RAINNC'][:, lat, lon] + \
                           nc_fid.variables['SNOWNC'][:, lat, lon] + \
                           nc_fid.variables['GRAUPELNC'][:, lat, lon]

            station_diff = station_prcp[1:len(times)] - station_prcp[0:len(times) - 1]

            station_file_path = stations_dir + '/' + row[0] + '-' + date.strftime('%Y-%m-%d') + '.txt'
            station_file = open(station_file_path, 'w')

            for t in range(0, len(times) - 1):
                station_file.write('%s %f\n' % (times[t], station_diff[t]))
            station_file.close()

    nc_fid.close()


def extract_kelani_basin_rainfall(nc_f, date, kelani_basin_file, wrf_output, basin_rf=1.0):
    points = np.genfromtxt(kelani_basin_file, delimiter=',')

    kel_lon_min = np.min(points, 0)[1]
    kel_lat_min = np.min(points, 0)[2]
    kel_lon_max = np.max(points, 0)[1]
    kel_lat_max = np.max(points, 0)[2]

    diff, kel_lats, kel_lons, times = extract_area_rf_series(nc_f, kel_lat_min, kel_lat_max, kel_lon_min, kel_lon_max)

    def get_bins(arr):
        sz = len(arr)
        return (arr[1:sz - 1] + arr[0:sz - 2]) / 2

    lat_bins = get_bins(kel_lats)
    lon_bins = get_bins(kel_lons)

    output_dir = wrf_output + '/kelani-basin/created-' + date.strftime('%Y-%m-%d')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    prev_day_1_file = wrf_output + '/wrfout_d03_' + (date - dt.timedelta(days=1)).strftime('%Y-%m-%d') + '_00:00:00'
    prev_day_2_file = wrf_output + '/wrfout_d03_' + (date - dt.timedelta(days=2)).strftime('%Y-%m-%d') + '_00:00:00'

    diff1, _, _, times1 = extract_area_rf_series(prev_day_1_file, kel_lat_min, kel_lat_max, kel_lon_min, kel_lon_max)
    diff2, _, _, times2 = extract_area_rf_series(prev_day_2_file, kel_lat_min, kel_lat_max, kel_lon_min, kel_lon_max)

    def write_forecast_to_raincell_file(output_file_path, alpha):
        output_file = open(output_file_path, 'w')

        res = 60
        data_hours = len(times) + 48
        start_ts = (date - dt.timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
        end_ts = (date + dt.timedelta(hours=len(times) - 1)).strftime('%Y-%m-%d %H:%M:%S')
        output_file.write("%d %d %s %s\n" % (res, data_hours, start_ts, end_ts))

        for h in range(0, data_hours):
            for point in points:
                rf_x = np.digitize(point[1], lon_bins)
                rf_y = np.digitize(point[2], lat_bins)
                if h < 24:
                    output_file.write('%d %f\n' % (point[0], diff2[h, rf_y, rf_x]))
                elif h < 48:
                    output_file.write('%d %f\n' % (point[0], diff1[h - 24, rf_y, rf_x]))
                elif h < 72:
                    output_file.write('%d %f\n' % (point[0], diff[h - 48, rf_y, rf_x] * alpha))
                else:
                    output_file.write('%d %f\n' % (point[0], diff[h - 48, rf_y, rf_x]))
        output_file.close()

    raincell_file_path = output_dir + '/RAINCELL.DAT'
    write_forecast_to_raincell_file(raincell_file_path, 1)

    for target_rf in [100, 150, 200, 250, 300]:
        write_forecast_to_raincell_file('%s.%d' % (raincell_file_path, target_rf), target_rf / basin_rf)


def extract_kelani_upper_basin_mean_rainfall(nc_f, date, times, kelani_basin_shp_file, wrf_output):
    nc_fid = Dataset(nc_f, 'r')

    kel_lon_min = 79.994117
    kel_lat_min = 6.754167
    kel_lon_max = 80.773182
    kel_lat_max = 7.229167

    lats = nc_fid.variables['XLAT'][0, :, 0]
    lons = nc_fid.variables['XLONG'][0, 0, :]

    lon_min = np.argmax(lons >= kel_lon_min) - 1
    lat_min = np.argmax(lats >= kel_lat_min) - 1
    lon_max = np.argmax(lons >= kel_lon_max)
    lat_max = np.argmax(lats >= kel_lat_max)

    polys = shapefile.Reader(kelani_basin_shp_file)

    kel_lats = nc_fid.variables['XLAT'][0, lat_min:lat_max + 1, lon_min:lon_max + 1]
    kel_lons = nc_fid.variables['XLONG'][0, lat_min:lat_max + 1, lon_min:lon_max + 1]

    prcp = nc_fid.variables['RAINC'][:, lat_min:lat_max + 1, lon_min:lon_max + 1] + \
           nc_fid.variables['RAINNC'][:, lat_min:lat_max + 1, lon_min:lon_max + 1] + \
           nc_fid.variables['SNOWNC'][:, lat_min:lat_max + 1, lon_min:lon_max + 1] + \
           nc_fid.variables['GRAUPELNC'][:, lat_min:lat_max + 1, lon_min:lon_max + 1]

    diff = prcp[1:len(times), :, :] - prcp[0: len(times) - 1, :, :]

    output_dir = wrf_output + '/kelani-upper-basin/'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_file_path = output_dir + '/mean-rf-' + date.strftime('%Y-%m-%d') + '.txt'
    output_file = open(output_file_path, 'w')

    for t in range(0, len(times) - 1):
        cnt = 0
        rf_sum = 0.0
        for y in range(0, len(kel_lats[:, 0])):
            for x in range(0, len(kel_lons[0, :])):
                if utils.is_inside_polygon(polys, kel_lats[y, x], kel_lons[y, x]):
                    cnt = cnt + 1
                    rf_sum = rf_sum + diff[t, y, x]
        output_file.write('%s %f\n' % (times[t], rf_sum / cnt))

    output_file.close()

    nc_fid.close()


def extract_kelani_upper_basin_mean_rainfall_sat(sat_dir, date, kelani_basin_shp_file, wrf_output):
    kel_lon_min = 79.994117
    kel_lat_min = 6.754167
    kel_lon_max = 80.773182
    kel_lat_max = 7.229167

    y = date.strftime('%Y')
    m = date.strftime('%m')
    d = date.strftime('%d')

    output_dir = wrf_output + '/kelani-upper-basin/sat'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_file_path = output_dir + '/mean-rf-sat-' + date.strftime('%Y-%m-%d') + '.csv'
    output_file = open(output_file_path, 'w')

    polys = shapefile.Reader(kelani_basin_shp_file)

    for h in range(0, 24):
        cnt = 0
        rf_sum = 0.0

        sh = str(h).zfill(2)
        sat_zip_file = '%s/%s/%s/%s/gsmap_nrt.%s%s%s.%s00.05_AsiaSS.csv.zip' % (sat_dir, y, m, d, y, m, d, sh)

        sat_zip = zipfile.ZipFile(sat_zip_file)
        sat = np.genfromtxt(sat_zip.open('gsmap_nrt.%s%s%s.%s00.05_AsiaSS.csv' % (y, m, d, sh)), delimiter=',',
                            names=True)
        sat_filt = sat[(sat['Lat'] <= kel_lat_max) & (sat['Lat'] >= kel_lat_min) & (sat['Lon'] <= kel_lon_max) & (
            sat['Lon'] >= kel_lon_min)]

        for p in sat_filt:
            if utils.is_inside_polygon(polys, p[0], p[1]):
                cnt = cnt + 1
                rf_sum = rf_sum + p[2]

        output_file.write('%s-%s-%s_%s:00:00 %f\n' % (y, m, d, sh, rf_sum / cnt))

    output_file.close()


def add_buffer_to_kelani_upper_basin_mean_rainfall(date, wrf_output):
    cells = 9433

    content = []
    first_line = ''
    for i in range(3, -1, -1):
        file_name = wrf_output + '/kelani-basin/created-' + (date - dt.timedelta(days=i)).strftime(
            '%Y-%m-%d') + '/RAINCELL.DAT'

        if os.path.exists(file_name):
            with open(file_name) as myfile:
                first_line = next(myfile)
                if i != 0:
                    head = [next(myfile) for x in xrange(cells * 24)]
                else:
                    head = [line for line in myfile]
            content.extend(head)
        else:
            head = ['%d 0.0\n' % (x % cells + 1) for x in xrange(cells * 24 + 1)]
            content.extend(head)

    out_dir = wrf_output + '/kelani-basin/new-created-' + date.strftime('%Y-%m-%d')
    out_name = out_dir + '/RAINCELL.DAT'

    first_line = first_line.split()
    first_line[1] = str(int(first_line[1]) + 24 * 3)
    first_line[2] = (date - dt.timedelta(days=3)).strftime('%Y-%m-%d')

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    out_file = open(out_name, 'w')
    out_file.write(' '.join(first_line) + '\n')
    for line in content:
        out_file.write(line)
    out_file.close()


def concat_rainfall_files(date, wrf_output, weather_stations):
    with open(weather_stations, 'rb') as stations_file:
        rf_dir = wrf_output + '/RF'
        for station_name in stations_file:
            station_name = station_name.split()[0]
            if not os.path.exists(rf_dir):
                os.makedirs(rf_dir)

            out_file_path = rf_dir + '/' + station_name + '.csv'
            if not os.path.exists(out_file_path):
                with open(out_file_path, 'w') as out_file:
                    out_file.write("Timestamp, Value, Time, ValID\n")

            with open(out_file_path, 'a') as out_file:
                rf_file = rf_dir + '/' + station_name + '-' + date.strftime('%Y-%m-%d') + '.txt'
                with open(rf_file) as rf_file:
                    # next(rf_file)
                    i = 0
                    for line in rf_file:
                        ts = line.split()[0]
                        val = line.split()[1]
                        ref = int(dt.datetime.strptime('2017-04-01', '%Y-%m-%d').strftime('%s')) / 3600
                        epoch = int(dt.datetime.strptime(ts, '%Y-%m-%d_%H:%M:%S').strftime('%s')) / 3600 - ref
                        # epoch = dt.datetime.strptime(ts, '%Y-%m-%d_%H:%M:%S').strftime('%s')
                        val_id = station_name[0:5] + date.strftime('%y%m%d-') + str(i / 24)
                        out_file.write('%s, %s, %s, %s\n' % (ts, val, epoch, val_id))
                        i += 1


def concat_rainfall_files_1(date, wrf_output, weather_stations):
    with open(weather_stations, 'rb') as stations_file:
        rf_dir = wrf_output + '/RF'
        for station_name in stations_file:
            station_name = station_name.split()[0]
            if not os.path.exists(rf_dir):
                os.makedirs(rf_dir)

            df = None
            out_file_path = rf_dir + '/' + station_name + '-merged.csv'
            if os.path.exists(out_file_path):
                df = pd.read_csv(out_file_path)

            rf_file = rf_dir + '/' + station_name + '-' + date.strftime('%Y-%m-%d') + '.txt'
            rf_df = pd.read_csv(rf_file, header=None, delim_whitespace=True)
            rf_df.columns = ['time', 'f' + date.strftime('%y%m%d')]
            rf_df['time'] = rf_df['time'].apply(
                lambda x: int(dt.datetime.strptime(x, '%Y-%m-%d_%H:%M:%S').strftime('%s')))

            if df is not None:
                df_out = pd.merge(df, rf_df, on='time', how='outer')
                df_out.to_csv(out_file_path, index=False)
            else:
                rf_df.to_csv(out_file_path, index=False)


def extract_point_rf_series(nc_f, lat, lon):
    nc_fid = Dataset(nc_f, 'r')

    times_len, times = extract_time_data(nc_f)
    lats = nc_fid.variables['XLAT'][0, :, 0]
    lons = nc_fid.variables['XLONG'][0, 0, :]

    lat_start_idx = np.argmin(abs(lats - lat))
    lon_start_idx = np.argmin(abs(lons - lon))

    prcp = nc_fid.variables['RAINC'][:, lat_start_idx, lon_start_idx] + \
           nc_fid.variables['RAINNC'][:, lat_start_idx, lon_start_idx] + \
           nc_fid.variables['SNOWNC'][:, lat_start_idx, lon_start_idx] + \
           nc_fid.variables['GRAUPELNC'][:, lat_start_idx, lon_start_idx]

    diff = prcp[1:times_len] - prcp[0:times_len - 1]

    nc_fid.close()

    return diff, np.array(times[0:times_len - 1])


def extract_area_rf_series(nc_f, lat_min, lat_max, lon_min, lon_max):
    if not os.path.exists(nc_f):
        raise IOError('File %s not found' % nc_f)

    nc_fid = Dataset(nc_f, 'r')

    times_len, times = extract_time_data(nc_f)
    lats = nc_fid.variables['XLAT'][0, :, 0]
    lons = nc_fid.variables['XLONG'][0, 0, :]

    lon_min_idx = np.argmax(lons >= lon_min) - 1
    lat_min_idx = np.argmax(lats >= lat_min) - 1
    lon_max_idx = np.argmax(lons >= lon_max)
    lat_max_idx = np.argmax(lats >= lat_max)

    prcp = nc_fid.variables['RAINC'][:, lat_min_idx:lat_max_idx, lon_min_idx:lon_max_idx] + \
           nc_fid.variables['RAINNC'][:, lat_min_idx:lat_max_idx, lon_min_idx:lon_max_idx] + \
           nc_fid.variables['SNOWNC'][:, lat_min_idx:lat_max_idx, lon_min_idx:lon_max_idx] + \
           nc_fid.variables['GRAUPELNC'][:, lat_min_idx:lat_max_idx, lon_min_idx:lon_max_idx]

    diff = prcp[1:times_len] - prcp[0:times_len - 1]

    nc_fid.close()

    return diff, lats[lat_min_idx:lat_max_idx], lons[lon_min_idx:lon_max_idx], np.array(times[0:times_len - 1])


def extract_jaxa_weather_stations(nc_f, weather_stations_file, output_dir):
    nc_fid = Dataset(nc_f, 'r')

    stations = pd.read_csv(weather_stations_file, header=0, sep=',')

    output_file_dir = os.path.join(output_dir, 'jaxa-stations-wrf-forecast')
    utils.create_dir_if_not_exists(output_file_dir)

    for idx, station in stations.iterrows():
        logging.info('Extracting station ' + str(station))

        rf, times = extract_point_rf_series(nc_f, station[2], station[1])

        output_file_path = os.path.join(output_file_dir,
                                        station[3] + '-' + str(station[0]) + '-' + times[0].split('_')[0] + '.txt')
        output_file = open(output_file_path, 'w')
        output_file.write('jaxa-stations-wrf-forecast\n')
        output_file.write(', '.join(stations.columns.values) + '\n')
        output_file.write(', '.join(str(x) for x in station) + '\n')
        output_file.write('timestamp, rainfall\n')
        for i in range(len(times)):
            output_file.write('%s, %f\n' % (times[i], rf[i]))
        output_file.close()

    nc_fid.close()


def extract_jaxa_satellite_data(start_ts_utc, end_ts_utc, output_dir):
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

    utils.download_parallel(url_dest_list)

    procs = multiprocessing.cpu_count()
    Parallel(n_jobs=procs)(
        delayed(process_zip_file)(i[1], i[2], lat_min, lon_min, lat_max, lon_max) for i in url_dest_list)

    # clean up temp dir
    shutil.rmtree(tmp_dir)


def process_zip_file(zip_file_path, out_file_path, lat_min, lon_min, lat_max, lon_max):
    sat_zip = zipfile.ZipFile(zip_file_path)
    sat = np.genfromtxt(sat_zip.open(os.path.basename(zip_file_path).replace('.zip', '')), delimiter=',', names=True)
    sat_filt = sat[
        (sat['Lat'] <= lat_max) & (sat['Lat'] >= lat_min) & (sat['Lon'] <= lon_max) & (sat['Lon'] >= lon_min)]
    lats = np.sort(np.unique(sat_filt['Lat']))
    lons = np.sort(np.unique(sat_filt['Lon']))

    cell_size = 0.1
    no_data_val = -99
    out_file = open(out_file_path, 'w')
    out_file.write('NCOLS %d\n' % len(lons))
    out_file.write('NROWS %d\n' % len(lats))
    out_file.write('XLLCORNER %f\n' % lons[0])
    out_file.write('YLLCORNER %f\n' % lats[0])
    out_file.write('CELLSIZE %f\n' % cell_size)
    out_file.write('NODATA_VALUE %d\n' % no_data_val)

    for lat in np.flip(lats, 0):
        for lon in lons:
            out_file.write(str(sat[(sat['Lat'] == lat) & (sat['Lon'] == lon)][0][2]) + ' ')
        out_file.write('\n')

    out_file.close()

    data = np.sort(sat_filt, order=['Lat', 'Lon'])['RainRate'].reshape(len(lats), len(lons))
    clevs = np.concatenate(([-1, 0], np.array([pow(2, i) for i in range(0, 9)])))
    create_contour_plot(data, out_file_path + '.png', lat_min, lon_min, lat_max, lon_max, out_file_path, clevs=clevs,
                        cmap=cm.s3pcpn_l)


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


def extract_all(wrf_home, start_date, end_date):
    logging.info('Extracting data from %s to %s' % (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    logging.info('WRF home : %s' % wrf_home)

    weather_st_file = res_mgr.get_resource_path('extraction/local/kelani_basin_stations.txt')
    kelani_basin_file = res_mgr.get_resource_path('extraction/local/kelani_basin_points.txt')
    kelani_basin_shp_file = res_mgr.get_resource_path('extraction/shp/kelani-upper-basin.shp')
    jaxa_weather_st_file = res_mgr.get_resource_path('extraction/local/jaxa_weather_stations.txt')

    dates = np.arange(start_date, end_date, dt.timedelta(days=1)).astype(dt.datetime)

    for date in dates:
        wrf_output = utils.get_output_dir(wrf_home)

        nc_f = wrf_output + '/wrfout_d03_' + date.strftime('%Y-%m-%d') + '_00:00:00'
        if not os.path.exists(nc_f):
            raise IOError('File %s not found' % nc_f)

        logging.info('Extracting time data')
        times_len, times = extract_time_data(nc_f)

        logging.info('Extract rainfall data for the metro colombo area')
        basin_rf = extract_metro_colombo(nc_f, date, wrf_output)
        logging.info('Basin rainfall' + str(basin_rf))

        logging.info('Extract weather station rainfall')
        extract_weather_stations(nc_f, date, times, weather_st_file, wrf_output)

        logging.info('Extract Kelani Basin rainfall')
        extract_kelani_basin_rainfall(nc_f, date, kelani_basin_file, wrf_output, basin_rf)

        logging.info('Extract Kelani upper Basin mean rainfall')
        extract_kelani_upper_basin_mean_rainfall(nc_f, date, times, kelani_basin_shp_file, wrf_output)

        logging.info('Extract Jaxa stations wrf rainfall')
        extract_jaxa_weather_stations(nc_f, jaxa_weather_st_file, wrf_output)

        logging.info('Exctract Jaxa sattellite rainfall data')
        # extract_jaxa_satellite_data(date, wrf_output)

        # logging.info('adding buffer to the RAINCELL.DAT file')
        # add_buffer_to_kelani_upper_basin_mean_rainfall(date, wrf_output)

        # logging.info('Concat the RF of the weather stations 1')
        # concat_rainfall_files(date, wrf_output, weather_st_file)

        # logging.info('Concat the RF of the weather stations 2')
        # concat_rainfall_files_1(date, wrf_output, weather_st_file)

        # print "##########################"
        # print "Analyze the Sat Images"
        # sat_data_dir = '/home/nira/Desktop/2016-event/05_AsiaSS'
        # extract_kelani_upper_basin_mean_rainfall_sat(sat_data_dir, date, kelani_basin_shp_file, wrf_output)


class TestExtractorMethods(unittest.TestCase):
    def test_extract_jaxa_satellite_data(self):
        extract_jaxa_satellite_data(utils.datetime_lk_to_utc(dt.datetime(2017, 5, 25)),
                                    utils.datetime_lk_to_utc(dt.datetime(2017, 5, 28)), '/tmp/rf')

        # def test_create_contour_plot(self):
        #     lat_min = 5.722969
        #     lon_min = 79.52146
        #     lat_max = 10.06425
        #     lon_max = 82.18992
        #
        #     tmp_dir = '/tmp/rf/tmp_jaxa'
        #     if not os.path.exists(tmp_dir):
        #         os.makedirs(tmp_dir)
        #
        #     url = 'ftp://rainmap:Niskur+1404@hokusai.eorc.jaxa.jp/realtime/txt/05_AsiaSS/2017/05/26/' \
        #           'gsmap_nrt.20170526.0600.05_AsiaSS.csv.zip'
        #     dest = os.path.join(tmp_dir, os.path.basename(url))
        #
        #     if not os.path.exists(dest):
        #         utils.download_file(url, dest)
        #
        #     sat_zip = zipfile.ZipFile(dest)
        #     sat = np.genfromtxt(sat_zip.open(os.path.basename(dest).replace('.zip', '')), delimiter=',', names=True)
        #     sat_filt = np.sort(
        #         sat[(sat['Lat'] <= lat_max) & (sat['Lat'] >= lat_min) & (sat['Lon'] <= lon_max) & (sat['Lon'] >= lon_min)],
        #         order=['Lat', 'Lon'])
        #     lats = len(np.unique(sat_filt['Lat']))
        #     lons = len(np.unique(sat_filt['Lon']))
        #     # data = np.flip(sat_filt['RainRate'].reshape(lats, lons), 0)
        #     data = sat_filt['RainRate'].reshape(lats, lons)
        #
        #     create_contour_plot(data, os.path.join(tmp_dir, 'output.png'), lat_min, lon_min, lat_max, lon_max, 'Rain rate')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    suite = unittest.TestLoader().loadTestsFromTestCase(TestExtractorMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
