"""Perform regular monitoring of the COS FUV and NUV dark rates

"""

from __future__ import print_function, absolute_import, division

import os
import datetime
import numpy as np
import shutil
import glob

from astropy.io import fits
from astropy.time import Time

from .solar import get_solar_data
from .plotting import plot_histogram, plot_time, plot_orbital_rate

from ..utils import corrtag_image
from ..database.db_tables import open_settings, load_connection
from sqlalchemy.sql.functions import concat

base_dir = '/grp/hst/cos/Monitors/Darks/'
web_directory = '/grp/webpages/COS/'

#-------------------------------------------------------------------------------

def get_temp(filename):
    """Get detector temperture during observation from spt filename

    Parameters
    ----------
    filename : str
        FITS file for which the temperature is to be Found

    Returns
    -------
    temperature : float
        Detector temperature at the time of the observation

    """

    with fits.open(filename) as hdu:
        detector = hdu[0].header['DETECTOR']
        segment = hdu[0].header['SEGMENT']
        rootname = hdu[0].header['ROOTNAME']

    if detector == 'FUV' and segment == 'FUVA':
        temp_keyword = 'LDCAMPAT'
    elif detector == 'FUV' and segment == 'FUVB':
        temp_keyword = 'LDCAMPBT'
    elif detector == 'NUV':
        temp_keyword = 'LMMCETMP'
    else:
        raise ValueError('What??? {} {}'.format(detector, segment))

    path, name = os.path.split(filename)
    spt_file = os.path.join(path, rootname + '_spt.fits')

    try:
        temperature = fits.getval(spt_file, temp_keyword, ext=2)
    except IOError:
        temperature = fits.getval(spt_file + '.gz', temp_keyword, ext=2)

    return temperature

#-------------------------------------------------------------------------------

def mjd_to_decyear( time_array ):
    """ pull this out when you get it into astropy.time

    """

    times = Time( time_array, scale='tt', format='mjd' )

    out_times = []
    for value in times:
        year = value.datetime.year
        n_days = (value.datetime - datetime.datetime(value.datetime.year, 1, 1)).total_seconds()
        total_days = (datetime.datetime(value.datetime.year+1, 1, 1) - datetime.datetime(value.datetime.year, 1, 1)).total_seconds()

        fraction = float(n_days) / total_days

        out_times.append(year + fraction)

    return np.array(out_times)

#-------------------------------------------------------------------------------

def pull_orbital_info(dataset, step=25):
    """ Pull second by second orbital information from the dataset

    """

    SECOND_PER_MJD = 1.15741e-5

    info = {}
    info['obsname'] = os.path.split(dataset)[-1]

    hdu = fits.open(dataset)
    try:
        timeline = hdu['timeline'].data
        segment = hdu[0].header['segment']
        print(timeline)
    except KeyError:
        yield info
        raise StopIteration

    if segment == 'N/A':
        segment = 'NUV'
        xlim = (0, 1024)
        ylim = (0, 1204)
        pha = (-1, 1)
    elif segment == 'FUVA':
        xlim = (1200, 15099)
        ylim = (380, 680)
        pha = (2, 23)
    elif segment == 'FUVB':
        xlim = (950, 15049)
        ylim = (440, 720)
        pha = (2, 23)
    else:
        raise ValueError('What segment is this? {}'.format(segment))

    info['rootname'] = hdu[0].header['rootname']
    info['detector'] = segment
    info['temp'] = get_temp(dataset)

    times = timeline['time'][::step].copy()
    lat = timeline['latitude'][:-1][::step].copy().astype(np.float64)
    lon = timeline['longitude'][:-1][::step].copy().astype(np.float64)
    try:
        sun_lat = timeline['sun_lat'][:-1][::step].copy().astype(np.float64)
        sun_lon = timeline['sun_lon'][:-1][::step].copy().astype(np.float64)
    except KeyError:
        sun_lat = lat.copy() * 0
        sun_lon = lat.copy() * 0


    mjd = hdu[1].header['EXPSTART'] + \
        times.copy()[:-1].astype(np.float64) * \
        SECOND_PER_MJD

    decyear = mjd_to_decyear(mjd)

    if not len(times):
        blank = np.array([0])
        yield info
        raise StopIteration

    events = hdu['events'].data
    filtered_index = np.where((events['PHA'] > pha[0]) &
                              (events['PHA'] < pha[1]) &
                              (events['XCORR'] > xlim[0]) &
                              (events['XCORR'] < xlim[1]) &
                              (events['YCORR'] > ylim[0]) &
                              (events['YCORR'] < ylim[1]))

    ta_index = np.where((events['XCORR'] > xlim[0]) &
                        (events['XCORR'] < xlim[1]) &
                        (events['YCORR'] > ylim[0]) &
                        (events['YCORR'] < ylim[1]))


    counts = np.histogram(events[filtered_index]['time'], bins=times)[0]
    ta_counts = np.histogram(events[ta_index]['time'], bins=times)[0]

    npix = float((xlim[1] - xlim[0]) * (ylim[1] - ylim[0]))
    counts = counts / npix / step
    ta_counts = ta_counts / npix / step

    if not len(lat) == len(counts):
        lat = lat[:-1]
        lon = lon[:-1]
        sun_lat = sun_lat[:-1]
        sun_lon = sun_lon[:-1]

    assert len(lat) == len(counts), \
        'Arrays are not equal in length {}:{}'.format(len(lat), len(counts))

    if not len(counts):
        yield info
    else:
        for i in range(len(counts)):
            ### - better solution than round?
            info['date'] = round(decyear[i], 3)
            info['dark'] = round(counts[i], 7)
            info['ta_dark'] = round(ta_counts[i], 7)
            info['latitude'] = round(lat[i], 7)
            info['longitude'] = round(lon[i], 7)
            info['sun_lat'] = round(sun_lat[i], 7)
            info['sun_lon'] = round(sun_lon[i], 7)

            yield info

#-------------------------------------------------------------------------------

def compile_phd():

    raise NotImplementedError("Nope, seriously can't do any of this.")

    #-- populate PHD table

    columns = ', '.join(['bin{} real'.format(pha) for pha in range(0,31)])
    c.execute("""CREATE TABLE {} ( obsname text, {})""".format(table, columns ))


    c.execute( """SELECT obsname FROM %s """ %(table))
    already_done = set( [str(item[0]) for item in c] )

    for filename in available:
        obsname = os.path.split(filename)[-1]
        if obsname in already_done:
            print(filename, 'done')
        else:
            print(filename, 'running')

        counts = pha_hist(filename)
        table_values = (obsname, ) + tuple(list(counts) )

        c.execute( """INSERT INTO %s VALUES (?{})""" % (table, ',?'*31 ),
                   table_values)

        db.commit()

#-------------------------------------------------------------------------------

def pha_hist(filename):
    hdu = fits.open( filename )
    pha_list_all = hdu[1].data['PHA']
    counts, bins = np.histogram(pha_list_all, bins=31, range=(0, 31))

    return counts

#-------------------------------------------------------------------------------

def make_plots(detector, TA=False):
    print('#-------------------#')
    print('Making plots for {}'.format(detector))
    print('#-------------------#')

    if detector == 'FUV':
        search_strings = ['_corrtag_a.fits', '_corrtag_b.fits']
        segments = ['FUVA', 'FUVB']
    elif detector == 'NUV':
        search_strings = ['_corrtag.fits']
        segments = ['NUV']
    else:
        raise ValueError('Only FUV or NUV allowed.  NOT:{}'.format(detector) )

    try:
        solar_data = np.genfromtxt(base_dir + 'solar_flux.txt', dtype=None)
        solar_date = np.array( mjd_to_decyear([line[0] for line in solar_data]) )
        solar_flux = np.array([line[1] for line in solar_data])
    except TypeError:
        solar_date = np.ones(1000)
        solar_flux = np.ones(1000)

    dark_key = 'dark'
    if TA:
        dark_key = 'ta_dark'

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    for key, segment in zip(search_strings, segments):
        #-- Plot vs time
        print('Plotting Time')
        data = engine.execute("""SELECT date,{},temp,latitude,longitude
                                 FROM darks
                                 WHERE detector = '{}'
                                 AND concat(temp, latitude, longitude)IS NOT NULL""".format(dark_key, segment)
                                 )
        data = [row for row in data]

        mjd = np.array([item.date for item in data])
        dark = np.array([item[1] for item in data])
        temp = np.array([item.temp for item in data])
        latitude = np.array([item.latitude for item in data])
        longitude = np.array([item.longitude for item in data])

        index = np.argsort(mjd)
        mjd = mjd[index]
        dark = dark[index]
        temp = temp[index]
        latitude = latitude[index]
        longitude = longitude[index]

        index_keep = np.where((longitude < 250) | (latitude > 10))[0]
        mjd = mjd[index_keep]
        dark = dark[index_keep]
        temp = temp[index_keep]

        outname = os.path.join(base_dir, detector, '{}_vs_time_{}.png'.format(dark_key, segment))
        plot_time(detector, dark, mjd, temp, solar_flux, solar_date, outname)

        #-- Plot vs orbit
        print('Plotting Orbit')
        data = engine.execute("""SELECT {},latitude,longitude,sun_lat,sun_lon,date
                                 FROM darks
                                 WHERE detector = '{}'
                                 AND concat(latitude,longitude,sun_lat,sun_lon)IS NOT NULL""".format(dark_key, segment)
                                 )
        data = [row for row in data]

        dark = np.array([item[0] for item in data])
        latitude = np.array([item[1] for item in data])
        longitude = np.array([item[2] for item in data])
        sun_lat = np.array([item[3] for item in data])
        sun_lon = np.array([item[4] for item in data])
        date = np.array([item[5] for item in data])

        index = np.argsort(date)
        dark = dark[index]
        latitude = latitude[index]
        longitude = longitude[index]
        sun_lat = sun_lat[index]
        sun_lon = sun_lon[index]

        outname = os.path.join(base_dir, detector, '{}_vs_orbit_{}.png'.format(dark_key, segment))
        plot_orbital_rate(longitude, latitude, dark, sun_lon, sun_lat, outname)

        #-- Plot histogram of darkrates
        print('Plotting Hist')
        data = engine.execute("""SELECT {},date
                                 FROM darks
                                 WHERE detector = '{}'
                                 AND concat(date, detector)IS NOT NULL""".format(dark_key, segment)
                                 )
        data = [item for item in data]

        dark = np.array([item[0] for item in data])
        date = np.array([item[1] for item in data])

        index = np.argsort(date)
        date = date[index]
        dark = dark[index]

        for year in set(map(int, date)):
            index = np.where( (date >= year) &
                              (date < year + 1))

            outname = os.path.join(base_dir, detector, '{}_hist_{}_{}.pdf'.format(dark_key, year, segment))
            plot_histogram(dark[index], outname)

        index = np.where(date >= date.max() - .5)
        outname = os.path.join(base_dir, detector, '{}_hist_-6mo_{}.pdf'.format(dark_key, segment))
        plot_histogram(dark[index], outname )

        outname = os.path.join(base_dir, detector, '{}_hist_{}.pdf'.format(dark_key, segment))
        plot_histogram(dark, outname)

#-------------------------------------------------------------------------------

def move_products():
    '''Move created pdf files to webpage directory
    '''

    print('#-------------------#')
    print('Moving products into')
    print('webpage directory')
    print('#-------------------#')

    for detector in ['FUV', 'NUV']:

        write_dir = web_directory + detector.lower() + '_darks/'
        move_list = glob.glob(base_dir + detector + '/*.p??')

        for item in move_list:
            os.chmod(item, 0o777)
            path, file_to_move = os.path.split(item)
            shutil.copy(item, write_dir + file_to_move)
            print('Moving: %s' % (file_to_move))

        os.system('chmod 777 ' + write_dir + '*.pdf')

#-------------------------------------------------------------------------------

def monitor():
    """Main monitoring pipeline"""

    get_solar_data('/grp/hst/cos/Monitors/Darks/')

    for detector in ['FUV', 'NUV']:
        make_plots(detector)

        if detector == 'FUV':
            make_plots(detector, TA=True)

    move_products()

#-------------------------------------------------------------------------------
