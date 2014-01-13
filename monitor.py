"""
Perform regular monitoring of the COS FUV and NUV dark rates

"""
import os
import sqlite3
import logging
import numpy as np
import scipy
import matplotlib.pyplot as plt

from astropy.io import fits
from astropy.time import Time
import datetime

from solar import get_solar_data
import plotting

base_dir = '/grp/hst/cos/Monitors/Darks/'

#-------------------------------------------------------------------------------

def pull_darks(base, detector):
    '''
    Recursively find all darks in the given base directory taken with the 
    given detector

    '''

    for root, dirs, files in os.walk(base):
        for filename in files:
            full_filename = os.path.join( root, filename )
            if not '_corrtag' in filename:
                continue
            if not '.fits' in filename: 
                continue
            if not fits.getval(full_filename, 'detector', ext=0) == detector: 
                continue
            if not fits.getval(full_filename, 'exptype', ext=0) == 'DARK': 
                continue
            
            yield full_filename

#-------------------------------------------------------------------------------

def get_temp( filename ):

    detector = fits.getval( filename, 'DETECTOR' )
    segment = fits.getval( filename, 'SEGMENT' )

    if detector == 'FUV' and segment == 'FUVA':
        temp_keyword = 'LDCAMPAT'
    elif detector == 'FUV' and segment == 'FUVB':
        temp_keyword = 'LDCAMPBT'
    elif detector == 'NUV':
        temp_keyword = 'LMMCETMP'
    else:
        raise ValueError('What??? {} {}'.format(detector, segment))

    path, name = os.path.split(filename)
    rootname = name[:9]
    spt_file = os.path.join(path, rootname + '_spt.fits')

    temperature = fits.getval(spt_file, temp_keyword, ext=2)

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

        out_times.append( year + fraction )

    return np.array(out_times)

#-------------------------------------------------------------------------------

def pull_orbital_info( dataset, step=1 ):
    """ Pull second by second orbital information from the dataset

    """

    SECOND_PER_MJD = 1.15741e-5

    hdu = fits.open( dataset )
    timeline = hdu['timeline'].data

    times = timeline['time'][::step].copy()
    lat = timeline['latitude'][:-1][::step].copy().astype(np.float64)
    lon = timeline['longitude'][:-1][::step].copy().astype(np.float64)
    try:
        sun_lat = timeline['sun_lat'][:-1][::step].copy().astype(np.float64)
        sun_lon = timeline['sun_lon'][:-1][::step].copy().astype(np.float64)
    except KeyError:
        sun_lat = lat.copy() * 0
        sun_lon = lat.copy() * 0

    
    mjd = hdu[1].header['EXPSTART']  + \
        times.copy()[:-1].astype( np.float64 ) * \
        SECOND_PER_MJD 

    decyear = mjd_to_decyear( mjd )

    if not len( times ):
        blank = np.array( [0] )
        return blank, blank, blank, blank, blank, blank

    counts = np.histogram( hdu['events'].data['time'], bins=times )[0]

    if not len( lat ) == len(counts):
        lat = lat[:-1]
        lon = lon[:-1]
        sun_lat = sun_lat[:-1]
        sun_lon = sun_lon[:-1]
    

    assert len(lat) == len(counts), \
        'Arrays are not equal in length {}:{}'.format( len(lat), len(counts) )


    return counts, decyear, lat, lon, sun_lat, sun_lon

#-------------------------------------------------------------------------------

def compile_darkrates(detector='FUV'):
    db = sqlite3.connect("/grp/hst/cos/Monitors/DB/cos_darkrates.db")

    c = db.cursor()
    table = '{}_stats'.format(detector)
    try:
        c.execute("""CREATE TABLE {} ( obsname text, date real, dark real, latitude real, longitude real, sun_lat real, sun_lon real, temp real)""".format(table))
    except sqlite3.OperationalError:
        pass

    location = '/grp/hst/cos/Monitors/Darks/{}/'.format( detector )
    c.execute( """SELECT obsname FROM %s """ %(table))
    already_done = set( [str(item[0]) for item in c] )
    
    for filename in pull_darks(location, detector):
        print filename
        obsname = os.path.split( filename )[-1]

        if obsname in already_done: 
            continue
    
        counts, date, lat, lon, sun_lat, sun_lon = pull_orbital_info( filename, 25 )
        temp = get_temp(filename)
 
        for i in range(len(counts)):
            c.execute( """INSERT INTO %s VALUES (?,?,?,?,?,?,?,?)""" % (table),
                       (obsname,
                        date[i],
                        counts[i],
                        lat[i],
                        lon[i],
                        sun_lat[i],
                        sun_lon[i],
                        temp))

        db.commit()

#-------------------------------------------------------------------------------

def make_plots( detector ):

    plt.ioff()
    db = sqlite3.connect("/grp/hst/cos/Monitors/DB/cos_darkrates.db")

    cursor = db.cursor()
    table = '{}_stats'.format(detector)

    if detector == 'FUV':
        search_strings = ['_corrtag_a.fits', '_corrtag_b.fits']
        segments = ['FUVA', 'FUVB']
    elif detector == 'NUV':
        search_strings = ['_corrtag.fits']
        segments = ['NUV']
    else:
        raise ValueError('Only FUV or NUV allowed.  NOT:{}'.format(detector) )

    solar_data = np.genfromtxt(base_dir + 'solar_flux.txt', dtype=None)
    solar_date = np.array( mjd_to_decyear([line[0] for line in solar_data]) )
    solar_flux = np.array([line[1] for line in solar_data])

    for key, segment in zip(search_strings, segments):
        #-- Plot vs time
        print 'Plotting Time'
        cursor.execute( """SELECT date,dark,temp FROM {} WHERE obsname LIKE '%{}%'""".format(table, key))

        data = [ item for item in cursor ]
        mjd = np.array( [item[0] for item in data] )
        dark = np.array( [item[1] for item in data] )
        temp = np.array( [item[2] for item in data] )
        
        index = np.argsort(mjd)
        mjd = mjd[index]
        dark = dark[index]
        temp = temp[index]

        outname = os.path.join(base_dir, detector, 'dark_vs_time_{}.png'.format(segment) )
        plotting.plot_time( detector, dark, mjd, temp, solar_flux, solar_date, outname )
        
        #-- Plot vs orbit
        print 'Plotting Orbit'
        cursor.execute( """SELECT dark,latitude,longitude,sun_lat,sun_lon,date FROM {} WHERE obsname LIKE '%{}%'""".format(table, key))
        data = [ item for item in cursor ]
        dark = np.array( [item[0] for item in data] )
        latitude = np.array( [item[1] for item in data] )
        longitude = np.array( [item[2] for item in data] )
        sun_lat = np.array( [item[3] for item in data] )
        sun_lon = np.array( [item[4] for item in data] )
        date = np.array( [item[5] for item in data] )

        index = np.argsort(date)
        dark = dark[index]
        latitude = latitude[index]
        longitude = longitude[index]
        sun_lat = sun_lat[index]
        sun_lon = sun_lon[index]
        
        outname = os.path.join(base_dir, detector, 'dark_vs_orbit_{}.png'.format(segment) )
        plotting.plot_orbital_rate(longitude, latitude, dark, sun_lon, sun_lat, outname )

#-------------------------------------------------------------------------------

def monitor():
    """ main monitoring pipeline"""

    get_solar_data( '/grp/hst/cos/Monitors/Darks/' )

    for detector in ['FUV', 'NUV']:
        compile_darkrates( detector )
        make_plots( detector )

#-------------------------------------------------------------------------------

if __name__ == '__main__':
    monitor()
