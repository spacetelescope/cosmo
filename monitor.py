"""
Perform regular monitoring of the COS FUV and NUV dark rates

"""
import os
import sqlite3
import logging
import numpy as np

from astropy.io import fits

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
        sun_lat = timeline['sun_lon'][:-1][::step].copy().astype(np.float64)
        sun_lon = timeline['sun_lat'][:-1][::step].copy().astype(np.float64)
    except KeyError:
        sun_lat = lat.copy() * 0
        sun_lon = lat.copy() * 0

    
    mjd = hdu[1].header['EXPSTART']  + \
        times.copy()[:-1].astype( np.float64 ) * \
        SECOND_PER_MJD 
        
    if not len( lat ) > 2:
        raise ValueError( 'Too few events' )
    if not len( times ) > 2: 
        raise ValueError( 'Too few events' )

    counts = np.histogram( hdu['events'].data['time'], bins=times )[0]

    if not len( lat ) == len(counts):
        lat = lat[:-1]
        lon = lon[:-1]
        sun_lat = sun_lat[:-1]
        sun_lon = sun_lon[:-1]
    

    assert len(lat) == len(counts), \
        'Arrays are not equal in length {}:{}'.format( len(lat), len(counts) )


    return counts, mjd, lat, lon, sun_lat, sun_lon

#-------------------------------------------------------------------------------

def compile_darkrates(detector='FUV'):
    db = sqlite3.connect("/grp/hst/cos/Monitors/DB/cos_darkrates.db")

    c = db.cursor()
    table = '{}_stats'.format(detector)
    try:
        c.execute("""CREATE TABLE {} ( obsname text,  mjd real, dark real, latitude real, longitude real, sun_lat real, sun_lon real)""".format(table))
    except sqlite3.OperationalError:
        pass

    location = '/grp/hst/cos/Monitors/Darks/{}/'.format( detector )
    for filename in pull_darks(location, detector):
        print filename
        obsname = os.path.split( filename )[-1]
        c.execute( """SELECT * FROM %s WHERE obsname='%s'""" %(table, obsname))

        # Check if the obsname is already present
        entries = [item for item in c]
        if len( entries ): 
            print 'Already checked'
            continue
    
        counts, mjd, lat, lon, sun_lat, sun_lon = pull_orbital_info( filename, 25 )
 
        for i in range(len(mjd)):
            c.execute( """INSERT INTO %s VALUES (?,?,?,?,?,?,?)""" % (table),
                       (obsname,
                        mjd[i],
                        counts[i],
                        lat[i],
                        lon[i],
                        sun_lat[i],
                        sun_lon[i]))

        db.commit()

#-------------------------------------------------------------------------------

if __name__ == '__main__':
    compile_darkrates('FUV')
