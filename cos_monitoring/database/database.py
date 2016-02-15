from __future__ import print_function, absolute_import, division

from astropy.io import fits
import os
from sqlalchemy import and_, or_, text
import sys
import numpy as np
import multiprocessing as mp
import types

from ..dark.monitor import monitor as dark_monitor
from ..dark.monitor import pull_orbital_info
from ..filesystem import find_all_datasets
from ..osm.monitor import pull_flashes
from ..stim.monitor import locate_stims
from ..stim.monitor import stim_monitor
from .db_tables import load_connection, open_settings
from .db_tables import Base
from .db_tables import Files, Headers
from .db_tables import Lampflash, Stims, Phd, Darks, sptkeys, Data

SETTINGS = open_settings()
Session, engine = load_connection(SETTINGS['connection_string'])

#-------------------------------------------------------------------------------

def mp_insert(args):
    """Wrapper function to parse arguments and pass into insertion function

    Parameters
    ----------
    args, tuple
        filename, table, function, (foreign key, optional)

    """

    if len(args) == 3:
        filename, table, function = args
        insert_with_yield(filename, table, function)
    elif len(args) == 4:
        filename, table, function, foreign_key = args
        insert_with_yield(filename, table, function, foreign_key)

#-------------------------------------------------------------------------------

def insert_with_yield(filename, table, function, foreign_key=None):
    """ Call function on filename and insert results into table

    Parameters
    ----------
    filename : str
        name of the file to call the function argument on
    table : sqlalchemy table object
        The table of the database to update.
    function : function
        The function to call, should be a generator
    foreign_key : int, optional
        foreign key to update the table with
    """

    Session, engine = load_connection(SETTINGS['connection_string'])
    session = Session()



    try:
        data = function(filename)

       if isinstance(data, dict):
           data = [data] 
       elif isinstance(data, types.GeneratorType)
           pass
       else:
           raise ValueError("Not designed to work with data of type {}".format(type(data)))

        #-- Pull data from generator and commit
        for i, row in enumerate(data):
            row['file_id'] = foreign_key
            if i == 0:
                print(row.keys())
            print(row.values())
            session.add(table(**row))

    except (IOError, ValueError) as e:
        #-- Handle missing files
        print(e.message)
        session.add(table(file_id=foreign_key))

        session.commit()
        session.close()
        engine.dispose()


    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def insert_files(**kwargs):
    """Populate the main table of all files in the base directory

    Directly populates the Files table with the full path to all existing files
    located recursively down through the file structure.

    Parameters
    ----------
    data_location : str, optional
        location of the data files to populate the table, defaults to './'

    """

    data_location = kwargs.get('data_location', './')
    print("Looking for new files in {}".format(data_location))

    session = Session()
    print("Querying previously found files")
    previous_files = {os.path.join(path, fname) for path, fname in session.query(Files.path, Files.name)}

    for i, (path, filename) in enumerate(find_all_datasets(data_location)):
        full_filepath = os.path.join(path, filename)

        if full_filepath in previous_files:
            print("Already found: {}".format(full_filepath))
            continue

        print("NEW: Found {}".format(full_filepath))

        #-- properly formatted HST data should be the first 9 characters
        #-- if this is not the case, insert NULL for this value
        rootname = filename.split('_')[0]
        if not len(rootname) == 9:
            rootname = None

        session.add(Files(path=path,
                          name=filename,
                          rootname=rootname))

        #-- Commit every 20 files to not lose too much progress if
        #-- a failure happens.
        if not i%20:
            session.commit()

    session.commit()
    session.close()

#-------------------------------------------------------------------------------

def populate_lampflash(num_cpu=1):
    """ Populate the lampflash table

    """
    print("Adding to lampflash")
    session = Session()

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(Files.name.like('%lampflash%')).\
                                outerjoin(Lampflash, Files.id == Lampflash.file_id).\
                                filter(Lampflash.file_id == None)]
    session.close()


    args = [(full_filename, Lampflash, pull_flashes, f_key) for f_key, full_filename in files_to_add]

    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------

def populate_stims(num_cpu=1):
    """ Populate the stim table

    """
    print("Adding to stims")
    session = Session()

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(Files.name.like('%corrtag\_%')).\
                                outerjoin(Stims, Files.id == Stims.file_id).\
                                filter(Stims.file_id == None)]
    session.close()


    args = [(full_filename, Stims, locate_stims, f_key) for f_key, full_filename in files_to_add]

    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------

def populate_darks(num_cpu=1):
    """ Populate the darks table

    """

    print("Adding to Darks")
    session = Session()

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                            outerjoin(Headers, Files.id == Headers.file_id).\
                            outerjoin(Darks, Files.id == Darks.file_id).\
                            filter(Headers.targname == 'DARK').\
                            filter(Darks.file_id == None)]

    session.close()

    args = [(full_filename, Darks, pull_orbital_info, f_key) for f_key, full_filename in files_to_add]

    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------

def populate_spt(num_cpu=1):
    """ Populate the table of primary header information

    """
    print("Adding SPT headers")
    session = Session()

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(Files.name.like('%\_spt.fits%')).\
                                outerjoin(sptkeys, Files.id == sptkeys.file_id)]

    args = [(full_filename, f_key) for f_key, full_filename in files_to_add]

    pool = mp.Pool(processes=num_cpu)
    pool.map(update_header,args)
#-------------------------------------------------------------------------------

def populate_primary_headers(num_cpu=1):
    """ Populate the table of primary header information

    """
    print("Adding to primary headers")

    session = Session()
    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(or_(Files.name.like('%corrtag%'), Files.name.like('%rawtag%'), Files.name.like('%\_x1d.fits'))).\
                                outerjoin(Headers, Files.id == Headers.file_id).\
                                filter(Headers.file_id == None)]

    print("Found {} files to add".format(len(files_to_add)))

    args = [(full_filename, f_key) for f_key, full_filename in files_to_add]
    pool = mp.Pool(processes=num_cpu)
    pool.map(update_header, args)

#-------------------------------------------------------------------------------

def update_header((args)):
    """update DB header table in parallel"""

    filename, f_key = args

    Session, engine = load_connection(SETTINGS['connection_string'])
    session = Session()

    if filename.endswith('spt.fits') or filename.endswith('spt.fits.gz'):
            try:
                with fits.open(filename) as hdu:
                    keywords = {'proc_type':hdu[1].header.get('proc_type', None),
                                'lomfstp':hdu[2].header.get('lomfstp', None),
                                'lapdxvdt':hdu[2].header.get('lapdxvdt', None),
                                'lapdlvdt':hdu[2].header.get('lapdlvdt', None),
                                'lom1posc':hdu[2].header.get('lom1posc', None),
                                'lom2posc':hdu[2].header.get('lom2posc', None),
                                'lom1posf':hdu[2].header.get('lom1posf', None),
                                'lom2posf':hdu[2].header.get('lom2posf', None)
                                                                                }
                    session.add(sptkeys(**keywords))
            except IOError as e:
                print(e.message)
                #-- Handle empty or corrupt FITS files
                session.add(sptkeys(file_id=f_key))
    else:
        try:
            with fits.open(filename) as hdu:
                keywords = {  'filetype':hdu[0].header['filetype'],
                                  'instrume':hdu[0].header['instrume'],
                                  'rootname':hdu[0].header['rootname'],
                                  'imagetyp':hdu[0].header['imagetyp'],
                                  'targname':hdu[0].header['targname'],
                                  'ra_targ':hdu[0].header['ra_targ'],
                                  'dec_targ':hdu[0].header['dec_targ'],
                                  'proposid':hdu[0].header['proposid'],
                                  'qualcom1':hdu[0].header.get('qualcom1', ''),
                                  'qualcom2':hdu[0].header.get('qualcom2', ''),
                                  'qualcom3':hdu[0].header.get('qualcom3', ''),
                                  'quality':hdu[0].header.get('quality', ''),
                                  'postarg1':hdu[0].header['postarg1'],
                                  'postarg2':hdu[0].header['postarg2'],
                                  'cal_ver':hdu[0].header['cal_ver'],
                                  'proctime':hdu[0].header['proctime'],

                                  'opus_ver':hdu[0].header['opus_ver'],
                                  'obstype':hdu[0].header['obstype'],
                                  'obsmode':hdu[0].header['obsmode'],
                                  'exptype':hdu[0].header['exptype'],
                                  'detector':hdu[0].header['detector'],
                                  'segment':hdu[0].header['segment'],
                                  'detecthv':hdu[0].header['detecthv'],
                                  'life_adj':hdu[0].header['life_adj'],
                                  'fppos':hdu[0].header['fppos'],
                                  'exp_num':hdu[0].header['exp_num'],
                                  'cenwave':hdu[0].header['cenwave'],
                                  'propaper':hdu[0].header['propaper'],
                                  'apmpos':hdu[0].header.get('apmpos', None),
                                  'aperxpos':hdu[0].header.get('aperxpos', None),
                                  'aperypos':hdu[0].header.get('aperypos', None),
                                  'aperture':hdu[0].header['aperture'],
                                  'opt_elem':hdu[0].header['opt_elem'],
                                  'shutter':hdu[0].header['shutter'],
                                  'extended':hdu[0].header['extended'],
                                  'obset_id':hdu[0].header.get('obset_id', None),
                                  'asn_id':hdu[0].header.get('asn_id', None),
                                  'asn_tab':hdu[0].header.get('asn_tab', None),
                                  'asn_mtyp':hdu[1].header['asn_mtyp'],
                                  'overflow':hdu[1].header.get('overflow', None),
                                  'nevents':hdu[1].header.get('nevents', None),
                                  'neventsa':hdu[1].header.get('neventsa', None),
                                  'neventsb':hdu[1].header.get('neventsb', None),
                                  'dethvla':hdu[1].header.get('dethvla', None),
                                  'dethvlb':hdu[1].header.get('dethvlb', None),
                                  'deventa':hdu[1].header.get('deventa', None),
                                  'deventb':hdu[1].header.get('deventb', None),
                                  'feventa':hdu[1].header.get('feventa', None),
                                  'feventb':hdu[1].header.get('feventb', None),
                                  'hvlevela':hdu[1].header.get('hvlevela', None),
                                  'hvlevelb':hdu[1].header.get('hvlevelb', None),
                                  'date_obs':hdu[1].header['date-obs'],
                                  'dpixel1a':hdu[1].header.get('dpixel1a', None),
                                  'dpixel1b':hdu[1].header.get('dpixel1b', None),
                                  'time_obs':hdu[1].header['time-obs'],
                                  'expstart':hdu[1].header['expstart'],
                                  'expend':hdu[1].header['expend'],
                                  'exptime':hdu[1].header['exptime'],
                                  'numflash':hdu[1].header.get('numflash', None),
                                  'ra_aper':hdu[1].header['ra_aper'],
                                  'dec_aper':hdu[1].header['dec_aper'],
                                  'shift1a':hdu[1].header.get('shift1a', None),
                                  'shift1b':hdu[1].header.get('shift1b', None),
                                  'shift1c':hdu[1].header.get('shift1c', None),
                                  'shift2a':hdu[1].header.get('shift2a', None),
                                  'shift2b':hdu[1].header.get('shift2b', None),
                                  'shift2c':hdu[1].header.get('shift2c', None),

                                  'sp_loc_a':hdu[1].header.get('sp_loc_a', None),
                                  'sp_loc_b':hdu[1].header.get('sp_loc_b', None),
                                  'sp_loc_c':hdu[1].header.get('sp_loc_c', None),
                                  'sp_nom_a':hdu[1].header.get('sp_nom_a', None),
                                  'sp_nom_b':hdu[1].header.get('sp_nom_b', None),
                                  'sp_nom_c':hdu[1].header.get('sp_nom_c', None),
                                  'sp_off_a':hdu[1].header.get('sp_off_a', None),
                                  'sp_off_b':hdu[1].header.get('sp_off_b', None),
                                  'sp_off_c':hdu[1].header.get('sp_off_c', None),
                                  'sp_err_a':hdu[1].header.get('sp_err_a', None),
                                  'sp_err_b':hdu[1].header.get('sp_err_b', None),
                                  'sp_err_c':hdu[1].header.get('sp_err_c', None),

                                  'dethvl':hdu[1].header.get('dethvl', None)
                                                                                }
                session.add(Headers(**keywords))
        except IOError as e:
            print(e.message)
            #-- Handle empty or corrupt FITS files
            session.add(Headers(file_id=f_key))

    session.commit()
    session.close()
    engine.dispose()
    '''
    path, name = os.path.split(filename)
    spt_file = os.path.join(path, keywords['rootname'] + '_spt.fits.gz')
    if os.path.isfile(spt_file):
        print(spt_file)
        with fits.open(spt_file) as spt_hdu:
            keywords['proc_type']=spt_hdu[1].header.get('proc_type', None)
            keywords['lomfstp']=spt_hdu[2].header.get('lomfstp', None)
            keywords['lapdxvdt']=spt_hdu[2].header.get('lapdxvdt', None)
            keywords['lapdlvdt']=spt_hdu[2].header.get('lapdlvdt', None)
            keywords['lom1posc']=spt_hdu[2].header.get('lom1posc', None)
            keywords['lom2posc']=spt_hdu[2].header.get('lom2posc', None)
            keywords['lom1posf']=spt_hdu[2].header.get('lom1posf', None)
            keywords['lom2posf']=spt_hdu[2].header.get('lom2posf', None)
    '''
#-------------------------------------------------------------------------------

def populate_data(num_cpu=1):
    print("adding to data")

    session = Session()
    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(Files.name.like('%_x1d.fits%')).\
                                outerjoin(Data, Files.id == Data.file_id).\
                                filter(Data.file_id == None)]
    session.close()

    args = [(full_filename, f_key) for f_key, full_filename in files_to_add]

    pool = mp.Pool(processes=num_cpu)
    pool.map(update_data, args)

#-------------------------------------------------------------------------------

def update_data((args)):
    """Update DB data table in parallel"""

    filename, f_key = args
    print(filename)

    Session, engine = load_connection(SETTINGS['connection_string'])
    session = Session()

    try:
        with fits.open(filename) as hdu:
            if len(hdu[1].data):
                flux_mean=hdu[1].data['flux'].ravel().mean()
                flux_max=hdu[1].data['flux'].ravel().max()
                flux_std=hdu[1].data['flux'].ravel().std()
            else:
                flux_mean = None
                flux_max = None
                flux_std = None

            session.add(Data(flux_mean=flux_mean,
                             flux_max=flux_max,
                             flux_std=flux_std,
                             file_id=f_key))

    except IOError as e:
        print(e.message)
        #-- Handle empty or corrupt FITS files
        session.add(Data(file_id=f_key))

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def delete_file_from_all(filename):
    """Delete a filename from all databases and directory structure

    Parameters
    ----------
    filename : str
        name of the file, will be pattern-matched with %filename%

    """

    session = Session()

    files_to_remove = [(result.id, os.path.join(result.path, result.name))
                            for result in session.query(Files).\
                                    filter(Files.name.like('%{}%'.format(filename)))]


    for (file_id, file_path) in files_to_remove:
        for table in reversed(Base.metadata.sorted_tables):
            session.query(table).filter(table.file_id==file_id).delete()
        print("Removing file {}".format(file_path))
        os.remove(file_path)

    session.close()

#-------------------------------------------------------------------------------

def clear_all_databases(SETTINGS):
    """Dump all databases of all contents...seriously"""

    if not raw_input("Are you sure you want to delete everything? Y/n") == 'Y':
        sys.exit("Not deleting, getting out of here.")

    session = Session()
    for table in reversed(Base.metadata.sorted_tables):
        #if table.name == 'files':
        #    #continue
        #    pass
        try:
            print("Deleting {}".format(table.name))
            session.execute(table.delete())
            session.commit()
        except:
            print("Cannot delete {}".format(table.name))
            pass

#-------------------------------------------------------------------------------

def do_all():
    print(SETTINGS)
    Base.metadata.create_all(engine)
    #insert_files(**SETTINGS)
    #populate_primary_headers(SETTINGS['num_cpu'])
    populate_spt(SETTINGS['num_cpu'])
    #populate_data(SETTINGS['num_cpu'])
    #populate_lampflash(SETTINGS['num_cpu'])
    #populate_darks(SETTINGS['num_cpu'])
    #populate_stims(SETTINGS['num_cpu'])

#-------------------------------------------------------------------------------

def run_all_monitors():
    dark_monitor()
    stim_monitor()

#-------------------------------------------------------------------------------

def clean_slate(config_file=None):
    clear_all_databases(SETTINGS)
    #Base.metadata.drop_all(engine, checkfirst=False)
    Base.metadata.create_all(engine)

    do_all()

    run_all_monitors()

#-------------------------------------------------------------------------------

if __name__ == "__main__":
    clean_slate('~/configure.yaml')
'''

    t = """
        CREATE TEMPORARY TABLE which_file (rootname CHAR(9), has_x1d BOOLEAN,has_corr BOOLEAN, has_raw BOOLEAN, has_acq BOOLEAN);
        CREATE INDEX info ON which_file (rootname, has_x1d, has_corr, has_raw, has_acq);

        INSERT INTO which_file (rootname, has_x1d, has_corr, has_raw, has_acq)
          SELECT rootname,
               IF(SUM(name LIKE '%_x1d%'), true, false) as has_x1d,
               IF(SUM(name LIKE '%_corrtag%'), true, false) as has_corr,
               IF(SUM(name LIKE '%_rawtag%'), true, false) as has_raw,
               IF(SUM(name LIKE '%_rawacq%'), true, false) as has_acq
                   FROM files
                   WHERE rootname NOT IN (SELECT headers.rootname from headers)
                   GROUP BY rootname;
        """
    q = """
        SELECT
         CASE
                           WHEN which_file.has_x1d = 1 THEN (SELECT CONCAT(files.path, '/', files.name) FROM files WHERE files.rootname = which_file.rootname AND
                                                                                                files.name LIKE CONCAT(which_file.rootname, '_x1d.fits%') LIMIT 1)
                           WHEN which_file.has_corr = 1 THEN (SELECT CONCAT(files.path, '/', files.name) FROM files WHERE files.rootname = which_file.rootname AND
                                                                                                 files.name LIKE CONCAT(which_file.rootname, '_corrtag%') LIMIT 1)
                           WHEN which_file.has_raw = 1 THEN (SELECT CONCAT(files.path, '/', files.name) FROM files WHERE files.rootname = which_file.rootname AND
                                                                                                files.name LIKE CONCAT(which_file.rootname, '_rawtag%') LIMIT 1)
                           WHEN which_file.has_acq = 1 THEN (SELECT CONCAT(files.path, '/', files.name) FROM files WHERE files.rootname = which_file.rootname AND
                                                                                                files.name LIKE CONCAT(which_file.rootname, '_rawacq%') LIMIT 1)
                           ELSE NULL
                         END as file_to_grab,
         CASE
                            WHEN which_file.has_x1d = 1 THEN (SELECT files.id FROM files WHERE files.rootname = which_file.rootname AND
                                                                                                 files.name LIKE CONCAT(which_file.rootname, '_x1d.fits%') LIMIT 1)
                            WHEN which_file.has_corr = 1 THEN (SELECT files.id FROM files WHERE files.rootname = which_file.rootname AND
                                                                                                 files.name LIKE CONCAT(which_file.rootname, '_corrtag%') LIMIT 1)
                            WHEN which_file.has_raw = 1 THEN (SELECT files.id FROM files WHERE files.rootname = which_file.rootname AND
                                                                                                 files.name LIKE CONCAT(which_file.rootname, '_rawtag%') LIMIT 1)
                            WHEN which_file.has_acq = 1 THEN (SELECT files.id FROM files WHERE files.rootname = which_file.rootname AND
                                                                                                 files.name LIKE CONCAT(which_file.rootname, '_rawacq%') LIMIT 1)
                            ELSE NULL
                          END as file_id
        FROM which_file
            WHERE (has_x1d = 1 OR has_corr = 1 OR has_raw = 1 OR has_acq);
    """

    #connection = engine.connect()
    #connection.execute(text(t))

    #results = engine.execute(text(t))
    #results.close()

    #files_to_add = [(result.file_id, result.file_to_grab) for result in connection.execute(text(q))
    #                    if not result.file_id == None]
'''
