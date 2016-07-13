from __future__ import print_function, absolute_import, division

from astropy.io import fits
import os
from sqlalchemy import and_, or_, text
import sys
import matplotlib as mpl
mpl.use('Agg')
import numpy as np
import multiprocessing as mp
import types
import argparse
import pprint

from ..cci.gainmap import make_all_hv_maps
from ..cci.gainmap import write_and_pull_gainmap
from ..cci.monitor import monitor as cci_monitor
from ..dark.monitor import monitor as dark_monitor
from ..dark.monitor import pull_orbital_info
from ..filesystem import find_all_datasets
from ..osm.monitor import pull_flashes
from ..osm.monitor import monitor as osm_monitor
from ..stim.monitor import locate_stims
from ..stim.monitor import stim_monitor
from .db_tables import load_connection, open_settings
from .db_tables import Base
from .db_tables import Files, Headers
from .db_tables import Lampflash, Stims, Darks, sptkeys, Data, Gain, Acqs


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
    else:
        raise ValueError("Not provided the right number of args")

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
        elif isinstance(data, types.GeneratorType):
            pass
        else:
            raise ValueError("Not designed to work with data of type {}".format(type(data)))

        #-- Pull data from generator and commit
        for i, row in enumerate(data):
            row['file_id'] = foreign_key
            #if i == 0:
            #    print(row.keys())
            #print(row.values())

        #-- Converts np arrays to native python type...
        #-- This is to allow the database to ingest values as type float
        #-- instead of Decimal Class types in sqlalchemy....
            for key in row:
                if isinstance(row[key], np.generic):
                    row[key] = np.asscalar(row[key])
                else:
                    continue

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

    for i, (path, filename) in enumerate(find_all_datasets(data_location, SETTINGS['num_cpu'])):
        full_filepath = os.path.join(path, filename)

        if full_filepath in previous_files:
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
                                filter(or_(Files.name.like('%lampflash%'), (Files.name.like('%_rawacq%')))).\
                                outerjoin(Lampflash, Files.id == Lampflash.file_id).\
                                filter(Lampflash.file_id == None)]
    session.close()


    args = [(full_filename, Lampflash, pull_flashes, f_key) for f_key, full_filename in files_to_add]

    print("Found {} files to add".format(len(args)))
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

    print("Found {} files to add".format(len(args)))
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
                            outerjoin(Headers, Files.rootname == Headers.rootname).\
                            outerjoin(Darks, Files.id == Darks.file_id).\
                            filter(Headers.targname == 'DARK').\
                            filter(Darks.file_id == None).\
                            filter(Files.name.like('%\_corrtag%'))]

    session.close()

    args = [(full_filename, Darks, pull_orbital_info, f_key) for f_key, full_filename in files_to_add]

    print("Found {} files to add".format(len(args)))
    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------

def populate_gain(num_cpu=1):
    """ Populate the cci gain table

    """

    print("Adding to gain")
    session = Session()

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                            outerjoin(Gain, Files.id == Gain.file_id).\
                            filter(or_(Files.name.like('l\_20161%\_00\____\_cci%'),
                                       Files.name.like('l\_20161%\_01\____\_cci%'))).\
                            filter(Gain.file_id == None)]
    session.close()

    args = [(full_filename, Gain, write_and_pull_gainmap, f_key) for f_key, full_filename in files_to_add]

    print("Found {} files to add".format(len(args)))
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
                                outerjoin(sptkeys, Files.id == sptkeys.file_id).\
                                filter(sptkeys.file_id == None)]
    session.close()
    args = [(full_filename, sptkeys, get_spt_keys, f_key) for f_key, full_filename in files_to_add]

    print("Found {} files to add".format(len(args)))
    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert,args)

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
    args = [(full_filename, Data, update_data, f_key) for f_key, full_filename in files_to_add]
    print("Found {} files to add".format(len(args)))
    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------

def populate_primary_headers(num_cpu=1):
    """ Populate the table of primary header information

    """
    print("Adding to primary headers")

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

    engine.execute(text(t))
    results = engine.execute(text(q))
    print(results)
    files_to_add = [(result.file_id, result.file_to_grab) for result in engine.execute(text(q))
                        if not result.file_id == None]

    print(files_to_add)
    args = [(full_filename, Headers, get_primary_keys, f_key) for f_key, full_filename in files_to_add]
    print("Found {} files to add".format(len(args)))
    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)
#-------------------------------------------------------------------------------
def populate_acqs(num_cpu=1):
    print("adding to data")

    session = Session()
    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(Files.name.like('%_rawacq.fits%')).\
                                outerjoin(Acqs, Files.id == Acqs.file_id).\
                                filter(Acqs.file_id == None)]
    session.close()
    args = [(full_filename, Acqs, get_acq_keys, f_key) for f_key, full_filename in files_to_add]
    print("Found {} files to add".format(len(args)))
    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)
#-------------------------------------------------------------------------------

def get_spt_keys(filename):
    """ Read the necessary keywords from SPT files

    Parameters
    ----------
    filename : str
        name of the file to read

    Returns
    -------
    keywords : dict
        dictionary of keyword,value pairs
    """

    with fits.open(filename) as hdu:
        keywords = {'rootname':hdu[0].header.get('rootname', None),
                    'proc_typ':hdu[0].header.get('proc_typ', None),
                    'prop_typ':hdu[0].header.get('prop_typ', None),
                    'lomfstp':hdu[2].header.get('lomfstp', None),
                    'lapxlvdt':hdu[2].header.get('lapxlvdt', None),
                    'lapdlvdt':hdu[2].header.get('lapdlvdt', None),
                    'lom1posc':hdu[2].header.get('lom1posc', None),
                    'lom2posc':hdu[2].header.get('lom2posc', None),
                    'lom1posf':hdu[2].header.get('lom1posf', None),
                    'lom2posf':hdu[2].header.get('lom2posf', None),
                    'ldcampat':hdu[2].header.get('ldcampat', None),
                    'ldcampbt':hdu[2].header.get('ldcampbt', None),
                    'lmmcetmp':hdu[2].header.get('lmmcetmp', None),
                    'dominant_gs':hdu[0].header.get('dgestar', None),
                    'secondary_gs':hdu[0].header.get('sgestar', None),
                    'start_time':hdu[0].header.get('PSTRTIME', None),
                    'search_dimensions':hdu[1].header.get('lqtascan', None),
                    'search_step_size':hdu[1].header.get('lqtastep', None),
                    'search_type':hdu[1].header.get('lqtacent', None),
                    'search_floor':hdu[1].header.get('lqtaflor', None),
                    'lqtadpos':hdu[1].header.get('lqtadpos', None),
                    'lqtaxpos':hdu[1].header.get('lqtaxpos', None),
                    'lqitime':hdu[1].header.get('lqitime', None)
                    }

    return keywords

#-------------------------------------------------------------------------------

def get_primary_keys(filename):
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
                          'randseed':hdu[0].header.get('randseed', None),
                          'asn_mtyp':hdu[1].header.get('asn_mtyp', None),
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

                          'dethvl':hdu[1].header.get('dethvl', None),
                                                                        }
    return keywords

#-------------------------------------------------------------------------------
def update_data(args):
    """Update DB data table in parallel"""

    #args is the filename!!! (might want to design this like other functions)
    try:
        with fits.open(args) as hdu:
            if len(hdu[1].data):
                data ={'flux_mean':hdu[1].data['flux'].ravel().mean(),
                       'flux_max':hdu[1].data['flux'].ravel().max(),
                       'flux_std':hdu[1].data['flux'].ravel().std(),
                       'wl_min':hdu[1].data['wavelength'].ravel().min(),
                       'wl_max':hdu[1].data['wavelength'].ravel().max()
                                                                              }
            else:
                data = {'flux_mean':None,
                        'flux_max':None,
                        'flux_std':None,
                        'wl_min':None,
                        'wl_max':None
                                        }
            return data
    except IOError:
        print('IOERROR')
#-------------------------------------------------------------------------------

def get_acq_keys(filename):
    with fits.open(filename) as hdu:
        keywords = {'rootname':hdu[0].header['rootname'],
                    'obset_id': hdu[1].header.get('obset_id', None),
                    'linenum':hdu[0].header['linenum'],
                    'exptype':hdu[0].header['exptype'],
                    'target':hdu[0].header.get('targname', None),
                    }
    return keywords

#-------------------------------------------------------------------------------

def cm_delete():
    parser = argparse.ArgumentParser(description='Delete file from all databases.')
    parser.add_argument('filename',
                        type=str,
                        help='search string to delete')
    args = parser.parse_args()

    delete_file_from_all(args.filename)

#-------------------------------------------------------------------------------

def delete_file_from_all(filename):
    """Delete a filename from all databases and directory structure

    Parameters
    ----------
    filename : str
        name of the file, will be pattern-matched with %filename%

    """

    session = Session()

    print(filename)
    files_to_remove = [(result.id, os.path.join(result.path, result.name))
                            for result in session.query(Files).\
                                    filter(Files.name.like("""%{}%""".format(filename)))]
    session.close()


    print("Found: ")
    print(files_to_remove)
    for (file_id, file_path) in files_to_remove:
        for table in reversed(Base.metadata.sorted_tables):
            print("Removing {}, {} from {}".format(file_path, file_id, table.name))
            if table.name == 'files':
                q = """DELETE FROM {} WHERE id={}""".format(table.name, file_id)
            else:
                q = """DELETE FROM {} WHERE file_id={}""".format(table.name, file_id)

            engine.execute(text(q))


#-------------------------------------------------------------------------------

def cm_describe():
    parser = argparse.ArgumentParser(description='Show file from all databases.')
    parser.add_argument('filename',
                        type=str,
                        help='search string to show')
    args = parser.parse_args()

    show_file_from_all(args.filename)

#-------------------------------------------------------------------------------

def show_file_from_all(filename):
    """
    """

    session = Session()

    print(filename)
    files_to_show = [result.rootname
                            for result in session.query(Files).\
                                    filter(Files.name.like("""%{}%""".format(filename)))]
    session.close()


    print("Found: ")
    print(files_to_show)
    for table in reversed(Base.metadata.sorted_tables):
        if not 'rootname' in table.columns:
            continue

        print("**************************")
        print("Searching {}".format(table.name))
        print("**************************")

        for rootname in set(files_to_show):
            q = """SELECT * FROM {} WHERE rootname LIKE '%{}%'""".format(table.name, rootname)

            results = engine.execute(text(q))

            for i, row in enumerate(results):
                for k in row.keys():
                    print(table.name, rootname, k, row[k])

#-------------------------------------------------------------------------------

def clear_all_databases(SETTINGS, nuke=False):
    """Dump all databases of all contents...seriously"""


    if not raw_input("Are you sure you want to delete everything? Y/N: ") == 'Y':
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
    #populate_spt(SETTINGS['num_cpu'])
    #populate_data(SETTINGS['num_cpu'])
    #populate_lampflash(SETTINGS['num_cpu'])
    #populate_darks(SETTINGS['num_cpu'])
    populate_gain(SETTINGS['num_cpu'])
    #populate_stims(SETTINGS['num_cpu'])
    #populate_acqs(SETTINGS['num_cpu'])

#-------------------------------------------------------------------------------

def run_all_monitors():
    print('RUNNING MONITORS')
    #dark_monitor(SETTINGS['monitor_location'])
    cci_monitor()
    #stim_monitor()
    #osm_monitor()

#-------------------------------------------------------------------------------

def clean_slate_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n",'--nuke',
                        help="Parser argument to nuke DB", default=True)
    args = parser.parse_args()
    return args

#-------------------------------------------------------------------------------

def clean_slate(config_file=None):

    args = clean_slate_parser()
    if args.nuke == True:
        #clear_all_databases(SETTINGS, args.nuke)
        #Base.metadata.drop_all(engine, checkfirst=False)
        #Base.metadata.create_all(engine)

        #do_all()

        #run_all_monitors()
        print('NUKKKKE')

#-------------------------------------------------------------------------------

if __name__ == "__main__":
    clean_slate('~/configure.yaml')
