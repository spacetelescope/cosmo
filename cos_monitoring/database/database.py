from __future__ import print_function, absolute_import, division

from astropy.io import fits
import os
from sqlalchemy import and_, or_
import sys
import yaml
import numpy as np
import multiprocessing as mp


from ..filesystem import find_all_datasets
from ..osm.monitor import pull_flashes
from .db_tables import load_connection
from .db_tables import Base
from .db_tables import Files, Headers, Data
from .db_tables import Lampflash, Variability, Stims, Phd


config_file = os.path.join(os.environ['HOME'], "configure.yaml")
with open(config_file, 'r') as f:
    SETTINGS = yaml.load(f)

Session, engine = load_connection(SETTINGS['connection_string'])

#-------------------------------------------------------------------------------

def mp_insert(args):
    filename, table, function = args
    insert_with_yield(filename, table, function)

#-------------------------------------------------------------------------------

def insert_with_yield(filename, table, function):
    """
    Insert or update the table with information in the record_dict.
    Parameters
    ----------
    table :
        The table of the database to update.
    data : dict
        A dictionary of the information to update.  Each key of the
        dictionary must be a column in the Table.
    id_num : string
        The row ID to update.  If id_test is blank, then a new row is
        inserted.
    """

    Session, engine = load_connection(SETTINGS['connection_string'])
    session = Session()

    for data in function(filename):
        print(data.values())
        try:
            session.add(table(**data))
        except IOError as e:
            print(e.message)
            #-- Handle empty or corrupt FITS files
            session.add(table())

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def insert_files(**kwargs):
    """Create table of path, filename"""

    session = Session()
    print("Querying previously found files")
    previous_files = {os.path.join(path, fname) for path, fname in session.query(Files.path, Files.name)}

    data_location = kwargs.get('data_location', './')
    print("Looking for new files in {}".format(data_location))

    for i, (path, filename) in enumerate(find_all_datasets(data_location)):
        full_filepath = os.path.join(path, filename)
        if full_filepath in previous_files:
            print("Already found: {}".format(full_filepath))
            continue

        print("NEW: Found {}".format(full_filepath))
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
    print("adding to lampflash")
    session = Session()
    ### also should have the _rawacqs as well
    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(Files.name.like('%lampflash%')).\
                                outerjoin(Lampflash, Files.id == Lampflash.file_id).\
                                filter(Lampflash.file_id == None)]
    session.close()


    args = [(full_filename, Lampflash, pull_flashes) for f_key, full_filename in files_to_add]

    pool = mp.Pool(processes=num_cpu)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------

def populate_primary_headers(num_cpu=1):
    print("adding to primary headers")

    session = Session()
    #files_to_add = [(result.id, os.path.join(result.path, result.name))
    #                    for result in session.query(Files).\
    #                            filter(or_(Files.name.like('%_rawaccum%'),
    #                                       Files.name.like('%_rawtag%'))).\
    #                            outerjoin(Headers, Files.id == Headers.file_id).\
    #                            filter(Headers.file_id == None)]

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(Files.name.like('%_x1d.fits%')).\
                                outerjoin(Data, Files.id == Data.file_id).\
                                filter(Data.file_id == None)]

    session.close()

    args = [(full_filename, f_key) for f_key, full_filename in files_to_add]

    pool = mp.Pool(processes=num_cpu)
    pool.map(update_header, args)

#-------------------------------------------------------------------------------

def update_header((args)):
    """update DB header table in parallel"""

    filename, f_key = args
    print(filename)

    Session, engine = load_connection(SETTINGS['connection_string'])
    session = Session()

    try:
        with fits.open(filename) as hdu:
            session.add(Headers(filetype=hdu[0].header['filetype'],
                                instrume=hdu[0].header['instrume'],
                                rootname=hdu[0].header['rootname'],
                                imagetyp=hdu[0].header['imagetyp'],
                                targname=hdu[0].header['targname'],
                                ra_targ=hdu[0].header['ra_targ'],
                                dec_targ=hdu[0].header['dec_targ'],
                                proposid=hdu[0].header['proposid'],
                                qualcom1=hdu[0].header.get('qualcom1', ''),
                                qualcom2=hdu[0].header.get('qualcom2', ''),
                                qualcom3=hdu[0].header.get('qualcom3', ''),
                                quality=hdu[0].header.get('quality', ''),
                                cal_ver=hdu[0].header['cal_ver'],
                                opus_ver=hdu[0].header['opus_ver'],
                                obstype=hdu[0].header['obstype'],
                                obsmode=hdu[0].header['obsmode'],
                                exptype=hdu[0].header['exptype'],
                                detector=hdu[0].header['detector'],
                                segment=hdu[0].header['segment'],
                                detecthv=hdu[0].header['detecthv'],
                                life_adj=hdu[0].header['life_adj'],
                                fppos=hdu[0].header['fppos'],
                                exp_num=hdu[0].header['exp_num'],
                                cenwave=hdu[0].header['cenwave'],
                                aperture=hdu[0].header['aperture'],
                                opt_elem=hdu[0].header['opt_elem'],
                                shutter=hdu[0].header['shutter'],
                                extended=hdu[0].header['extended'],
                                obset_id=hdu[0].header['obset_id'],
                                asn_id=hdu[0].header['asn_id'],
                                asn_tab=hdu[0].header['asn_tab'],

                                hvlevela=hdu[1].header.get('hvlevela', -999),
                                hvlevelb=hdu[1].header.get('hvlevelb', -999),
                                date_obs=hdu[1].header['date-obs'],
                                dpixel1a=hdu[1].header.get('dpixel1a', -999),
                                dpixel1b=hdu[1].header.get('dpixel1b', -999),
                                time_obs=hdu[1].header['time-obs'],
                                expstart=hdu[1].header['expstart'],
                                expend=hdu[1].header['expend'],
                                exptime=hdu[1].header['exptime'],
                                numflash=hdu[1].header.get('numflash', None),
                                ra_aper=hdu[1].header['ra_aper'],
                                dec_aper=hdu[1].header['dec_aper'],
                                shift1a=hdu[1].header.get('shift1a', -999),
                                shift1b=hdu[1].header.get('shift1b', -999),
                                shift1c=hdu[1].header.get('shift1c', -999),
                                shift2a=hdu[1].header.get('shift2a', -999),
                                shift2b=hdu[1].header.get('shift2b', -999),
                                shift2c=hdu[1].header.get('shift2c', -999),
                                sp_loc_a=hdu[1].header.get('sp_loc_a', -999),
                                sp_loc_b=hdu[1].header.get('sp_loc_b', -999),
                                sp_loc_c=hdu[1].header.get('sp_loc_c', -999),
                                sp_nom_a=hdu[1].header.get('sp_nom_a', -999),
                                sp_nom_b=hdu[1].header.get('sp_nom_b', -999),
                                sp_nom_c=hdu[1].header.get('sp_nom_c', -999),
                                sp_off_a=hdu[1].header.get('sp_off_a', -999),
                                sp_off_b=hdu[1].header.get('sp_off_b', -999),
                                sp_off_c=hdu[1].header.get('sp_off_c', -999),
                                sp_err_a=hdu[1].header.get('sp_err_a', -999),
                                sp_err_b=hdu[1].header.get('sp_err_b', -999),
                                sp_err_c=hdu[1].header.get('sp_err_c', -999),

                                file_id=f_key))
    except IOError as e:
        print(e.message)
        #-- Handle empty or corrupt FITS files
        session.add(Headers(file_id=f_key))

    session.commit()
    session.close()
    engine.dispose()

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
                flux_mean=round(hdu[1].data['flux'].ravel().mean(), 5)
                flux_max=round(hdu[1].data['flux'].ravel().max(), 5)
                flux_std=round(hdu[1].data['flux'].ravel().std(), 5)
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
        if table.name == 'files':
            #continue
            pass
        try:
            print("Deleting {}".format(table.name))
            session.execute(table.delete())
            session.commit()
        except:
            pass

#-------------------------------------------------------------------------------

def clean_slate(config_file=None):
    #clear_all_databases(SETTINGS)
    #Base.metadata.drop_all(engine, checkfirst=False)
    Base.metadata.create_all(engine)

    print(SETTINGS)
    #insert_files(**SETTINGS)
    #populate_primary_headers(SETTINGS['num_cpu'])
    #populate_data(SETTINGS['num_cpu'])
    populate_lampflash(SETTINGS['num_cpu'])

#-------------------------------------------------------------------------------

if __name__ == "__main__":
    clean_slate('~/configure.yaml')
