from __future__ import print_function, absolute_import, division

import os

from sqlalchemy.engine import create_engine

from .db_tables import load_connection, open_settings
from .db_tables import Base
from .db_tables import Files, Headers
from .db_tables import Lampflash, Stims, Phd, Darks, sptkeys, Data, Gain

from ..scripts.create_master_csv import csv_generator

def query_to_text(query,path,filename):

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    connection = engine.connect()

    results = connection.execute(query)
    print('                                                          '+ filename +'                                                                ')
    print('========================================================================================================================================')
    for row in results:
        print(row)
    print('========================================================================================================================================')
    print('WRITING {} REPORT TO DIR {}'.format(filename,path))
    csv_generator(results,results.keys(),path,filename)



#===============================================================================
def query_darks_null():
    q = """SELECT * FROM darks JOIN files ON
            darks.file_id = files.id WHERE
            concat(darks.obsname,
                   darks.rootname,
                   darks.detector,
                   darks.date,
                   darks.dark,
                   darks.ta_dark,
                   darks.latitude,
                   darks.longitude,
                   darks.sun_lat,
                   darks.sun_lon,
                   darks.temp)
            IS NULL ORDER BY files.name;"""

    query_to_text(q,os.getcwd(),'null_darks_tab.txt')
#===============================================================================

def query_data_null():
    q = """SELECT * FROM data JOIN files ON
            data.file_id = files.id WHERE
            concat(data.flux_mean,
                   data.flux_max,
                   data.flux_std,
                   data.wl_min,
                   data.wl_max
                   )
            IS NULL ORDER BY files.name;"""

    query_to_text(q,os.getcwd(),'null_data_tab.txt')

#===============================================================================

def query_files_null():
    q = """SELECT * FROM files WHERE
           files.path NOT LIKE '/smov/cos/Data/CCI' AND
            concat(files.id,
                   files.path,
                   files.name,
                   files.rootname
                   )
            IS NULL ORDER BY files.id;"""

    query_to_text(q,os.getcwd(),'null_files_tab.txt')

#===============================================================================

def query_gain_null():
    q = """SELECT * FROM gain JOIN files ON
            gain.file_id = files.id WHERE
            concat(gain.id,
                   gain.x,
                   gain.y,
                   gain.counts,
                   gain.std,
                   gain.segment,
                   gain.dethv,
                   gain.expstart,
                   gain.file_id)
            IS NULL ORDER BY files.name;"""
    query_to_text(q,os.getcwd(),'null_gain_tab.txt')

#===============================================================================

def query_lamp_null():
    q = """SELECT * FROM lampflash JOIN files ON
            lampflash.file_id = files.id WHERE
            concat(lampflash.date,
                   lampflash.rootname,
                   lampflash.proposid,
                   lampflash.detector,
                   lampflash.opt_elem,
                   lampflash.cenwave,
                   lampflash.fppos,
                   lampflash.lamptab,
                   lampflash.flash,
                   lampflash.x_shift,
                   lampflash.y_shift,
                   lampflash.found
                   )
            IS NULL ORDER BY files.id;"""
    query_to_text(q,os.getcwd(),'null_lamp_tab.txt')

#===============================================================================

def query_stims_null():
    q = """SELECT * FROM stims JOIN files ON
            stims.file_id = files.id WHERE
            concat(stims.time,
                   stims.rootname,
                   stims.abs_time,
                   stims.stim1_x,
                   stims.stim1_y,
                   stims.stim2_x,
                   stims.stim2_y,
                   stims.counts,
                   stims.segment
                   )
            IS NULL ORDER BY files.id;"""
    query_to_text(q,os.getcwd(),'null_stims_tab.txt')
#===============================================================================


def query_all():
    query_gain_null()
    query_darks_null()
    query_files_null()
    query_data_null()
    query_lamp_null()
    query_stims_null()
