from __future__ import print_function, absolute_import, division

import os

from sqlalchemy.engine import create_engine
from glue import qglue
import pandas as pd

from .db_tables import load_connection, open_settings
from .db_tables import Base
from .db_tables import Files, Headers
from .db_tables import Lampflash, Stims, Phd, Darks, sptkeys, Data, Gain

from ..scripts.create_master_csv import csv_generator

def query_open_in_glue(query):

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    connection = engine.connect()
    results = connection.execute(query)
    '''
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    '''
    data_dict = {}
    for row in results:
        for key in results.keys():
            data_dict.setdefault(key, []).append(row[key])


    data = pd.DataFrame(data_dict)

    qglue(data=data)



#===============================================================================
def main():
    'MAYBE USE ARGPARSE FOR MULTIPLE QUERIES'
    #q = raw_input('ENTER QUERY HERE: ')
    q = """SELECT * FROM darks;"""
    query_open_in_glue(q)

#===============================================================================
