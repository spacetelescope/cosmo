from __future__ import print_function, absolute_import, division

import argparse
import os
import itertools

from sqlalchemy.engine import create_engine
from glue import qglue
import pandas as pd

from .db_tables import load_connection, open_settings
from .db_tables import Base
from .db_tables import Files, Headers
from .db_tables import Lampflash, Stims, Phd, Darks, sptkeys, Data, Gain

from ..scripts.create_master_csv import csv_generator
#===============================================================================
def build_query(connection,args):
    
    if len(args.columns) == 1:
        cmd = """SELECT %s FROM %s;""" % (args.columns[0],args.tables)
        results = connection.execute(cmd)

    if len(args.columns) > 1:
        columns = ",".join(args.columns)
        cmd = """SELECT %s FROM %s""" % (columns,args.tables)
        results = connection.execute(cmd)

    return results
#===============================================================================
def query_open_in_glue(args):

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    drops = map(''.join, itertools.product(*((c.upper(), c.lower()) for c in 'DROP')))
    '''
    for drop in drops:
        if drop in query:
            print(query)
            print('WHY WOULD YOU TRY TO DROP THE TABLE? GET OUT!!!!!!!!!!!')
            exit()
    '''
    connection = engine.connect()
    results = build_query(connection, args)

    data_dict = {}
    for row in results:
        for key in results.keys():
            data_dict.setdefault(key, []).append(row[key])


    data = pd.DataFrame(data_dict)

    qglue(data=data)

#===============================================================================
def main():

    parser = argparse.ArgumentParser(description='Processes Queries for glueviz')
    parser.add_argument('tables', type=str,
                   help='Table or tables to be quiried')
    parser.add_argument('-c','--columns', nargs='+', type=str,
                   help='Name of the columns in tables you want to query')
    parser.add_argument('-j','--join', type=str, default=None,
                   help='Table header you want to join on')
    parser.add_argument('-w','--where', type=str, default=None,
                   help='Add where statement in query')
    args = parser.parse_args()

    query_open_in_glue(args)

#===============================================================================
if __name__ == "__main__":
    main()
