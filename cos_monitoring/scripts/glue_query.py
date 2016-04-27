from __future__ import print_function, absolute_import, division

import os

from sqlalchemy.engine import create_engine
from glue import qglue

from .db_tables import load_connection, open_settings
from .db_tables import Base
from .db_tables import Files, Headers
from .db_tables import Lampflash, Stims, Phd, Darks, sptkeys, Data, Gain


def query_open_in_glue(query):

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    connection = engine.connect()

    results = connection.execute(query)

    for row in results:
        print(row)



#===============================================================================
def main():
    'MAYBE USE ARGPARSE FOR MULTIPLE QUERIES'
    q = input('ENTER QUERY HERE: ')
    print(q)
    query_open_in_glue(q)

#===============================================================================
