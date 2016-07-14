#! /usr/bin/env python
from __future__ import print_function, absolute_import, division

'''
This module compares what datasets are currently in
/smov/cos/Data (by accessing the COS database) versus all datasets currently
archived in MAST. All missing datasets will be requested and placed in
the appropriate directory in /smov/cos/Data.

Use:
    This script is intended to be used in a cron job.
'''

__author__ = "Jo Taylor"
__date__ = "04-13-2016"
__maintainer__ = "Jo Taylor"
__email__ = "jotaylor@stsci.edu"

import time
import pickle
import os
import yaml
from collections import OrderedDict
import pdb
from sqlalchemy import text
from subprocess import Popen, PIPE

from ..database.db_tables import load_connection
from .request_data import run_all_retrievals

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def connect_cosdb():
    '''
    Connect to the COS database on greendev and store lists of all files.

    Parameters:
    -----------
        None

    Returns:
    --------
        nullrows : list
            All rootnames of files where ASN_ID = NONE.
        asnrows : list
            All ASN_IDs for files where ASN_ID is not NONE.
        smovjitrows : list
            All rootnames of jitter files.
    '''

    # Open the configuration file for the COS database connection (MYSQL).
    config_file = os.path.join(os.environ['HOME'], "configure.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

        print("Querying COS greendev database....")
        # Connect to the database.
        Session, engine = load_connection(SETTINGS['connection_string'])
        # All entries that ASN_ID = NULL (should be individual jits, acqs)
        null = list(engine.execute("SELECT rootname FROM headers WHERE asn_id='NONE';"))
        # All entries that have ASN_ID defined (will pull science, jit, acq)
        jitters = list(engine.execute("SELECT rootname FROM files "
                                      "WHERE RIGHT(rootname,1) = 'j';"))
        asn = list(engine.execute("SELECT asn_id FROM headers "
                                  "WHERE asn_id!='NONE';"))

        # Store SQLAlchemy results as lists
        nullrows = []
        asnrows = []
        smovjitrows = []
        for row in null:
            nullrows.append(row["rootname"].upper())
        for row in asn:
            asnrows.append(row["asn_id"])
        for row in jitters:
            smovjitrows.append(row["rootname"].upper())

        # Close connection
        engine.dispose()
        return nullrows, asnrows, smovjitrows

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def connect_dadsops():
    '''
    Connect to the MAST database on HARPO and store lists of all files.

    Parameters:
    -----------
        None

    Returns:
    --------
        jitdict : dictionary
            Dictionary where the keys are rootnames of jitter files and the
            values are the corresponding proposal IDs.
        sciencedict : dictionary
            Dictionary where the keys are rootnames of all COS files and the
            values are the corresponding proposal IDs.
        ccidict : dictionary
            Dictionary where the keys are rootnames of CCI files and the
            values are the corresponding proposal IDs.
    '''

    # Open the configuration file for the MAST database connection (TSQL).
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

    # Get all jitter, science (ASN), and CCI datasets.
    print("Querying MAST dadsops_rep database....")
    cci_query = "SELECT ads_data_set_name,ads_pep_id FROM " \
    "archive_data_set_all WHERE ads_archive_class='csi'\ngo\n"
    jitter_query = "SELECT ads_data_set_name,ads_pep_id FROM " \
    "archive_data_set_all WHERE ads_instrument='cos' AND " \
    "ads_data_set_name LIKE '%J'\ngo\n"
    science_query = "SELECT sci_data_set_name,sci_pep_id FROM " \
    "science WHERE sci_instrume='cos'\ngo\n"
    
    cci = janky_connect(SETTINGS, cci_query)    
    jitters = janky_connect(SETTINGS, jitter_query)
    science = janky_connect(SETTINGS, science_query)
   
    # Store results as dictionaries (we need dataset name and proposal ID).
    jitdict = OrderedDict()
    sciencedict = OrderedDict()
    ccidict = OrderedDict()
    for row in jitters:
        jitdict[row[0]] = row[1]
    for row in science:
        sciencedict[row[0]] = row[1]
    for row in cci:
        ccidict[row[0]] = row[1]
    
    return jitdict, sciencedict, ccidict
        
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
def janky_connect(SETTINGS, query_string):
    # Connect to the database.
    p = Popen("tsql -S%s -D%s -U%s -P%s -t'|||'" % (SETTINGS["server"], 
              SETTINGS["database"], SETTINGS["username"], 
              SETTINGS["password"]), shell=True, stdin=PIPE, stdout=PIPE,
              stderr=PIPE, close_fds=True)
    (transmit, receive, err) = (p.stdin, p.stdout, p.stderr)
    transmit.write(query_string)
    transmit.close()
    query_result = receive.readlines()
    receive.close()
    error_report = err.readlines()
    err.close()
    
    result = [x.strip().split("|||") for x in query_result if "|||" in x][1:]

    return result
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def compare_tables():
    '''
    Compare the set of all files currently in the COS repository to the list
    all files currently ingested into MAST. Retrieve missing datasets.

    Parameters:
    -----------
        None

    Returns:
    --------
        Nothing
    '''

    nullrows, asnrows, smovjitrows = connect_cosdb()
    jitdict, sciencedict, ccidict = connect_dadsops()
    print("Finding missing COS data...")

    existing = set()
    existing_jit = set()

    # Determine which science, jitter, and CCI datasets are missing.
    missing_sci_names = list(set(sciencedict.keys()) - existing)
    missing_jit_names = list(set(jitdict.keys()) - existing_jit)
    missing_cci_names = list(set(ccidict.keys()) - existing)

    # For science and jitter data, determine corresponding proposal ID for
    # each missing dataset name (to place in the correct directory).
    # We don't need the propids for CCIs (NULL)
    missing_sci_props = [sciencedict[i] for i in missing_sci_names]
    missing_jit_props = [jitdict[i] for i in missing_jit_names]

    # Combine science and jitter lists.
    missing_data_names = missing_sci_names + missing_jit_names
    missing_data_props_str = missing_sci_props + missing_jit_props
    missing_data_props = [int(x) for x in missing_data_props_str]

    # Create dictionaries groupoed by proposal ID, it is much easier
    # to retrieve them this way.
    prop_keys = set(missing_data_props)
    prop_vals = [[] for x in xrange(len(prop_keys))]
    prop_dict = dict(zip(prop_keys, prop_vals))
    for i in xrange(len(missing_data_names)):
        prop_dict[missing_data_props[i]].append(missing_data_names[i])

    pkl_file = "filestoretrieve.p"
    pickle.dump(prop_dict, open(pkl_file, "wb"))
    cwd = os.getcwd()
    print("Missing data written to pickle file {0}".format(os.path.join(cwd,pkl_file)))
#    run_all_retrievals(pkl_file)

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

if __name__ == "__main__":
    compare_tables()
