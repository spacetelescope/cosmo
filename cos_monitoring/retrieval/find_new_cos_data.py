#! /usr/bin/env python
'''
This is a module that will compare what datasets are currently in
/smov/cos/Data (by accessing the COS database) versus all datasets currently
archived in MAST. All missing datasets will be requested and placed in 
the appropriate directory in /smov/cos/Data.

Authors:
    Jo Taylor, Dec. 2015

Use: 
    This script is intended to be used in a cron job.
'''

from __future__ import print_function, absolute_import, division

import pickle
import os
import yaml
from collections import OrderedDict

from cos_monitoring.database.db_tables import load_connection
# fix path
from request_data import retrieve_data

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def connect_cosdb():
    # Open the configuration file for the COS database connection (MYSQL).
    config_file = os.path.join(os.environ['HOME'], "configure.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)
    
        # Connect to the database.
        Session, engine = load_connection(SETTINGS['connection_string'])
        # All entries that ASN_ID = NULL (should be individual jits, acqs)
        null = engine.execute("SELECT rootname FROM headers 
                              WHERE asn_id IS NULL;")
        # All entries that have ASN_ID defined (will pull science, jit, acq)
        asn = engine.execute("SELECT asn_id FROM headers 
                             WHERE asn_id IS NOT NULL;")

        # Store SQLAlchemy results as lists
        nullrows = []
        asnrows = []
        for row in null:
            nullrows.append(row["rootname"].upper())
        for row in asn:
            asnrows.append(row["asn_id"])
        
        # Close connection
        engine.dispose()
        return nullrows, asnrows

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def connect_dadsops():
    # Open the configuration file for the MAST database connection (TSQL).
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)
    
        # Connect to the database.
        Session, engine = load_connection(SETTINGS['connection_string'])
        engine.execute("use dadsops_rep;")
        # Get all jitter, science (ASN), and CCI datasets.
        jitters = engine.execute("SELECT ads_data_set_name,ads_pep_id 
                                 FROM archive_data_set_all 
                                 WHERE ads_instrument='cos' 
                                 AND ads_data_set_name LIKE '%J';")
        science = engine.execute("SELECT sci_data_set_name,sci_pep_id 
                                 FROM science WHERE sci_instrume='cos';")
        cci = engine.execute("SELECT ads_data_set_name,ads_pep_id 
                             FROM archive_data_set_all 
                             WHERE ads_archive_class='csi';")
       
        # Store SQLAlchemy results as dictionaries (we need dataset name 
        # and proposal ID).
        jitdict = OrderedDict()
        sciencedict = OrderedDict()
        ccidict = OrderedDict()
        for row in jitters:
            jitdict[row["ads_data_set_name"]] = row["ads_pep_id"]
        for row in science:
            sciencedict[row["sci_data_set_name"]] = row["sci_pep_id"]
        for row in cci:
            ccidict[row["ads_data_set_name"]] = row["ads_pep_id"]

        # Close connection
        engine.dispose()
        return jitdict, sciencedict, ccidict

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def compare_tables(nullrows, asnrows, jitdict, sciencedict, ccidict):
    existing = set(nullrows + asnrows)

    # Determine which science, jitter, and CCI datasets are missing.
    missing_sci_names = list(set(sciencedict.keys()) - existing)
    missing_jit_names = list(set(jitdict.keys()) - existing)
    missing_cci_names = list(set(ccidict.keys()) - existing)

    # For science and jitter data, determine corresponding proposal ID for 
    # each missing dataset name (to place in the correct directory).
    # We don't need the propids for CCIs (NULL)
    missing_sci_props = [sciencedict[i] for i in missing_sci_names]
    missing_jit_props = [jitdict[i] for i in missing_jit_names]

    # Combine science and jitter lists.
    missing_data_names = missing_sci_names + missing_jit_names
    missing_data_props = missing_sci_props + missing_jit_props

    # Create dictionaries groupoed by proposal ID, it is much easier
    # to retrieve them this way.
    prop_keys = set(missing_data_props)
    prop_vals = [[] for x in xrange(len(prop_keys))]
    prop_dict = dict(zip(prop_keys, prop_vals))
    for i in xrange(len(missing_data_names)):
        prop_dict[missing_data_props[i]].append(missing_data_names[i])

    # This is for testing, to just retrieve one proposal
#    test_prop = prop_dict.keys()[0]
#    test_dir = "/grp/hst/cos3/smov_testing/retrieval"
#    test_data = prop_dict[prop_dict.keys()[0]]
#    pickle.dump({test_prop:test_data}, open("test.p","wb"))

    # Retrieve data for each proposal.
    for prop in prop_dict.keys():
        prop_dir = "/smov/cos/Data/{0}".format(prop)
        os.chmod(prop_dir, 0755)
#        print("I am retrieving %s datasets for %s" % (len(prop_dict[prop]),prop))
         # uncomment to actually retrieve. Don't want to be accidentally
         # sending requests for 1000s of files for now though.
#        retrieve_data(prop_dir, prop_dict[prop])

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

if __name__ == "__main__":
    # bad to use same names?
    nullrows, asnrows = connect_cosdb()
    jitdict, sciencedict, ccidict = connect_dadsops()
    compare_tables(nullrows, asnrows, jitdict, sciencedict, ccidict)

# There are two types of data that will show up in my query of all MAST data_set_names:
# 1) association data (ending in 0 generally), which will also pick up jitter, 
#   lampflash, and acq data
# 2) single exposure data ending in Q (e.g. dark) 
