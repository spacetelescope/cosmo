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
        nullsmov : list
            All rootnames of files where ASN_ID = NONE.
        asnsmov : list
            All ASN_IDs for files where ASN_ID is not NONE.
        jitsmov : list
            All rootnames of jitter files.
    '''

    # Open the configuration file for the COS database connection (MYSQL).
    config_file = os.path.join(os.environ['HOME'], "configure.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

        print("Querying COS greendev database....")
        # Connect to the database.
        Session, engine = load_connection(SETTINGS['connection_string'])
        sci_files = list(engine.execute("SELECT DISTINCT rootname FROM files "
                                        "WHERE rootname IS NOT NULL;"))
        cci_files = list(engine.execute("SELECT DISTINCT name FROM files "
                                        "WHERE rootname IS NULL AND " 
                                        "LEFT(name,1)='l';"))
                                        
        # Store SQLAlchemy results as lists
        all_sci = [row["rootname"].upper() for row in sci_files]
        all_cci = [row["name"].strip("_cci.fits.gz").upper() for row in cci_files]
        all_smov = all_sci + all_cci
        
        # Close connection
        engine.dispose()
        return all_smov

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
        jitmast : dictionary
            Dictionary where the keys are rootnames of jitter files and the
            values are the corresponding proposal IDs.
        sciencemast : dictionary
            Dictionary where the keys are rootnames of all COS files and the
            values are the corresponding proposal IDs.
        ccimast : dictionary
            Dictionary where the keys are rootnames of CCI files and the
            values are the corresponding proposal IDs.
    '''

    # Open the configuration file for the MAST database connection (TSQL).
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

    # Get all jitter, science (ASN), and CCI datasets.
    print("Querying MAST dadsops_rep database....")
    query0 = "SELECT ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE ads_instrument='cos'\ngo"
    # Some COS observations don't have ads_instrumnet=cos
    query1 = "SELECT ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE LEN(ads_data_set_name)=9 "\
    "AND ads_data_set_name LIKE 'L%'\ngo"

    all_cos = janky_connect(SETTINGS, query0) 
    all_l = janky_connect(SETTINGS, query1) 
    all_mast_res = all_cos + all_l
    # Store results as dictionaries (we need dataset name and proposal ID).
    all_mast = {row[0]:row[1] for row in all_mast_res if len(row[0]) == 9 or row[0].startswith("L_")}
    
    return all_mast
        
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

    badness = ["locale", "charset", "1>", "affected"]
    result = [x.strip().split("|||") if "|||" in x else x.strip() 
              for x in query_result if not any(y in x for y in badness)]
    # Ensure that nothing was wrong with the query syntax.
    assert (len(error_report) < 3), "Something went wrong in query:{0}".format(error_report[2])
    return result

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def compare_tables(pkl_it):
    '''
    Compare the set of all files currently in the COS repository to the list
    all files currently ingested into MAST. Retrieve missing datasets.

    Parameters:
    -----------
        pkl_it : Boolean
            True if output should be saved to pickle file.

    Returns:
    --------
        Nothing
    '''

    existing = connect_cosdb()
    mast = connect_dadsops()
    print("Finding missing COS data...")

    # Determine which datasets are missing.
    missing_names = list(set(mast.keys()) - set(existing))

    # To retrieve ALL COS data, uncomment the line below.
    missing_names = list(set(mast.keys() ))

    # Create dictionaries groupoed by proposal ID, it is much easier
    # to retrieve them this way.
    # For most data, determine corresponding proposal ID. CCIs and some
    # odd files will have proposal ID = NULL though.
    missing_props = [int(mast[x]) if mast[x] != "NULL" else "CCI" if x.startswith("L_") else mast[x] for x in missing_names]
    prop_keys = set(missing_props)
    prop_vals = [[] for x in xrange(len(prop_keys))]
    prop_dict = dict(zip(prop_keys, prop_vals))
    for i in xrange(len(missing_names)):
        prop_dict[missing_props[i]].append(missing_names[i])

    print("Data missing for {0} programs".format(len(prop_keys)))
    if pkl_it:
        pkl_file = "filestoretrieve.p"
        pickle.dump(prop_dict, open(pkl_file, "wb"))
        cwd = os.getcwd()
        print("Missing data written to pickle file {0}".format(os.path.join(cwd,pkl_file)))
        run_all_retrievals(pkl_file=pkl_file)
    else:
        run_all_retrievals(prop_dict=prop_dict)

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", dest="pkl_it", action="store_true", default=False,
                        help="Save output to pickle file")
    args = parser.parse_args()

    compare_tables(args.pkl_it)
