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

import shlex
import urllib
import time
import pickle
import os
import yaml
import argparse
import glob
from sqlalchemy import text
from subprocess import Popen, PIPE

from ..database.db_tables import load_connection
from .request_data import run_all_retrievals
from .manualabor import work_laboriously
 
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def connect_cosdb():
    '''
    Connect to the COS database on greendev and get a list of all files.

    Parameters:
    -----------
        None

    Returns:
    --------
        all_smov : list
            All rootnames of all files.
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
        all_mast : dictionary
            Dictionary where the keys are rootnames of files and the
            values are the corresponding proposal IDs.
    '''

    # Open the configuration file for the MAST database connection (TSQL).
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

    # Get all jitter, science (ASN), and CCI datasets.
    print("Querying MAST dadsops_rep database....")
    query0 = "SELECT ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE ads_instrument='cos' "\
    "AND ads_data_set_name NOT LIKE 'LZ%' AND "\
    "ads_best_version='Y'\ngo"
    # Some COS observations don't have ads_instrumnet=cos
    query1 = "SELECT ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE LEN(ads_data_set_name)=9 "\
    "AND ads_data_set_name LIKE 'L%' AND ads_instrument='cos' "\
    "AND ads_best_version='Y'\ngo"

    all_cos = janky_connect(SETTINGS, query0) 
    all_l = janky_connect(SETTINGS, query1)
    all_mast_res = all_cos + all_l
    
    # Store results as dictionaries (we need dataset name and proposal ID).
    # Don't get Podfiles (LZ*)
    all_mast = {row[0]:row[1] for row in all_mast_res if not row[0].startswith("LZ_")}
    badfiles = []
    for key,val in all_mast.items():
        if val == "NULL":
            if key.startswith("LF") or key.startswith("LN") or key.startswith("L_"):
                all_mast[key] = "CCI"
            elif len(key) == 9:
                program_id = key[1:4]
                query2 = "SELECT DISTINCT proposal_id FROM executed WHERE "\
                "program_id='{0}'\ngo".format(program_id)
                SETTINGS["database"] = "opus_rep"
                prop = janky_connect(SETTINGS, query2)
                if len(prop) == 0:
                    badfiles.append(key)
                else:
                    all_mast[key] = prop[0]
            else:
                all_mast.pop(key, None)

    return all_mast
        
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def janky_connect(SETTINGS, query_string):
    '''
    Connecting to the MAST database is near impossible using SQLAlchemy. 
    Instead, connect through a subprocess call.

    Parameters:
    -----------
        SETTINGS : dictionary
            The connection settings necessary to connect to a database.
        query_string : str
            SQL query text.

    Returns:
    --------
        result : list
            List where each index is a list consisting of [rootname, PID]] 

    '''
    # Connect to the MAST database through tsql.
    command_line = "tsql -S{0} -D{1} -U{2} -P{3} -t'|||'".format(SETTINGS["server"],SETTINGS["database"], SETTINGS["username"], SETTINGS["password"]) 
    args = shlex.split(command_line)
    p = Popen(args,
              stdin=PIPE, 
              stdout=PIPE,
              stderr=PIPE, 
              close_fds=True)
    (transmit, receive, err) = (p.stdin, p.stdout, p.stderr)
    transmit.write(query_string.encode("utf-8"))
    transmit.close()
    query_result0 = receive.readlines()
    receive.close()
    error_report0 = err.readlines()
    err.close()
    
    query_result = [x.decode("utf-8") for x in query_result0] 
    error_report = [x.decode("utf-8") for x in error_report0] 
         
    badness = ["locale", "charset", "1>", "affected"]
    result = [x.strip().split("|||") if "|||" in x else x.strip() 
              for x in query_result if not any(y in x for y in badness)]
    # Ensure that nothing was wrong with the query syntax.
    assert (len(error_report) < 3), "Something went wrong in query:{0}".format(error_report[2])
    return result

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
def tally_cs():
    '''
    For testing purposes, tabulate all files in central store 
    (/grp/hst/cos2/smov_testing/) and request all missing datasets.
    
    Parameters:
    -----------
        None

    Returns:
    --------
        smovfiles : list
            A list of all fits files in /grp/hst/cos2/smov_testing
    '''

    smovdir = "/grp/hst/cos2/smov_testing"
    print ("Checking {0}...".format(smovdir))
    allsmov = glob.glob(os.path.join(smovdir, "*", "*fits*"))
    smovfiles = [os.path.basename(x).split("_cci")[0].upper() if "cci" in x else os.path.basename(x).split("_")[0].upper() for x in allsmov]
    
    return smovfiles 

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def compare_tables(use_cs):
    '''
    Compare the set of all files currently in the COS repository to the list
    all files currently ingested into MAST.

    Parameters:
    -----------
        use_cs : Bool
            Switch to find missing data comparing what is currently on 
            central store, as opposed to using the COSMO database.

    Returns:
    --------
        prop_dict : dictionary
            Dictionary where the key is the proposal, and values are the
            missing data.
    '''

    ensure_no_pending()

    if use_cs:
        existing = tally_cs()
    else:
        existing = connect_cosdb()
    mast = connect_dadsops()
    print("Finding missing COS data...")

    # Determine which datasets are missing.
    missing_names = list(set(mast.keys()) - set(existing))

    # To retrieve ALL COS data, uncomment the line below.
#    missing_names = list(set(mast.keys() ))

    # Create dictionaries groupoed by proposal ID, it is much easier
    # to retrieve them this way.
    # For most data, determine corresponding proposal ID. CCIs and some
    # odd files will have proposal ID = NULL though.
    missing_props = [int(mast[x]) if mast[x] not in ["CCI","NULL"] else mast[x] for x in missing_names]
    prop_keys = set(missing_props)
    prop_vals = [[] for x in range(len(prop_keys))]
    prop_dict = dict(zip(prop_keys, prop_vals))
    for i in range(len(missing_names)):
        prop_dict[missing_props[i]].append(missing_names[i])
    
    return prop_dict

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
def request_missing(prop_dict, pkl_it, run_labor, prl):
    '''
    Call request_data to Retrieve missing datasets.

    Parameters:
    -----------
        prop_dict : dictionary
            Dictionary where the key is the proposal, and values are the
            missing data.
        pkl_it : Boolean
            True if output should be saved to pickle file.
        run_labor : Boolean
            True if manualabor should be run after retrieving data.
        prl : Boolean
            Switch for running functions in parallel

    Returns:
    --------
        Nothing
    '''
    if len(prop_dict.keys()) == 0:
        print("There are no missing datasets")
        if run_labor:
            work_laboriously(prl)
    else:
        print("Data missing for {0} programs".format(len(prop_dict.keys())))
        if pkl_it:
            pkl_file = "filestoretrieve.p"
            pickle.dump(prop_dict, open(pkl_file, "wb"))
            cwd = os.getcwd()
            print("Missing data written to pickle file {0}".format(os.path.join(cwd,pkl_file)))
            run_all_retrievals(prop_dict=None, pkl_file=pkl_file, run_labor=run_labor, prl=prl)
        else:
            print("Retrieving data now...")
            run_all_retrievals(prop_dict=prop_dict, pkl_file=None, run_labor=run_labor, prl=prl)

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def check_for_pending():
    '''
    Check the appropriate URL and get the relevant information about pending
    requests.

    Parameters:
    -----------
        None

    Returns:
    --------
        num : int
            Number of pending archive requests (can be zero)
        badness : Bool
            True if something went wrong with an archive request 
            (e.g. status=KILLED) 
        status_url : str
            URL to check for requests
    '''

    MYUSER = "jotaylor"
    status_url = "http://archive.stsci.edu/cgi-bin/reqstat?reqnum=={0}".format(MYUSER)
    
    tries = 5
    while tries > 0:
        try:
            urllines = urllib.urlopen(status_url).readlines()
        except IOError:
            print("Something went wrong connecting to {0}.".format(status_url))
            tries -= 1
            time.sleep(30)
            badness = True
        else:
            tries = -100
            for line in urllines:
                mystr = "of these requests are still RUNNING"
                if mystr in line:
                    num_requests = [x.split(mystr) for x in line.split()][0][0]
                    assert (num_requests.isdigit()), "A non-number was found in line {0}!".format(line)
                    num = int(num_requests)
                    badness = False
                    break
                else:
                    badness = True

    return num, badness, status_url

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def ensure_no_pending():
    '''
    Check for any pending archive requests, and if there are any, wait until
    they finish.

    Parameters:
    -----------
        None

    Returns: 
    --------
        Nothing.

    '''
    num_requests, badness, status_url = check_for_pending()
    while num_requests > 0:
        assert (badness == False), "Something went wrong during requests, check {0}".format(status_url)
        print("There are still requests pending from a previous COSMO run, waiting 5 minutes...")
        time.sleep(300)
        num_requests, badness, status_url = check_for_pending()
    else:
        print("All pending requests finished, moving on!")

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", dest="pkl_it", action="store_true", default=False,
                        help="Save output to pickle file")
    parser.add_argument("--cs", dest="use_cs", action="store_true",
                        default=False, 
                        help="Find missing data comparing to central store, not DB") 
    parser.add_argument("--labor", dest="run_labor", action="store_false",
                        default=True, 
                        help="Switch to turn off manualabor after retrieving data.") 
    parser.add_argument("--prl", dest="prl", action="store_true",
                        default=False, help="Parallellize functions")
    args = parser.parse_args()

    prop_dict = compare_tables(args.use_cs)
    request_missing(prop_dict, args.pkl_it, args.run_labor, args.prl)
