#! /usr/bin/env python
from __future__ import print_function, absolute_import, division

"""
This module compares what datasets are currently in
/smov/cos/Data (by accessing the COS database) versus all datasets currently
archived in MAST. All missing datasets will be requested and placed in
the appropriate directory in /smov/cos/Data.

Use:
    This script is intended to be used in a cron job.
"""

__author__ = "Jo Taylor"
__date__ = "04-13-2016"
__maintainer__ = "Jo Taylor"
__email__ = "jotaylor@stsci.edu"

import datetime
import shlex
import urllib
import time
import pickle
import os
import yaml
import argparse
import glob
import pyfastcopy
import shutil
import numpy as np
from sqlalchemy import text
from subprocess import Popen, PIPE

from ..database.db_tables import load_connection
from .manualabor import work_laboriously, chmod, parallelize, PERM_755, PERM_872
from .retrieval_info import BASE_DIR, CACHE

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def connect_cosdb():
    """
    Connect to the COS database on greendev and get a list of all files.

    Parameters:
    -----------
        None

    Returns:
    --------
        all_smov : list
            All rootnames of all files in the COS greendev database.
    """

    # Open the configuration file for the COS database connection (MYSQL).
    config_file = os.path.join(os.environ['HOME'], "configure.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

        print("Querying COS greendev database for existing data...")
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
    """
    Connect to the MAST database on HARPO and store lists of all files.

    Parameters:
    -----------
        None

    Returns:
    --------
        all_mast_priv : dictionary
            Dictionary where the keys are rootnames of proprietary files and
            the values are the corresponding proposal IDs.
        all_mast_pub : dictionary
            Dictionary where the keys are rootnames of publicly available files
            and the values are the corresponding proposal IDs.
    """

    # Open the configuration file for the MAST database connection (TSQL).
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

    # Get all jitter, science (ASN), and CCI datasets.
    print("Querying MAST dadsops_rep database for all COS data...")
    query0 = "SELECT ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE ads_instrument='cos' "\
    "AND ads_data_set_name NOT LIKE 'LZ%' AND "\
    "ads_best_version='Y' AND ads_archive_class!='EDT'\ngo"
    # Some COS observations don't have ads_instrumnet=cos
    query1 = "SELECT ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE LEN(ads_data_set_name)=9 "\
    "AND ads_data_set_name LIKE 'L%' AND ads_instrument='cos' "\
    "AND ads_best_version='Y' and ads_archive_class!='EDT'\ngo"

    # Now expand on the previous queries by only selecting non-propietary data.
    utc_dt = datetime.datetime.utcnow()
    utc_str = utc_dt.strftime("%b %d %Y %I:%M:%S%p")
    query0_pub = query0.split("\ngo")[0] + " and ads_release_date<='{0}'\ngo".format(utc_str)
    query1_pub = query1.split("\ngo")[0] + " and ads_release_date<='{0}'\ngo".format(utc_str)

    all_cos_priv = janky_connect(SETTINGS, query0) 
    all_l_priv = janky_connect(SETTINGS, query1)
    all_mast_sql_priv = all_cos_priv + all_l_priv
    
    all_cos_pub = janky_connect(SETTINGS, query0_pub) 
    all_l_pub = janky_connect(SETTINGS, query1_pub)
    all_mast_sql_pub = all_cos_pub + all_l_pub
   
    all_mast_priv = _sql_to_dict(SETTINGS, all_mast_sql_priv)
    all_mast_pub = _sql_to_dict(SETTINGS, all_mast_sql_pub)

    return all_mast_priv, all_mast_pub
        
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def _sql_to_dict(SETTINGS, sql_list):
    """
    Store results of SQL query (list) as a dictionary with proposal IDs
    as the keys and (individual) dataset rootname as the values. 

    Parameters:
    -----------
        SETTINGS : dictionary
            The connection settings necessary to connect to a database.
        sql_list : list
            SQL results from janky_connect() stored as a list.

    Returns:
    --------
        sql_dict : dictionary
            Dictionary of SQL results.
    """

    # Store results as dictionaries (we need dataset name and proposal ID).
    # Don't get podfiles (LZ*)
    sql_dict = {row[0]:row[1] for row in sql_list if not row[0].startswith("LZ_")}
    badfiles = []
    for key in list(sql_dict):
        if sql_dict[key] == "NULL":
            if key.startswith("LF") or key.startswith("LN") or key.startswith("L_"):
                sql_dict[key] = "CCI"
            elif len(key) == 9:
                program_id = key[1:4]
                query2 = "SELECT DISTINCT proposal_id FROM executed WHERE "\
                "program_id='{0}'\ngo".format(program_id)
                SETTINGS["database"] = "opus_rep"
                prop = janky_connect(SETTINGS, query2)
                if len(prop) == 0:
                    badfiles.append(key)
                else:
                    sql_dict[key] = prop[0]
            else:
                sql_dict.pop(key, None)

    return sql_dict

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def janky_connect(SETTINGS, query_string):
    """
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

    """
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
    # I can't stop, someone help me.
    # https://imgur.com/xioMcFe (h/t E. Snyder)
    result = [x.strip().split("|||") if "|||" in x else x.strip() 
              for x in query_result if not any(y in x for y in badness)]
    # Ensure that nothing was wrong with the query syntax.
    assert (len(error_report) < 3), "Something went wrong in query:{0}".format(error_report[2])
    
    return result

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def tally_cs():
    """
    For testing purposes, tabulate all files in central store 
    (/grp/hst/cos2/smov_testing/) and request all missing datasets.
    
    Parameters:
    -----------
        None

    Returns:
    --------
        smovfiles : list
            A list of all fits files in BASE_DIR
    """

    print ("Checking {0} for existing data...".format(BASE_DIR))
    allsmov = glob.glob(os.path.join(BASE_DIR, "*", "*fits*"))
    smovfiles = [os.path.basename(x).split("_cci")[0].upper() if "cci" in x 
                 else os.path.basename(x).split("_")[0].upper() 
                 for x in allsmov]
    
    return smovfiles 

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def compare_tables(use_cs):
    """
    Compare the set of all files currently in the COS repository to the list
    all files currently ingested into MAST.

    Parameters:
    -----------
        use_cs : Bool
            Switch to find missing data comparing what is currently on 
            central store, as opposed to using the COSMO database.

    Returns:
    --------
        missing_data : dictionary
            Dictionary where each key is the proposal, and values are the
            missing data.
    """

    if use_cs:
        existing = tally_cs()
    else:
        existing = connect_cosdb()
    mast_priv, mast_pub = connect_dadsops()
    print("Checking to see if any missing COS data...")

    # To retrieve *ALL* COS data, uncomment the line below.
#    missing_names = list(set(mast.keys() ))

    missing_data_priv = _determine_missing(mast_priv, existing)
    missing_data_pub = _determine_missing(mast_pub, existing)

    return missing_data_priv, missing_data_pub

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def _determine_missing(in_dict, existing):
    """
    Given a dictionary describing all (public or proprietary) data and a 
    dictionary describing all the data already downloaded into central store,
    determine what datasets are missing.

    Parameters:
    -----------
        in_dict : dictionary
            For all (public or proprietary) data, a dictionary where each key 
            is any PID and the value is one single dataset for that PID
            (i.e. there are multiple keys for the same PID).
        existing : 
            For all datasets already in central store, a dictionary where each
            key is any PID and the value is one single dataset for that PID
            (i.e. there are multiple keys for the same PID).

    Returns:
    --------
        missing_data : dictionary
            A dictionary of missing data where each key is a PID and the value 
            is all missing datsets for that PID.
    """

    # Determine which datasets are missing.
    missing_names = list(set(in_dict.keys()) - set(existing))
    
    # Create dictionaries grouped by proposal ID, it is much easier
    # to retrieve them this way.
    # For most data, determine corresponding proposal ID. CCIs and some
    # odd files will have proposal ID = NULL though.
    missing_props = [int(in_dict[x]) if in_dict[x] not in ["CCI","NULL"] 
                     else in_dict[x] for x in missing_names]
    prop_keys = set(missing_props)
    prop_vals = [[] for x in range(len(prop_keys))]
    missing_data = dict(zip(prop_keys, prop_vals))
    for i in range(len(missing_names)):
        missing_data[missing_props[i]].append(missing_names[i])
    
    return missing_data

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def pickle_missing(missing_data, pkl_file=None):
    """
    Pickle the dictionary describing missing data.

    Parameters:
    -----------
        missing_data : dictionary
            Dictionary where the key is the proposal, and values are the
            missing data.
        pkl_file : str
            Name of output pickle file.

    Returns:
    --------
        Nothing
    """
   
    print("Data missing for {0} programs".format(len(missing_data.keys())))
    if not pkl_file:
        pkl_file = "filestoretrieve.p"
    pickle.dump(missing_data, open(pkl_file, "wb"))
    cwd = os.getcwd()
    print("Missing data written to pickle file {0}".format(os.path.join(cwd,pkl_file)))

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def check_for_pending():
    """
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
    """

    MYUSER = "jotaylor"
    status_url = "http://archive.stsci.edu/cgi-bin/reqstat?reqnum=={0}".format(MYUSER)
    
    tries = 5
    while tries > 0:
        try:
            urllines0 = urllib.request.urlopen(status_url).readlines()
            urllines = [x.decode("utf-8") for x in urllines0]
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
    """
    Check for any pending archive requests, and if there are any, wait until
    they finish.

    Parameters:
    -----------
        None

    Returns: 
    --------
        Nothing.

    """
    num_requests, badness, status_url = check_for_pending()
    while num_requests > 0:
        print("There are still {0} requests pending from a previous COSMO run, waiting 5 minutes...".format(num_requests))
        assert (badness == False), "Something went wrong during requests, check {0}".format(status_url)
        time.sleep(300)
        num_requests, badness, status_url = check_for_pending()
    else:
        print("All pending requests finished, moving on!")

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def get_cache():
    """
    Determine all the datasets that are currently in the COS central store 
    cache. 
    
    Parameters:
    -----------
        None
    
    Returns:
    --------
        array : array
            The full path of every dataset in the cache.
        array : array
            The rootname of every datset in the cache. 
    """
    
    print("Tabulating list of all cache COS datasets (this may take several minutes)...")
    cos_cache = glob.glob(os.path.join(CACHE, "l*/l*/*fits*"))
    cache_roots = [os.path.basename(x)[:9].upper() for x in cos_cache]

    return np.array(cos_cache), np.array(cache_roots)

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def copy_cache(missing_data, run_labor, prl, do_chmod, testmode):
    """
    When there are missing public datasets, check to see if any of them can
    be copied from the COS cache in central store, which is faster than
    requesting them from MAST. 

    Parameters:
    -----------
        missing_data : dictionary
            Dictionary where each key is the proposal, and values are the
            missing data.
        run_labor : Bool
            Switch to run manualabor if necessary.
        prl : Bool
            Switch to run manualabor in parallel or not.
        do_chmod : Bool
            This will be deleted eventually. Switch to run chmod in manualabor.
    
    Returns:
    --------
        missing_data : dictionary
            Dictionary where each key is the proposal, and values are the
            missing data after copying any available data from the cache.
    """

    roots_to_copy = 0
    total_copied = 0
    start_missing = len(missing_data.keys()) 
    cos_cache, cache_roots = get_cache()

    for key in list(missing_data):
# Assuming an average file size of 170MB, this comes to a total of 5TB for 30,000 datasets.
# It is possible to completely retrieve all raw and product datasets and leave them
# unzipped before running manualabor.
        if roots_to_copy > 3000:
            if run_labor:
                print("Running manualabor now...")
                work_laboriously(prl, do_chmod)
            roots_to_copy = 0
        retrieve_roots = missing_data[key]
        roots_in_cache = [x for x in retrieve_roots if x in cache_roots]
        if roots_in_cache:
            dest = os.path.join(BASE_DIR, str(key))
            if not os.path.isdir(dest):
                os.mkdir(dest)
            roots_to_copy += len(roots_in_cache)
            total_copied += len(roots_in_cache)
            
            # Create a generator where each element is an array with all 
            # file types that match each rootname. Then concatenate all these
            # individual arrays for ease of copying.
            to_copy = np.concatenate(tuple( (cos_cache[np.where(cache_roots == x)] 
                                           for x in roots_in_cache) ))

            # By importing pyfastcopy, shutil performance is automatically
            # enhanced
            for cache_item in to_copy:
                if testmode is False:
                    shutil.copyfile(cache_item, os.path.join(dest, os.path.basename(cache_item)))
            print("Copied {0} items to {1}".format(len(to_copy), dest))

            updated_retrieve_roots = [x for x in retrieve_roots if x not in roots_in_cache]
            if not updated_retrieve_roots:
                missing_data.pop(key, "Something went terribly wrong, {0} isn't in dictionary".format(key))
            else:
                missing_data[key] = updated_retrieve_roots

    end_missing = len(missing_data.keys()) 
    
    print("\nCopied {} total roots from cache, {} complete PIDs".format(
          total_copied, start_missing-end_missing))

    return missing_data

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def copy_entire_cache(cos_cache):
    """
    In development. 
    """
    prop_map = {}
    for item in cos_cache:
        filename = os.path.basename(item)
        ippp = filename[:4]
        
        if not ippp in prop_map.keys():
            hdr0 = pf.getheader(item,0)
            proposid = hdr0["proposid"]
            prop_map[ippp] = proposid
        else:
            proposid = prop_map[ippp]
        
        dest = os.path.join(BASE_DIR, proposid, filename)
        # By importing pyfastcopy, shutil performance is automatically enhanced
        shutil.copyfile(item, dest)

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def find_new_cos_data(pkl_it, pkl_file, use_cs, run_labor, prl, do_chmod,
                      testmode):
    """
    Workhorse function, determine what data already exist on disk/in the 
    greendev DB and determine if data are missing. Copy any data from local
    cache if possible.

    Parameters:
    -----------
        pkl_it : Bool
            Switch to pickle final dictionary of data to requested from MAST.
        pkl_file : str
            Name of output pickle file.
        use_cs : 
            Switch to find missing data comparing what is currently on 
            central store, as opposed to using the COSMO database.
        run_labor : Bool
            Switch to run manualabor if necessary.
         prl : 
            Switch to run manualabor in parallel or not.
        do_chmod : Bool
            This will be deleted eventually. Switch to run chmod in manualabor.
        testmode : Bool
            Used purely for testing, some time-consuming steps will be skipped.

    Returns:
    --------
        all_missing_data : dictionary
            Dictionary of all datasets that need to be requested from MAST, 
            where each key is a PID and the value is a list of all missing 
            datasets for that PID.
    """

    missing_data_priv, missing_data_pub = compare_tables(use_cs)
    print("{} proprietary programs missing\n{} public programs missing".format(
          len(missing_data_priv.keys()), len(missing_data_pub.keys())))
# For right now, this is commented out because the idea is to run find_new_cos_data
# to completion then separately run manualabor
# Is this really necessary? vv
#    if len(missing_data_priv.keys()) == 0 and len(missing_data_pub.keys()) == 0:
#        print("There are no missing datasets...")
#        if run_labor:
#            print("Running manualabor now...")
#            work_laboriously(prl, do_chmod)
    if missing_data_pub:
        print("Some missing data are non-propietary.")
        print("Checking to see if any missing data in local cache...")
        missing_data_pub_rem = copy_cache(missing_data_pub, run_labor, prl, do_chmod,
                                          testmode)
        # Some nonstandard data isn't stored in the cache (e.g. MMD), so
        # check if any other public data needs to be retrieved.
        if missing_data_pub_rem:
            all_missing_data = missing_data_priv.copy()
            for k,v in missing_data_pub_rem.items():
                if k in all_missing_data:
                    all_missing_data[k] = list(set(all_missing_data[k]) |
                                               set(v))
                else:
                    all_missing_data[k] = v
    else:
        print("All missing data are proprietary.")
        all_missing_data = missing_data_priv
        
# Not utilized for the moment, see Issue #22 on github.
#    ensure_no_pending()

    if pkl_it:
        pickle_missing(all_missing_data, pkl_file)

    return all_missing_data
    
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", dest="pkl_it", action="store_true", default=False,
                        help="Save output to pickle file")
    parser.add_argument("--pklfile", dest="pkl_file", default=None,
                        help="Name for output pickle file")
    parser.add_argument("--cs", dest="use_cs", action="store_true",
                        default=False, 
                        help="Find missing data comparing to central store, not DB") 
    parser.add_argument("--labor", dest="run_labor", action="store_false",
                        default=True, 
                        help="Switch to turn off manualabor after retrieving data.") 
    parser.add_argument("--prl", dest="prl", action="store_false",
                        default=True, help="Parallellize functions")
    parser.add_argument("--chmod", dest="do_chmod", action="store_false",
                        default=True, help="Switch to chmod directory or not")
    args = parser.parse_args()

    find_new_cos_data(args.pkl_it, args.pkl_file, args.use_cs, args.run_labor, 
                      args.prl, args.do_chmod, False)
