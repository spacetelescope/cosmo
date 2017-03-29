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
import pyfastcopy
import shutil
from sqlalchemy import text
from subprocess import Popen, PIPE

from ..database.db_tables import load_connection
from .manualabor import work_laboriously, chmod, PERM_755, PERM_872
from .retrieval_info import BASE_DIR, CACHE

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
    for key in list(all_mast):
        if all_mast[key] == "NULL":
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
            A list of all fits files in BASE_DIR
    '''

    print ("Checking {0}...".format(BASE_DIR))
    allsmov = glob.glob(os.path.join(BASE_DIR, "*", "*fits*"))
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
def pickle_missing(prop_dict):
    '''
    Pickle the dictionary describing missing data.

    Parameters:
    -----------
        prop_dict : dictionary
            Dictionary where the key is the proposal, and values are the
            missing data.

    Returns:
    --------
        Nothing
    '''
   
    print("Data missing for {0} programs".format(len(prop_dict.keys())))
    pkl_file = "filestoretrieve.p"
    pickle.dump(prop_dict, open(pkl_file, "wb"))
    cwd = os.getcwd()
    print("Missing data written to pickle file {0}".format(os.path.join(cwd,pkl_file)))

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
        print("There are still {0} requests pending from a previous COSMO run, waiting 5 minutes...".format(num_requests))
        assert (badness == False), "Something went wrong during requests, check {0}".format(status_url)
        time.sleep(300)
        num_requests, badness, status_url = check_for_pending()
    else:
        print("All pending requests finished, moving on!")

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def get_cache():
    print("Tabulating list of all cache COS datasets (this may take several minutes)...")
    cos_cache = glob.glob("/ifs/archive/ops/hst/public/l*/l*/*fits*")
    cache_roots = [os.path.basename(x)[:9].upper() for x in cos_cache]

    return cos_cache, cache_roots

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def copy_cache(prop_dict, prl, do_chmod):
    cos_cache, cache_roots = get_cache()
    print("Checking to see if any missing data in local cache...")
    total_copied = 0
    for key in list(prop_dict):
        if total_copied > 50:
            work_laboriously(prl, do_chmod)
            total_copied = 0
        vals = prop_dict[key]
        to_copy = [ cos_cache[cache_roots.index(x)] for x in vals if x in cache_roots ]
        if to_copy:
            total_copied += len(to_copy)
            dest = os.path.join(BASE_DIR, str(key))
            if do_chmod:
                chmod(dest, PERM_755, None, True)
            for cache_item in to_copy:
                shutil.copyfile(cache_item, 
                                os.path.join(dest, os.path.basename(cache_item)))
            print("Copied {0} items from the cache to {1}".format(len(to_copy),
                  dest))
            new_vals = [x for x in vals if x not in cache_roots]
            if not new_vals:
                prop_dict.pop(key, None)
            else:
                prop_dict[key] = new_vals

    if do_chmod:
        chmod(dest, PERM_872, None, True)
    return prop_dict

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def copy_entire_cache(cos_cache):
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
        shutil.copyfile(item, dest)

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def find_new_cos_data(pkl_it, use_cs, run_labor, prl, do_chmod):
    prop_dict0 = compare_tables(use_cs)
    if len(prop_dict0.keys()) == 0:
        print("There are no missing datasets, running manualabor")
        if run_labor:
            work_laboriously(prl, do_chmod)
    else:
        prop_dict1 = copy_cache(prop_dict0, prl, do_chmod)
        ensure_no_pending()
        if pkl_it:
            pickle_missing(prop_dict1)

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
    parser.add_argument("--prl", dest="prl", action="store_false",
                        default=True, help="Parallellize functions")
    parser.add_argument("--chmod", dest="do_chmod", action="store_false",
                        default=True, help="Switch to chmod directory or not")
    args = parser.parse_args()

    find_new_cos_data(args.pkl_it, args.use_cs, args.run_labor, args.prl, args.do_chmod)
