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

from datetime import datetime as dt
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
from subprocess import Popen, PIPE
from collections import defaultdict
 
from ..database.db_tables import load_connection
from .manualabor import parallelize, combine_2dicts, compress_files, timefunc
from .retrieval_info import BASE_DIR, CACHE

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def connect_cosdb():
    """
    Connect to the COS team's database, cos_cci, on the server greendev to
    determine which COS datasets are currently in the local repository.

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

def get_all_mast_data():
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

    # Get all jitter, science (ASN), and CCI datasets.
    print("Querying MAST databases for all COS data...")
    query0 = "SELECT distinct ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE ads_instrument='cos' "\
    "AND ads_data_set_name NOT LIKE 'LZ%' AND "\
    "ads_best_version='Y' AND ads_archive_class!='EDT'\ngo"
    # Some COS observations don't have ads_instrumnet=cos
    query1 = "SELECT distinct ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE LEN(ads_data_set_name)=9 "\
    "AND ads_data_set_name LIKE 'L%' AND ads_instrument='cos' "\
    "AND ads_best_version='Y' and ads_archive_class!='EDT'\ngo"

    # Now expand on the previous queries by only selecting non-propietary data.
    utc_dt = dt.utcnow()
    utc_str = utc_dt.strftime("%b %d %Y %I:%M:%S%p")
    query0_pub = query0.split("\ngo")[0] + " and ads_release_date<='{0}'\ngo".format(utc_str)
    query1_pub = query1.split("\ngo")[0] + " and ads_release_date<='{0}'\ngo".format(utc_str)

    query0_priv = query0.split("\ngo")[0] + " and ads_release_date>='{0}'\ngo".format(utc_str)
    query1_priv = query1.split("\ngo")[0] + " and ads_release_date>='{0}'\ngo".format(utc_str)

    all_cos_priv = janky_connect(query0_priv) 
    all_l_priv = janky_connect(query1_priv)
    all_mast_sql_priv = all_cos_priv + all_l_priv
    
    all_cos_pub = janky_connect(query0_pub) 
    all_l_pub = janky_connect(query1_pub)
    all_mast_sql_pub = all_cos_pub + all_l_pub
   
    all_mast_priv = _sql_to_dict(all_mast_sql_priv)
    all_mast_pub = _sql_to_dict(all_mast_sql_pub)

    return all_mast_priv, all_mast_pub
        
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def get_pid(rootname):
    
    program_id = rootname[1:4].upper()
    query = "SELECT DISTINCT proposal_id FROM executed WHERE "\
    "program_id='{0}'\ngo".format(program_id)
    prop = janky_connect(query, database="opus_rep")

    if len(prop) == 0:
        return None
    else:
        return prop[0]

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def find_missing_exts(existing, existing_root):
    """
    If something causes the code to crash mid-copy, some data products may
    not be copied. The only way to check is this to determine what the
    expected products are and compare that to what is currently in central
    store.

    Parameters:
    -----------
        existing : list
            List of all existing files currently in COSMO, this includes
            path name and filetype, e.g. 
            /grp/hst/cos2/smov_testing/13974/lcqf15meq_counts.fits
        existing_root : list
            List of rootnames all existing files currently in COSMO.

    Returns:
    --------
        missing_files : list
            
    """

    # Split query into chunks of 10K to avoid running out of processer 
    # resources.
    chunksize= 10000
    chunks = [existing_root[i:i+chunksize] for i in range(0, len(existing_root), chunksize)]
   
    missing_files_l = []
    pids = [] 
    for chunk in chunks:
        query = "SELECT distinct afi_file_name, ads_pep_id"\
        " FROM archive_files, archive_data_set_all WHERE"\
        " ads_data_set_name=afi_data_set_name"\
        " AND ads_best_version='y'"\
        " AND ads_generation_date= afi_generation_date"\
        " AND ads_archive_class=afi_archive_class"\
        " AND ads_archive_class IN ('cal','asn')"\
        " AND ads_data_set_name IN {0}\ngo".format(tuple(chunk))
   
        filenames = janky_connect(query)
        expected_files_d = _sql_to_dict(filenames)
        expected_files_s = set([row[0] for row in filenames])
        existing_files_s = set([os.path.basename(x).strip(".gz") for x in existing])
        missing_files_l_chunk = list(expected_files_s - existing_files_s)
        missing_files_l += missing_files_l_chunk
        if len(missing_files_l_chunk) == 0:
            continue
        pids_chunk = [int(expected_files_d[x]) for x in missing_files_l_chunk]
        pids += pids_chunk

    if len(missing_files_l) == 0:
        return None
    missing_files = _group_dict_by_pid(missing_files_l, pids)
    print("{} single extensions missing for {} programs that were already retrieved- this is probably because COSMO crashed in an earlier run.".format(len(missing_files_l), len(missing_files)))
    return missing_files

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def _sql_to_dict(sql_list, groupbykey=True):
    """
    Store results of SQL query (list) as a dictionary with proposal IDs
    as the keys and (individual) dataset rootname as the values. 

    Parameters:
    -----------
        sql_list : list
            SQL results from janky_connect() stored as a list.
        groupbykey : Bool
            If True, it will sort SQL 0th results by 1st results, e.g. group
            [["ld5301rfq", "14736"], ["ld5301rtq", "14736"]] ->
            {"14736": ["ld5301rfq", "ld5301rtq"]}
            If False, it will simply turn results into dictionary, e.g.
            [["ld5301rfq", "14736"], ["ld5301rtq", "14736"]] ->
            {"ld5301rfq": "14736", "ld5301rtq": "14736"}. 
            Note that, if False, PIDs will not be looked up, so NULL, MMD, CCI,
            etc. programs will not have correct PIDs.

    Returns:
    --------
        sql_dict : dictionary
            Dictionary of SQL results.
    """

    # Store results as dictionaries. Don't get podfiles (LZ*)
    sql_dict = {row[0]:row[1] for row in sql_list if not row[0].startswith("LZ_")}
    
    if groupbykey is True:
        badfiles = []
        for key in list(sql_dict):
            if sql_dict[key] == "NULL":
                if key.startswith("LF") or key.startswith("LN") or key.startswith("L_"):
                    sql_dict[key] = "CCI"
                elif len(key) == 9:
                    prop = get_pid(key)
                    if prop is None:
                        badfiles.append(key)
                    else:
                        sql_dict[key] = prop
                else:
                    sql_dict.pop(key, None)

    return sql_dict

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def janky_connect(query_string, database=None):
    """
    Connecting to the MAST database is near impossible using SQLAlchemy. 
    Instead, connect through a subprocess call.

    Parameters:
    -----------
        query_string : str
            SQL query text.

    Returns:
    --------
        result : list
            List where each index is a list consisting of [rootname, PID]] 

    """

    # Open the configuration file for the MAST database connection (TSQL).
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)
    if database is not None:
        SETTINGS["database"] = database

    # Connect to the MAST database through tsql.
    # Should use shlex, but this strips the correct format of the username
    # for AD login, i.e. stsci\\jotaylor. Instead, do it manually.
    #command_line = "tsql -S{0} -D{1} -U{2} -P{3} -t'|||'".format(SETTINGS["server"],SETTINGS["database"], SETTINGS["username"], SETTINGS["password"]) 
    #args = shlex.split(command_line)
    args = ["tsql",
            "-S{}".format(SETTINGS["server"]),
            "-D{}".format(SETTINGS["database"]),
            "-U{}".format(SETTINGS["username"]),
            "-P{}".format(SETTINGS["password"]),
            "-t|||"]
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

def tally_cs(mydir=BASE_DIR, uniq_roots=True):
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

    print ("Checking {0} for existing data...".format(mydir))
    allsmov = glob.glob(os.path.join(mydir, "*", "*fits*"))
    smovfilenames = [os.path.basename(x) for x in allsmov]
    smovroots = [x.split("_cci")[0].upper() if "cci" in x 
                 else x.split("_")[0].upper() 
                 for x in smovfilenames]
    
    if uniq_roots:
        return allsmov, smovfilenames, list(set(smovroots))
    else:
        return allsmov, smovfilenames, smovroots


#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def find_missing_data(use_cs):
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
        existing, existing_filenames, existing_root = tally_cs()
    else:
        existing_root = connect_cosdb()

    print("Checking to see if there are any missing COS data...")
    missing_exts = find_missing_exts(existing, existing_root)

    mast_priv, mast_pub = get_all_mast_data()

    missing_data_priv = _determine_missing(mast_priv, existing_root)
    missing_data_pub = _determine_missing(mast_pub, existing_root)

    return missing_data_priv, missing_data_pub, missing_exts

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def _determine_missing(in_dict, existing_root):
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
        existing_root : 
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
    missing_names = list(set(in_dict.keys()) - set(existing_root))
    
    missing_props = [int(in_dict[x]) if in_dict[x] not in ["CCI","NULL"] 
                     else in_dict[x] for x in missing_names]

    missing_data = _group_dict_by_pid(missing_names, missing_props)

    return missing_data

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def _group_dict_by_pid(filenames, pids):
    # Create dictionaries grouped by proposal ID, it is much easier
    # to retrieve them this way.
    # For most data, determine corresponding proposal ID. CCIs and some
    # odd files will have proposal ID = NULL though.
    keys = set(pids) 
    vals = [[] for x in range(len(keys))]
    outd = dict(zip(keys, vals))
    for i in range(len(filenames)):
        outd[pids[i]].append(filenames[i])
    
    return outd    

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

@timefunc
def tabulate_cache():
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
    cache_filenames = [os.path.basename(x) for x in cos_cache]
    cache_roots = [x[:9].upper() for x in cache_filenames]

    return np.array(cos_cache), np.array(cache_filenames), np.array(cache_roots)

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

@timefunc
def find_missing_in_cache(missing_dict, cache_a, cos_cache):
    total_copied = 0
    start_missing = len(missing_dict.keys()) 
    to_copy_d = {}
    for key in list(missing_dict):
        missing_files = missing_dict[key]
        missing_in_cache = list(set(missing_files) & set(cache_a))
        if len(missing_in_cache) == 0:
            continue
        total_copied += len(missing_in_cache)
        
        updated_missing = [x for x in missing_files if x not in missing_in_cache]
        updated_missing = list(set(missing_files) - set(missing_in_cache))
        
        if not updated_missing:
            missing_dict.pop(key, "Something went terribly wrong, {0} isn't in dictionary".format(key))
        else:
            missing_dict[key] = updated_missing
    
        # Create a generator where each element is an array with all 
        # file types that match each missing dataset. Then concatenate all these
        # individual arrays for ease of copying.
        # Joe said this makes sense, so it's ok, right?
        try:
            to_copy = np.concatenate(tuple( (cos_cache[np.where(cache_a == x)] 
                                   for x in missing_in_cache) ))
        except ValueError:
            print("RUH ROH!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        to_copy_d[key] = to_copy
        
    end_missing = len(missing_dict.keys()) 
    
    print("\tCopying {} total roots from cache, {} complete PIDs".format(
          total_copied, start_missing-end_missing))

    return missing_dict, to_copy_d
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def copy_from_cache(to_copy):
    for pid, cache_files in to_copy.items():
        dest = os.path.join(BASE_DIR, str(pid))
        print("\tCopying {} files from cache into {}".format(len(cache_files), dest))
        if not os.path.isdir(dest):
            os.mkdir(dest)

        # By importing pyfastcopy, shutil performance is automatically
        # enhanced
        compress_dest = dest
        compress_files(cache_files, outdir=compress_dest, remove_orig=False, 
                       verbose=False)

    return to_copy

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def copy_cache(missing_data, missing_exts=None, prl=True):
    """
    When there are missing public datasets, check to see if any of them can
    be copied from the COS cache in central store, which is faster than
    requesting them from MAST. 

    Parameters:
    -----------
        missing_data : dictionary
            Dictionary where each key is the proposal, and values are the
            missing data.
        missing_exts : dictionary
            Dictionary where each key is the proposal, and values are 
            missing single raw or product files from previous COSMO runs. 
        out_q : multiprocess.Queue object
            If not None, the output of this function will be passed to 
            the Queue object in order to be curated during multiprocessing. 
    
    Returns:
    --------
        missing_data : dictionary
            Dictionary where each key is the proposal, and values are the
            missing data after copying any available data from the cache.
    """

    cos_cache, cache_filenames, cache_roots = tabulate_cache()

    missing_data, to_copy_root = find_missing_in_cache(missing_data, cache_roots, cos_cache)
    
    if missing_exts is not None:
        print("looking at exts")
        missing_exts, to_copy_exts = find_missing_in_cache(missing_exts, cache_filenames, cos_cache)
        to_copy = combine_2dicts(to_copy_root, to_copy_exts)
        missing_ext_roots = {k:list(set([dataset[:9].upper() for dataset in v])) for k,v in missing_exts.items()}
        still_missing = combine_2dicts(missing_data, missing_ext_roots)
    else:
        to_copy = to_copy_root
        still_missing = missing_data

    if to_copy:
        parallelize("smart", "check_usage", copy_from_cache, to_copy)

    return still_missing

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

def check_proprietary_status(rootnames):
    '''
    Given a series of rootnames, sort them by PID and proprietary status:
    65545 for proprietary (gid for STSCI/cosgo group) and 6045 
    (6045 # gid for STSCI/cosstis group) for public.

    Parameters:
    -----------
        rootnames : array-like
            Rootnames to query. 

    Returns:
    --------
        release_dates : dict
            A dictionary whose keys are 65545 for proprietary data, or
            6045 for public data and whose values are dictionaries
            ordered by PID and values are rootnames.
    '''
    chunksize = 10000
    chunks = [rootnames[i:i+chunksize] for i in range(0, len(rootnames), chunksize)]
    priv_id = 65545
    pub_id = 6045

    sql_results = []
    for chunk in chunks:
#        query = "SELECT DISTINCT afi_file_name, ads_release_date, ads_pep_id "\
#        "FROM archive_files,archive_data_set_all "\
#        "WHERE ads_data_set_name=afi_data_set_name "\
#        "AND ads_best_version='Y' "\
#        "AND ads_archive_class IN ('cal', 'asn') "\
#        "AND ads_data_set_name IN {}\ngo".format(tuple(chunk))
        query = "SELECT DISTINCT ads_data_set_name, ads_release_date, ads_pep_id "\
        "FROM archive_data_set_all "\
        "WHERE ads_best_version='Y' "\
        "AND ads_archive_class IN ('cal', 'asn') "\
        "AND ads_data_set_name IN {}\ngo".format(tuple(chunk))

        results = janky_connect(query)
        sql_results += results

    print("Query finished.")
    utc_dt = dt.utcnow()
    utc_str = utc_dt.strftime("%b %d %Y %I:%M:%S%p")
    
    propr_status = []
    filenames = []
    for row in sql_results:
        file_dt = dt.strptime(row[1], "%b %d %Y %I:%M:%S:%f%p")
        if file_dt <= utc_dt:
            propr_status.append(pub_id)
        else:
            propr_status.append(ppriv_idd)
        filenames.append(row[0])
    
    return propr_status, filenames

#    propr_status = {priv_id: [], pub_id: []}
#    for row in sql_results:
#        file_dt = dt.strptime(row[1], "%b %d %Y %I:%M:%S:%f%p")
#        if file_dt <= utc_dt:
#            propr_status[pub_id] += glob.glob(os.path.join(BASE_DIR, row[2], row[0].lower()+"*"))
#        else:
#            propr_status[priv_id] += glob.glob(os.path.join(BASE_DIR, row[2], row[0].lower()+"*"))

#    release_dates = {priv_id: defaultdict(list), pub_id: defaultdict(list)}
#    for row in sql_results:
#        file_dt = dt.strptime(row[1], "%b %d %Y %I:%M:%S:%f%p")
#        if file_dt <= utc_dt:
#            release_dates[pub_id][int(row[2])].append(row[0])
#        else:
#            release_dates[priv_id][int(row[2])].append(row[0]) 
#    return release_dates
    

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def find_new_cos_data(pkl_it, pkl_file, use_cs=False, prl=True):
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
         prl : 
            Switch to run manualabor in parallel or not.

    Returns:
    --------
        all_missing_data : dictionary
            Dictionary of all datasets that need to be requested from MAST, 
            where each key is a PID and the value is a list of all missing 
            datasets for that PID.
    """
    print("*"*72)
    missing_data_priv, missing_data_pub, missing_exts = find_missing_data(use_cs)
    print("\t{} proprietary program(s) missing: {}\n\t{} public program(s) missing: {}".format(
          len(missing_data_priv.keys()), list(missing_data_priv.keys()), 
          len(missing_data_pub.keys()), list(missing_data_pub.keys()) ))
    
    if missing_data_pub:
        print("Checking to see if any missing public data is in local cache...")
        missing_data_pub_rem = copy_cache(missing_data_pub, missing_exts)
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
            all_missing_data = missing_data_priv
    elif missing_data_priv:
        print("All missing data are proprietary.")
        all_missing_data = missing_data_priv
    else:
        print("There are no missing data.")
        all_missing_data = {}
        
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
    parser.add_argument("--prl", dest="prl", action="store_false",
                        default=True, help="Parallellize functions")
    args = parser.parse_args()

    find_new_cos_data(args.pkl_it, args.pkl_file, args.use_cs,
                      args.prl, False)
