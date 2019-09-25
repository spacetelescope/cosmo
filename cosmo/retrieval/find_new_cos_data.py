#! /usr/bin/env python
"""
This module compares what datasets are currently in COSMO versus all datasets 
currently archived in MAST. All missing datasets will be requested and 
placed in the appropriate directory in COSMO.
"""

__author__ = "Jo Taylor"
__date__ = "04-13-2016"
__maintainer__ = "Camellia Magness"
__email__ = "cmagness@stsci.edu"

from datetime import datetime as dt
import pickle
import os
import argparse
import glob
import numpy as np
from subprocess import Popen, PIPE
from tqdm import tqdm

from .. import SETTINGS
from .manualabor import parallelize, combine_2dicts, compress_files, timefunc

BASE_DIR = SETTINGS["filesystem"]["source"]
USERNAME = SETTINGS["retrieval"]["username"]


# --------------------------------------------------------------------------- #


@timefunc
def find_new_cos_data(pkl_it, pkl_file):  # IN USE
    """
    Workhorse function, determine what data already exist on disk and
    determine what data need to be requested. Copy any data from local cache if
    possible.

    Parameters:
    -----------
    pkl_it : Bool
        Switch to pickle final dictionary of data to requested from MAST.
    pkl_file : str
        Name of output pickle file.

    Returns:
    --------
    all_missing_data : dictionary
        Dictionary of all datasets that need to be requested from MAST,
        where each key is a PID and the value is a list of all missing
        datasets for that PID.
    """
    print("*" * 72)
    missing_data_priv, missing_data_pub, missing_exts = find_missing_data()

    print(
        f"\t{len(missing_data_priv.keys())} proprietary program(s) missing: {list(missing_data_priv.keys())}\n"
        f"\t{len(missing_data_pub.keys())} public program(s) missing: {list(missing_data_pub.keys())}"
    )

    # if there is public missing data need to copy what we can and add the
    # rest to the list of data to be requested
    if missing_data_pub:
        print("Checking to see if any missing public data is in local cache...")
        missing_data_pub_rem = copy_cache(missing_data_pub, missing_exts)

        # Some nonstandard data isn't stored in the cache (e.g. MMD), so
        # check if any other public data needs to be retrieved.
        if missing_data_pub_rem:
            all_missing_data = missing_data_priv.copy()

            for k, v in missing_data_pub_rem.items():
                if k in all_missing_data:
                    all_missing_data[k] = list(set(all_missing_data[k]) | set(v))

                else:
                    all_missing_data[k] = v

        # if there isn't any public data that can't be copied, the only data
        # to request is private data
        else:
            all_missing_data = missing_data_priv

    # if there is no missing public data at all, the only data to request is
    # private data
    elif missing_data_priv:
        print("All missing data are proprietary.")
        all_missing_data = missing_data_priv

    else:
        print("There are no missing data.")
        all_missing_data = {}

    # Not utilized for the moment, see Issue #22 on github.
    #    ensure_no_pending()

    # should only pickle if there is data to request
    if pkl_it and all_missing_data:
        pickle_missing(all_missing_data, pkl_file)

    return all_missing_data


def find_missing_data():  # IN USE
    """
    Compare the set of all files currently in the COS repository to the list
    all files currently ingested into MAST.

    Returns:
    --------
    missing_data : dictionary
        Dictionary where each key is the proposal, and values are the
        missing data.
    """

    existing, existing_filenames, existing_roots = tally_cs()

    print("Checking to see if there are any missing COS data...")
    # determine any missing extensions from partially retrieved COS data sets
    missing_exts = find_missing_exts(existing, existing_roots)

    # get lists of all the mast data
    mast_priv, mast_pub = get_all_mast_data()

    # determine what missing data is public/proprietary by looking at what
    # exists in COSMO vs what is in MAST
    missing_data_priv = _determine_missing(mast_priv, existing_roots)
    missing_data_pub = _determine_missing(mast_pub, existing_roots)

    return missing_data_priv, missing_data_pub, missing_exts


def tally_cs(mydir=BASE_DIR, uniq_roots=True):  # IN USE
    """
    Tabulate all files in BASE_DIR and return lists of files.

    Parameters:
    -----------
    mydir : str, default=BASE_DIR
        Absolute path to filesystem to tabulate all files from.

    uniq_roots: bool, default=True
        Flag to determine whether or not to only return unique rootnames.
        default is to return unique rootnames only.

    Returns:
    --------
    all_existing: list
        Full path to all files in BASE_DIR

    all_filenames: list
        Basename for all files in BASE_DIR

    unique_rootnames OR all_rootnames: list
        Rootnames for unique files if uniq_roots (default); else all files
    """

    print(f"Checking {mydir} for existing data...")
    all_existing = glob.glob(os.path.join(mydir, "*", "*fits*"))
    all_filenames = [os.path.basename(x) for x in tqdm(all_existing)]

    all_rootnames = [
        x.split("_cci")[0].upper() if "cci" in x else x.split("_")[0].upper()
        for x in tqdm(all_filenames)
    ]

    if uniq_roots:
        unique_rootnames = list(set(all_rootnames))
        return all_existing, all_filenames, unique_rootnames

    else:
        return all_existing, all_filenames, all_rootnames


def find_missing_exts(existing, existing_roots):  # IN USE
    """
    If something causes the code to crash mid-copy, some data products may
    not be copied. The only way to check is this to determine what the
    expected products are and compare that to what is currently in central
    store.

    Parameters:
    -----------
    existing : list
        List of all existing files currently in COSMO, this includes
            path name and filetype
    existing_roots : list
        List of rootnames all existing files currently in COSMO.

    Returns:
    --------
    missing_files : list
        List of files that are missing in COSMO as compared with the MAST
        database. Returns None if there are no files missing.

    """

    # Split query into chunks of 10K to avoid running out of processor
    # resources.
    chunksize = 10000
    chunks = [existing_roots[i:i + chunksize] for i in
              range(0, len(existing_roots), chunksize)]

    missing_files_l = []
    pids = []
    for chunk in tqdm(chunks):
        # noinspection SqlDialectInspection
        query = "SELECT distinct afi_file_name, ads_pep_id" \
                " FROM archive_files, archive_data_set_all WHERE" \
                " ads_data_set_name=afi_data_set_name" \
                " AND ads_best_version='y'" \
                " AND ads_generation_date= afi_generation_date" \
                " AND ads_archive_class=afi_archive_class" \
                " AND ads_archive_class != 'EDT'" \
                " AND ads_data_set_name IN {0}\ngo".format(tuple(chunk))

        # connecting to the MAST database and finding all the files that
        # match the query--that is, all files that are ones we want from
        # COS, EVER
        filenames = janky_connect(query)

        # SQL query from janky_connect will return a list of file names. we
        # need this as a dictionary to be useable
        expected_files_d, bad_files = _sql_to_dict(filenames)

        # sorting the expected files
        expected_files_s = set([row[0] for row in filenames])

        # Discard files with no corresponding PID
        if bad_files:
            for bad_file in bad_files:
                expected_files_s.discard(bad_file)

        # sorting existing files passed to this function
        existing_files_s = set([os.path.basename(x).strip(".gz") for x in existing])

        # diff in expected files (all COS files we want, EVER), and ones we
        # already have (in this chunk)
        missing_files_l_chunk = list(expected_files_s - existing_files_s)

        # add the missing files from this chunk to the full list
        missing_files_l += missing_files_l_chunk

        if len(missing_files_l_chunk) == 0:
            continue

        # collect the PIDs from the chunk
        pids_chunk = [int(expected_files_d[x]) for x in missing_files_l_chunk]

        # add this chunk of PIDs to the full list
        pids += pids_chunk

    if len(missing_files_l) == 0:
        return

    # grouping the missing files into dictionaries by PID for requesting
    missing_files = _group_dict_by_pid(missing_files_l, pids)

    print(
        f"{len(missing_files_l)} single extensions missing for {len(missing_files)} programs that were already "
        f"retrieved- this is probably because COSMO crashed in an earlier run."
    )

    return missing_files


def janky_connect(query_string, database=SETTINGS["retrieval"]["database"]):
    # IN USE
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
    # config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    # with open(config_file, 'r') as f:
    #     SETTINGS = yaml.load(f)
    # if database is not None:
    #     SETTINGS["database"] = database
    # NOTE: HAVE SWITCHED THIS TO READ FROM THE COSMO_CONFIG FILE
    # as far as i can tell, get_pid() is the only active function that uses
    # this with a different database

    # Connect to the MAST database through tsql.
    # Should use shlex, but this strips the correct format of the username
    # for AD login, i.e. stsci\\jotaylor. Instead, do it manually.
    # command_line = "tsql -S{0} -D{1} -U{2} -P{3} -t'|||'".format(SETTINGS[
    # "server"],SETTINGS["database"], SETTINGS["username"], SETTINGS[
    # "password"])
    # args = shlex.split(command_line)
    connection_args = [
        "tsql",
        f"-S{SETTINGS['retrieval']['server']}",
        f"-D{database}",
        "-U{'stsci\\' + SETTINGS['retrieval']['username']}",
        f"-P{SETTINGS['retrieval']['password']}",
        "-t|||"
    ]

    pipe = Popen(connection_args, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    (transmit, receive, err) = (pipe.stdin, pipe.stdout, pipe.stderr)
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
    result = [
        x.strip().split("|||") if "|||" in x else x.strip() for x in query_result if not any(y in x for y in badness)
    ]

    # Ensure that nothing was wrong with the query syntax.
    assert (len(error_report) < 3), f"Something went wrong in query:{error_report[2]}"

    return result


def _sql_to_dict(sql_list, groupbykey=True):  # IN USE
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
    sql_dict = {row[0]: row[1] for row in sql_list if not row[0].startswith("LZ_")}
    badfiles = []

    if groupbykey is True:

        for name in list(sql_dict):
            key = name.split('_')[0].upper()

            if sql_dict[name] == "NULL":

                # Manually identify CCI files
                if name.startswith("lf") or name.startswith("ln") or name.startswith("l_"):
                    sql_dict[name] = "CCI"

                elif len(key) == 9:
                    prop = get_pid(key)  # attempt to find the PID

                    if prop is None or prop == 'NULL':
                        badfiles.append(name)

                    else:
                        sql_dict[name] = prop

                else:
                    sql_dict.pop(name, None)

    return sql_dict, badfiles


def get_pid(rootname):  # IN USE
    """
    Given a sql rootname, find the associated PID.

    Parameters
    ----------
    rootname: str
        This string will be in a sql format, presumably

    Returns
    -------
    pid: str
        PID associated with the rootname given
    """
    program_id = rootname[1:4].upper()
    # noinspection SqlDialectInspection
    query = "SELECT DISTINCT proposal_id FROM executed WHERE " \
            "program_id='{0}'\ngo".format(program_id)

    prop = janky_connect(query, database="opus_rep")

    pid = None

    if len(prop) > 0:
        pid = prop[0]

    return pid


def _group_dict_by_pid(filenames, pids):  # IN USE
    """
    This function groups the given files into a dictionary by PID.

    Parameters
    ----------
    filenames: list
        List of filenames
    pids: list
        List of PIDS

    Returns
    -------
    outd: dict
        Dictionary of grouped PIDs and filenames
    """
    # Create dictionaries grouped by proposal ID, it is much easier
    # to retrieve them this way.
    # For most data, determine corresponding proposal ID. CCIs and some
    # odd files will have proposal ID = NULL though.
    keys = set(pids)
    vals = [[] for _ in range(len(keys))]
    outd = dict(zip(keys, vals))

    for i in range(len(filenames)):
        outd[pids[i]].append(filenames[i])

    return outd


def get_all_mast_data():  # IN USE
    """
    Connect to the MAST database on HARPO and store lists of all files.

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

    # noinspection SqlDialectInspection
    query0 = "SELECT distinct ads_data_set_name,ads_pep_id FROM " \
             "archive_data_set_all WHERE ads_instrument='cos' " \
             "AND ads_data_set_name NOT LIKE 'LZ%' AND " \
             "ads_best_version='Y' AND ads_archive_class!='EDT'\ngo"

    # Some COS observations don't have ads_instrument=cos
    # noinspection SqlDialectInspection
    query1 = "SELECT distinct ads_data_set_name,ads_pep_id FROM " \
             "archive_data_set_all WHERE LEN(ads_data_set_name)=9 " \
             "AND ads_data_set_name LIKE 'L%' AND ads_instrument='cos' " \
             "AND ads_best_version='Y' and ads_archive_class!='EDT'\ngo"

    # Now expand on the previous queries by only selecting non-proprietary
    # data
    utc_dt = dt.utcnow()
    utc_str = utc_dt.strftime("%b %d %Y %I:%M:%S%p")

    query0_pub = query0.split("\ngo")[0] + f" and ads_release_date<='{utc_str}'\ngo"
    query1_pub = query1.split("\ngo")[0] + f" and ads_release_date<='{utc_str}'\ngo"

    query0_priv = query0.split("\ngo")[0] + f" and ads_release_date>='{utc_str}'\ngo"
    query1_priv = query1.split("\ngo")[0] + f" and ads_release_date>='{utc_str}'\ngo"

    all_cos_priv = janky_connect(query0_priv)
    all_l_priv = janky_connect(query1_priv)

    # all COS data on MAST that is proprietary
    all_mast_sql_priv = all_cos_priv + all_l_priv

    all_cos_pub = janky_connect(query0_pub)
    all_l_pub = janky_connect(query1_pub)

    # all COS data on MAST that is public
    all_mast_sql_pub = all_cos_pub + all_l_pub

    # converting queries from SQL returned form into dictionaries
    all_mast_priv, _ = _sql_to_dict(all_mast_sql_priv)
    all_mast_pub, _ = _sql_to_dict(all_mast_sql_pub)

    return all_mast_priv, all_mast_pub


def _determine_missing(in_dict, existing_root):  # IN USE
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

    missing_props = [int(in_dict[x]) if in_dict[x] not in ["CCI", "NULL"] else in_dict[x] for x in missing_names]

    missing_data = _group_dict_by_pid(missing_names, missing_props)

    return missing_data


def copy_cache(missing_data, missing_exts=None):  # IN USE
    """
    When there are missing public datasets, check to see if any of them can
    be copied from the hst public cache in central store, which is faster than
    requesting them from MAST.

    Parameters:
    -----------
    missing_data : dictionary
        Dictionary where each key is the proposal, and values are the
        missing data.
    missing_exts : dictionary
        Dictionary where each key is the proposal, and values are
        missing single raw or product files from previous COSMO runs.

    Returns:
    --------
    missing_data : dictionary
        Dictionary where each key is the proposal, and values are the
        missing data after copying any available data from the cache.
    """

    cos_cache, cache_filenames, cache_roots = tabulate_cache()

    missing_data, to_copy_root = find_missing_in_cache(missing_data, cache_roots, cos_cache)

    # calling find_missing_in_cache() twice is redundant-- should check for
    # any missing exts first and then just make one call with all the
    # missing data
    if missing_exts:
        # specifically looking for any exts that might be missing from
        # partially retrieved data sets
        print("looking at exts")

        # find just the missing exts in the cache for the partial data sets
        missing_exts, to_copy_exts = find_missing_in_cache(missing_exts, cache_filenames, cos_cache)

        # combining the root and exts lists that need to be copied into a
        # dictionary that is the union of the unique values
        to_copy = combine_2dicts(to_copy_root, to_copy_exts)

        # basically doing the same for all the rootnames associated with
        # missing exts
        missing_ext_roots = {
            k: list(set([dataset[:9].upper() for dataset in v])) for k, v in
            missing_exts.items()
        }

        # combine the rootname dictionaries for the missing data and missing
        # extensions to be requested
        still_missing = combine_2dicts(missing_data, missing_ext_roots)

    else:
        # if there are no missing_exts, don't need to bother combining those
        # dictionaries of rootnames relevant to the missing extensions,
        # so just go ahead with missing full datasets
        to_copy = to_copy_root
        still_missing = missing_data

    if to_copy:
        # if there is data that needs to be copied, copy it from the cache
        # in parallel
        parallelize(copy_from_cache, to_copy)

    # return the missing data set dictionary that needs to be requested
    return still_missing


def tabulate_cache():  # IN USE
    """
    Determine all the data sets that are currently in the COS central store
    cache.

    Returns:
    --------
    cos_cache : array
        The full path of every COS data set in the cache.

    cache_filenames : array
        Full filename of every COS data set in the cache.

    cache_roots : array
        The rootname of every COS data set in the cache.
    """

    print("\tTabulating list of all cache COS datasets (this may take several minutes)...")

    cos_cache = glob.glob(os.path.join(SETTINGS["retrieval"]["cache"], "l*/l*/*fits*"))
    cache_filenames = [os.path.basename(x) for x in cos_cache]
    cache_roots = [x[:9].upper() for x in cache_filenames]

    return np.array(cos_cache), np.array(cache_filenames), np.array(cache_roots)


def find_missing_in_cache(missing_dict, cache_roots, cos_cache):  # IN USE
    """
    Find as much missing data in the cache as possible. This is used to look
    for public missing data only.

    Parameters
    ----------
    missing_dict : dict
        Dictionary of (public) missing data to look for. Keys are PIDs.

    cache_roots : Any
        Array of rootnames of all COS data in cache.

    cos_cache : Any
        Array of full path names for all COS data in cache.

    Returns
    -------
    missing_dict : dict
        Remaining PIDs as keys with list of files from that program that
        were not found in the cache to copy.

    to_copy_d: dict
        PIDs as keys with list of files that are available to copy from the
        cache.
    """
    total_copied = 0
    start_missing = len(missing_dict.keys())  # initial number of missing PIDs

    to_copy_d = {}
    for key in list(missing_dict):
        missing_files = missing_dict[key]  # missing files associated with PID
        missing_in_cache = list(set(missing_files) & set(cache_roots))

        # missing files that are in the cache
        if len(missing_in_cache) == 0:  # if none of the missing files are
            # in the cache, continue
            continue

        total_copied += len(missing_in_cache)

        updated_missing = list(set(missing_files) - set(missing_in_cache))
        # update missing files list by removing files that were just
        # determined to be in the cache

        if not updated_missing:
            # if there is no difference in original missing list and new
            # list based on searching for key = PID, this PID is misplaced
            # and therefore is removed from the list of missing data sets
            missing_dict.pop(key, f"Something went terribly wrong, {key} isn't in dictionary")

        else:
            # if there are any remaining missing files associated with key =
            # PID, set those in the missing dictionary
            missing_dict[key] = updated_missing

        # Create a generator where each element is an array with all
        # file types that match each missing data set. Then concatenate all
        # these individual arrays for ease of copying.
        # Joe said this makes sense, so it's ok, right?
        try:
            # collect a tuple of all the files that are available to get from
            # the cache for this key = PID
            to_copy = np.concatenate(tuple((cos_cache[np.where(cache_roots == x)] for x in missing_in_cache)))

        except ValueError:
            print("RUH ROH!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            to_copy = []

        # set tuple associated with each PID that is the list of all
        # available files to copy to that PID
        to_copy_d[key] = to_copy

    # missing PIDs left over
    end_missing = len(missing_dict.keys())

    print(f"\tPreparing to copy {total_copied} total root(s) from cache, {start_missing - end_missing} complete PID(s)")

    return missing_dict, to_copy_d


def copy_from_cache(to_copy):  # IN USE
    """
    This function copies the given files from the cache to the destination
    directory (BASE_DIR) and compresses them.

    Parameters
    ----------
    to_copy: dict
        The dictionary of files to be copied, organized by keys of PID
    """
    for pid, cache_files in to_copy.items():
        dest = os.path.join(BASE_DIR, str(pid))
        print(f"\tCopying {len(cache_files)} file(s) from cache into {dest}")

        if not os.path.isdir(dest):
            os.mkdir(dest)

        # By importing pyfastcopy, shutil performance is automatically
        # enhanced
        compress_dest = dest
        compress_files(cache_files, outdir=compress_dest, remove_orig=False, verbose=False)


def pickle_missing(missing_data, pkl_file=None):  # IN USE
    """
    Pickle the dictionary describing missing data.

    Parameters:
    -----------
    missing_data : dictionary
        Dictionary where the key is the proposal, and values are the
        missing data.

    pkl_file : str, default=None
        Name of output pickle file. Will create one if there isn't one given.
    """
    if not pkl_file:
        pkl_file = "filestoretrieve.p"

    pickle.dump(missing_data, open(pkl_file, "wb"))
    cwd = os.getcwd()

    print(f"Missing data written to pickle file {os.path.join(cwd, pkl_file)}")


def check_proprietary_status(rootnames):  # IN USE
    """
    Given a series of rootnames, sort them by PID and proprietary status:
    65545 for proprietary (gid for STSCI/cosgo group) and 6045 (gid for
    STSCI/cosstis group) for public.

    Parameters:
    -----------
    rootnames : array-like
        Rootnames to query

    Returns:
    --------
    propr_status: list
        List of status of file, whether it is proprietary or public data

    filenames:
        List of files matching rootnames given, corresponding to statuses in
        propr_status, given in sql format
    """
    chunksize = 10000
    chunks = [rootnames[i:i + chunksize] for i in range(0, len(rootnames), chunksize)]
    priv_id = 65545
    pub_id = 6045

    sql_results = []
    for chunk in chunks:
        # noinspection SqlDialectInspection
        query = "SELECT DISTINCT ads_data_set_name, ads_release_date, " \
                "ads_pep_id " \
                "FROM archive_data_set_all " \
                "WHERE ads_best_version='Y' " \
                "AND ads_archive_class IN ('cal', 'asn') " \
                "AND ads_data_set_name IN {}\ngo".format(tuple(chunk))

        results = janky_connect(query)
        sql_results += results

    utc_dt = dt.utcnow()
    # utc_str = utc_dt.strftime("%b %d %Y %I:%M:%S%p")

    propr_status = []
    filenames = []
    for row in sql_results:
        file_dt = dt.strptime(row[1], "%b %d %Y %I:%M:%S:%f%p")

        if file_dt <= utc_dt:
            propr_status.append(pub_id)

        else:
            propr_status.append(priv_id)

        filenames.append(row[0])

    return propr_status, filenames


# --------------------------------------------------------------------------- #


if __name__ == "__main__":
    """
    In the event this module needs to be run individually, it can parse 
    necessary arguments for find_new_cos_data().
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", dest="pkl_it", action="store_true",
                        default=False,
                        help="Save output to pickle file")
    parser.add_argument("--pklfile", dest="pkl_file", default=None,
                        help="Name for output pickle file")
    args = parser.parse_args()

    find_new_cos_data(args.pkl_it, args.pkl_file)

# --------------------------------------------------------------------------- #
