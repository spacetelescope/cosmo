#! /usr/bin/env python

from __future__ import print_function, absolute_import, division

'''
Contains functions that facilitate the downloading of COS data
via XML requests to MAST.

Use:
    This script is intended to be imported by other modules that
    download COS data.
'''

__author__ = "Jo Taylor"
__date__ = "04-13-2016"
__maintainer__ = "Jo Taylor"
__email__ = "jotaylor@stsci.edu"

import argparse
import re
import pickle
import os
import urllib
import string
from datetime import datetime
import time
import stat
import yaml
try:
    from http.client import HTTPSConnection
except ImportError:
    from httplib import HTTPSConnection

from .manualabor import work_laboriously
from .SignStsciRequest import SignStsciRequest
from .logging_dec import log_function
from .retrieval_info import BASE_DIR, CACHE

MAX_RETRIEVAL = 20
MYUSER = "jotaylor"
PERM_755 = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
REQUEST_TEMPLATE = string.Template('\
<?xml version=\"1.0\"?> \n \
<!DOCTYPE distributionRequest SYSTEM \"http://dmswww.stsci.edu/dtd/sso/distribution.dtd\"> \n \
  <distributionRequest> \n \
    <head> \n \
      <requester userId = \"$archive_user\" email = \"$email\" source = "starview"/> \n \
      <delivery> \n \
        <ftp hostName = \"$ftp_host\" loginName = \"$ftp_user\" directory = \"$ftp_dir\" secure = \"true\" /> \n \
      </delivery> \n \
      <process compression = \"none\"/> \n \
    </head> \n \
    <body> \n \
      <include> \n \
        <select> \n \
          $suffix\n \
        </select> \n \
        $datasets \n \
      </include> \n \
    </body> \n \
  </distributionRequest> \n' )

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

#@log_function
def build_xml_request(dest_dir, datasets):
    '''
    Build the xml request string.

    Parameters:
        dest_dir : string
            The path fo the directory in which the data will be downloaded.
        datasets : list
            A list of datasets to be requested.

    Returns:
        xml_request : string
            The xml request string.
    '''
    archive_user = MYUSER
    email = MYUSER + "@stsci.edu"
    host = "plhstins1.stsci.edu"
    ftp_user = MYUSER
    ftp_dir = dest_dir
    # Not currently using suffix dependence.
    suffix = "<suffix name=\"*\" />"
    dataset_str = "\n"
    for item in datasets:
        dataset_str = "{0}          <rootname>{1}</rootname>\n".format(dataset_str,item)

    request_str = REQUEST_TEMPLATE.safe_substitute(
        archive_user = archive_user,
        email = email,
        ftp_host = host,
        ftp_dir =ftp_dir,
        ftp_user = ftp_user,
        suffix = suffix,
        datasets = dataset_str)
    xml_request = string.Template(request_str)
    xml_request = xml_request.template

    return xml_request

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

#@log_function
def submit_xml_request(xml_file):
    '''
    Submit the xml request to MAST.

    Parameters:
        xml_file : string
            The xml request string.

    Returns:
        response : httplib object
            The xml request submission result.
    '''
    home = os.environ.get("HOME")
    user = os.environ.get("USER")

    signer = SignStsciRequest()
    request_xml_str = signer.signRequest("{0}/.ssh/privkey.pem".format(home),
                                         xml_file)
    params = urllib.parse.urlencode({
        "dadshost": "dmsops1.stsci.edu",
        "dadsport": 4703,
        "mission": "HST",
        "request": request_xml_str})
    headers = {"Accept": "text/html",
               "User-Agent": "{0}PythonScript".format(user)}
    req = HTTPSConnection("archive.stsci.edu")
    req.request("POST", "/cgi-bin/dads.cgi", params, headers)
    response = req.getresponse().read()

    return response

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

#@log_function
def retrieve_data(dest_dir, datasets):
    '''
    For a given list of datasets, submit an xml request to MAST
    to retrieve them to a specified directory.

    Parameters:
        dest_dir : string
            The path to the directory to which the data will be downloaded.
        datasets : list
            A list of datasets to be requested.

    Returns:
        tracking_ids : list
            The tracking IDs for all submitted for a given program.
    '''
    dataset_lists = (datasets[i:i+MAX_RETRIEVAL]
                     for i in range(0, len(datasets), MAX_RETRIEVAL))
    tracking_ids = []
    for item in dataset_lists:
        try:
            xml_file = build_xml_request(dest_dir, item)
            result0 = submit_xml_request(xml_file)
            result = result0.decode("utf-8")
            tmp_id = re.search("("+MYUSER+"[0-9]{5})", result).group()
            tracking_ids.append(tmp_id)
        # If you can't get a ID, MAST is most likely down. 
        except AttributeError:
            print("Something is wrong on the MAST side...")
            print("Unsuccessful request for {0}".format(item))
            print("Continuing to next dataset.")
            pass 
    
    return tracking_ids

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

#@log_function
def check_id_status(tracking_id):
    '''
    Check every 15 minutes to see if all submitted datasets have been
    retrieved. Based on code from J. Ely.

    Parameters:
        tracking_id : string
            A submission ID string..

    Returns:
        done : bool
            Boolean specifying is data is retrieved or not.
        killed : bool
            Boolean specifying is request was killed.
    '''

    done = False
    killed = False
#    print tracking_id
    status_url = "http://archive.stsci.edu/cgi-bin/reqstat?reqnum=={0}".format(tracking_id)    
    # In case connection to URL is down.
    tries = 5
    while tries > 0:
        try:
            urllines0 = urllib.request.urlopen(status_url).readlines()
            urllines = [x.decode("utf-8") for x in urllines0]
        except IOError:
            print("Something went wrong connecting to {0}.".format(status_url))
            tries -= 1
            time.sleep(30)
            killed = True
        else:
            tries = -100
            for line in urllines:
                if "State" in line:
                    if "COMPLETE" in line:
                        done = True
                        killed = False
                    elif "KILLED" in line:
                        killed = True

    return done, killed

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

#@log_function
def cycle_thru(prop_dict, prop, all_tracking_ids_tmp):
    '''
    For a given proposal, determine path to put data.
    Retrieve data for a proposal and keep track of the tracking IDs
    for each submission (each proposal may have multiple submissions
    depending on number of datasets in proposal).

    Parameters:
        prop_dict : dictionary
            A dictionary whose keys are proposal IDs and keys are
            dataset names for each ID.
        prop : int
            The proposal ID for which to retrieve data.
        all_tracking_ids_tmp : list
            Running tally of all tracking IDs for all propsals retrieved
            to date. 

    Returns:
        all_tracking_ids_tmp : list
            Running tally of all tracking IDs for all propsals retrieved
            to date.
    '''

    prop_dir = os.path.join(BASE_DIR, str(prop))
    if not os.path.exists(prop_dir):
        os.chmod(BASE_DIR, PERM_755)
        os.mkdir(prop_dir)
    os.chmod(prop_dir, PERM_755)
    print("I am retrieving {0} dataset(s) for {1}".format(len(prop_dict[prop]),prop))
    ind_id = retrieve_data(prop_dir, prop_dict[prop])
    for item in ind_id:
        all_tracking_ids_tmp.append(item)

    return all_tracking_ids_tmp

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def check_data_retrieval(all_tracking_ids):
    '''
    Given a list of request IDs, check the retrieval status.

    Parameters:
    -----------
        all_tracking_ids : list
            Running tally of all tracking IDs for all propsals retrieved
            to date.
    
    Returns:
    --------
        counter : list
            Each value in the list describes the completion status of a request. 
        not_yet_retrieved : list
            IDs of all requests that have not yet completed. 
    '''

    counter = []
    for tracking_id in all_tracking_ids:
        status,badness = check_id_status(tracking_id)
        counter.append(status)
        if badness:
            print("!"*70)
            print("RUH ROH!!! Request {0} was killed or cannot be connected!".format(tracking_id))
            counter.append(badness)
    not_yet_retrieved = [all_tracking_ids[i] for i in 
                         range(len(counter)) if not counter[i]]

    return counter, not_yet_retrieved

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

#@log_function
def run_all_retrievals(prop_dict=None, pkl_file=None, prl=True, do_chmod=False):
    '''
    Open the pickle file containing a dictionary of all missing COS data
    to be retrieved. It is set up to handle all situations (if run daily=few
    programs, if re-retrieving all data=hundreds of programs). If there
    are more than 100 programs to be retrieved, stop every 100 programs and
    run manualabor to calibrate and zip files. If this is not done, you will
    QUICKLY run out of storage when running manualabor later.
    Split the total number of programs into groups, once a defined number
    of programs have been retrieved (int_num), add more to the queue.
    Using this method, there are always pending programs (as opposed to
    retrieving X, waiting until finished, then starting another X).
    For each proposal in a group, use cycle_thru to request all datasets.
    If there are too many datasets, it will split it up for manageability.

    Paramaters:
        pkl_file : string
            Name of pickle file with data to be retrieved.
        prl : Boolean
            Switch for running functions in parallel

    Returns:
        Nothing
    '''

    if pkl_file:
        prop_dict = pickle.load(open(pkl_file, "rb"))
    prop_dict_keys = prop_dict.keys()
    pstart = 0
    pend = 10 # should be 10
    int_num = 5 # should be 5
    century = 50 # should be 50
    all_tracking_ids = []
    end_msg = "\nAll data from {0} programs were successfully "
    
    # If less than pend programs were requested, do not enter while loop.
    if pend > len(prop_dict_keys):
        for prop in prop_dict_keys:
            all_tracking_ids = cycle_thru(prop_dict, prop, all_tracking_ids)
        counter = []
        num_ids = len(all_tracking_ids)
        while sum(counter) < len(all_tracking_ids):
            counter,not_yet_retrieved = check_data_retrieval(all_tracking_ids)
            if sum(counter) < len(all_tracking_ids):
                print(datetime.now())
                print("Data not yet delivered for {0}. Checking again in " 
                      "5 minutes".format(not_yet_retrieved))
                time.sleep(350)
        else:
            print(end_msg.format(len(prop_dict_keys)))
    
    # While the number of processed programs is less than total programs
    while pend < len(prop_dict_keys): 
        
        # If there are more than N*50 programs to be retrieved, stop 
        # and calibrate and zip.
        if pend > century:
            century += 50 # should be 50
            print("Pausing retrieval to calibrate and zip current data")
            work_laboriously(prl, do_chmod)
        # For each proposal (prop) in the current grouping (total number
        # of programs split up for manageability), retrieve data for it.
        for prop in prop_dict_keys[pstart:pend]:
            all_tracking_ids = cycle_thru(prop_dict, prop, all_tracking_ids)
        # This while loop keeps track of how many submitted programs in the 
        # current group (defined as from pstart:pend) have been entirely
        # retrieved, stored in counter. Once the number of finished
        # programs reaches (total# - int_num), another int_num of programs
        # are added to the queue. While data not retrieved, wait 5 minutes
        # before checking again.
        counter = []
        num_ids = len(all_tracking_ids)
        while sum(counter) < (num_ids - int_num):
            counter = []
            for tracking_id in all_tracking_ids:
                status,badness = check_id_status(tracking_id)
                counter.append(status)
                if badness:
                    print("!"*70)
                    print("RUH ROH!!! Request {0} was killed or cannot be connected!".format(tracking_id))
                    counter.append(badness)
            not_yet_retrieved = [all_tracking_ids[i] for i in 
                                 range(len(counter)) if not counter[i]]
            if sum(counter) < (num_ids - int_num):
                print(datetime.now())
                print("Data not yet delivered for {0}. Checking again in " 
                      "5 minutes".format(not_yet_retrieved))
                time.sleep(350)
        # When total# - int_num programs have been retrieved, add int_num more
        # to queue.
        else:
            print("\nData delivered, adding {0} program(s) to queue".
                  format(int_num))
            pstart = pend
            pend += int_num
    # When pend > total # of programs, it does not mean all have been
    # retrieved. Check, and retrieve if so.
    else:
        "delivered. ".format(len(prop_dict_keys))
        if (len(prop_dict_keys) - (pend-int_num)) > 0:
            for prop in prop_dict_keys[pend-int_num:]:
                all_tracking_ids = cycle_thru(prop_dict, prop, all_tracking_ids)
            print(end_msg)
        else:
            print(end_msg)

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prl", dest="prl", action="store_true",
                        default=False, help="Parallellize functions")
    parser.add_argument("--chmod", dest="do_chmod", action="store_true",
                        default=True, help="Switch to turn on chmod")
    args = parser.parse_args()
    prl = args.prl
    
    cwd = os.getcwd()
    pkl_file = os.path.join(cwd,"filestoretrieve.p")
    run_all_retrievals(prop_dict=None, pkl_file=pkl_file, prl=args.prl, do_chmod=args.do_chmod)
