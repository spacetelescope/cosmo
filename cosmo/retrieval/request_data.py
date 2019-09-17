#! /usr/bin/env python

from __future__ import print_function, absolute_import, division

"""
Contains functions that facilitate the downloading of COS data
via XML requests to MAST.
"""

__author__ = "Jo Taylor"
__date__ = "04-13-2016"
__maintainer__ = "Camellia Magness"
__email__ = "cmagness@stsci.edu"

import argparse
import re
import pickle
import os
import urllib
import string
from datetime import datetime
import time
import stat

from http.client import HTTPSConnection

from .SignStsciRequest import SignStsciRequest
from .. import SETTINGS

BASE_DIR = SETTINGS["filesystem"]["source"]
USERNAME = SETTINGS["retrieval"]["username"]
MAX_RETRIEVAL = 20
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
  </distributionRequest> \n')


# --------------------------------------------------------------------------- #


def run_all_retrievals(prop_dict=None, pkl_file=None):
    """
    Get the proposal IDs of datasets to be retrieved from either the pickle
    file or the dictionary. If there are a large number of programs,
    these will be retrieved in batches. For each batch, once a set number
    have been retrieved, add more to the queue.

    Apparently, this was supposed to handle the case of re-retrieving all
    data by stopping every 100 programs and calibrating and zipping files as it
    went but this part of the code has all been commented out.

    Legacy comment below:

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

    Parameters
    ----------
    prop_dict: dict, default=None
        Dictionary with all data that needs to be requested, keys by PID

    pkl_file : str, default=None
        Name of pickle file with data to be retrieved
    """
    if pkl_file:
        prop_dict = pickle.load(open(pkl_file, "rb"))

    # if there is no pickle file and no prop_dict given, there will be a
    # problem
    prop_dict_keys = prop_dict.keys()

    if len(prop_dict_keys) == 0:
        return

    pstart = 0
    pend = 20  # should be 10
    int_num = 10  # should be 5
    # century = 3000  # should be 50
    all_tracking_ids = []

    # If less than pend programs were requested, do not enter while loop.
    if pend > len(prop_dict_keys):
        for prop in prop_dict_keys:
            # keep passing the tracking ids and accumulating the complete list
            all_tracking_ids = cycle_thru(prop_dict, prop, all_tracking_ids)

        counter = []
        # num_ids = len(all_tracking_ids)
        # sum(counter) will be equal to len(all_tracking_ids) when all data
        # has been delivered
        while sum(counter) < len(all_tracking_ids):
            counter, not_yet_retrieved = check_data_retrieval(all_tracking_ids)

            if sum(counter) < len(all_tracking_ids):
                print(
                    f"{datetime.now()}\n"
                    f"Data not yet delivered for {not_yet_retrieved}. Checking again in 5 minutes"
                )
                time.sleep(350)

    # While the number of processed programs is less than total programs
    while pend < len(prop_dict_keys):

        # If there are more than N*50 programs to be retrieved, stop
        # and calibrate and zip.
        # See note in find_new_cos_data about total COS dataset sizes.
        #        if pend > century:
        #            century += 50 # should be 50
        #            print("Pausing retrieval to calibrate and
        #            zip current data")
        #            work_laboriously(prl, do_chmod)

        # For each proposal (prop) in the current grouping (total number
        # of programs split up for manageability), retrieve data for it.
        for prop in list(prop_dict_keys)[pstart:pend]:
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
            # this is all basically what check_data_retrieval() does

            for tracking_id in all_tracking_ids:
                status, badness = check_id_status(tracking_id)
                counter.append(status)

                if badness:
                    print(
                        f"!" * 70 + "\n" +
                        f"RUH ROH!!! Request {tracking_id} was killed or cannot be connected!"
                    )

                    counter.append(badness)

            not_yet_retrieved = [all_tracking_ids[i] for i in range(len(counter)) if not counter[i]]

            if sum(counter) < (num_ids - int_num):
                print(
                    f"{datetime.now()}\n"
                    f"Data not yet delivered for {not_yet_retrieved}. Checking again in 5 minutes"
                )
                time.sleep(350)

        # When total# - int_num programs have been retrieved, add int_num more
        # to queue.
        else:
            print(f"\nData delivered, adding {int_num} program(s) to queue")
            pstart = pend
            pend += int_num

    # When pend > total # of programs, it does not mean all have been
    # retrieved. Check, and retrieve if so.
    else:
        # how on earth does this condition mean anything, especially since
        # it is unclear how many times pend += int_num
        if (len(prop_dict_keys) - (pend - int_num)) > 0:
            for prop in list(prop_dict_keys)[pend - int_num:]:
                all_tracking_ids = cycle_thru(prop_dict, prop, all_tracking_ids)

    print(f"All data from {len(prop_dict_keys)} programs were successfully delivered.")


def cycle_thru(prop_dict, prop, all_tracking_ids_tmp):  # IN USE
    """
    For a given proposal, determine path to put data. Build and submit an
    xml request to retrieve data for a proposal and keep track of the
    tracking IDs for each submission (each proposal may have multiple
    submissions depending on number of datasets in proposal).

    Parameters
    ----------
    prop_dict : dict
        A dictionary whose keys are proposal IDs and values are dataset names
        for each ID

    prop : int
        The proposal ID for which to retrieve data

    all_tracking_ids_tmp : list
         Running tally of all tracking IDs for all proposals retrieved so far

    Returns
    -------
    all_tracking_ids_tmp : list
        Updated tally of all tracking IDs for all proposals retrieved so far
    """
    prop_dir = os.path.join(BASE_DIR, str(prop))

    if not os.path.exists(prop_dir):
        os.mkdir(prop_dir)

    print(f"Requesting {len(prop_dict[prop])} association(s) for {prop}")

    # retrieve_data() pretty much does everything
    ind_id = retrieve_data(prop_dir, prop_dict[prop])

    for item in ind_id:
        all_tracking_ids_tmp.append(item)

    return all_tracking_ids_tmp


def retrieve_data(dest_dir, datasets):  # IN USE
    """
    For a given list of datasets (theoretically these are all associated
    with a PID, but that is not required to use this function), build and
    submit an xml request to MAST to retrieve them to a specified directory.
    Also keep track of the tracking IDS for these datasets.

    Parameters
    ----------
    dest_dir : str
        The path to the directory to which the data will be downloaded

    datasets : Any
        An array of datasets to be requested

    Returns
    -------
    tracking_ids : list
        The tracking IDs for all submitted datasets
    """
    if isinstance(datasets, str):
        datasets = [datasets]

    elif type(datasets) is not list:
        datasets = datasets.tolist()

    dataset_lists = (datasets[i:i + MAX_RETRIEVAL] for i in range(0, len(datasets), MAX_RETRIEVAL))

    tracking_ids = []

    for item in dataset_lists:
        try:
            xml_string = build_xml_request(dest_dir, item)
            result0 = submit_xml_request(xml_string)
            result = result0.decode("utf-8")
            tmp_id = re.search("(" + USERNAME + "[0-9]{5})", result).group()

            if "FAILURE" in result:
                print(f"Request {tmp_id} for program {dest_dir} failed.")

            tracking_ids.append(tmp_id)

        # If you can't get a ID, MAST is most likely down.
        except AttributeError:
            print(
                f"\tSomething is wrong on the MAST side...\n"
                f"\tUnsuccessful request for {dest_dir} {item}\n"
                f"\tIgnoring this request and continuing to next.\n"
            )
            pass

    return tracking_ids


def build_xml_request(dest_dir, datasets):  # IN USE
    """
    Build the xml request string.

    Parameters
    ----------
    dest_dir : str
        The path for the directory in which the data will be downloaded

    datasets : list
        A list of datasets to be requested

    Returns
    -------
    xml_request : str
        The xml request string
    """
    archive_user = USERNAME
    email = USERNAME + "@stsci.edu"
    host = SETTINGS["retrieval"]["host"]
    ftp_user = USERNAME
    ftp_dir = dest_dir
    # Not currently using suffix dependence.
    suffix = "<suffix name=\"*\" />"
    dataset_str = "\n"

    for item in datasets:
        dataset_str = f"{dataset_str}<rootname>{item}</rootname>\n"

    request_str = REQUEST_TEMPLATE.safe_substitute(
        archive_user=archive_user,
        email=email,
        ftp_host=host,
        ftp_dir=ftp_dir,
        ftp_user=ftp_user,
        suffix=suffix,
        datasets=dataset_str
    )

    xml_request = string.Template(request_str)
    xml_request = xml_request.template

    return xml_request


def submit_xml_request(xml_string):  # IN USE
    """
    Submit the xml request to MAST.

    Parameters
    ----------
    xml_string : str
        The xml request string

    Returns
    -------
    response : httplib object
        The xml request submission result
    """
    home = os.environ.get("HOME")
    user = os.environ.get("USER")

    # this is a module written by someone at ST. we don't touch what's
    # inside at all
    signer = SignStsciRequest()
    request_xml_str = signer.signRequest(f"{home}/.ssh/privkey.pem", xml_string)

    # noinspection PyUnresolvedReferences
    params = urllib.parse.urlencode(
        {"dadshost": "dmsops1.stsci.edu", "dadsport": 4703, "mission": "HST", "request": request_xml_str}
    )

    headers = {"Accept": "text/html", "User-Agent": f"{user}PythonScript"}

    req = HTTPSConnection("archive.stsci.edu")
    req.request("POST", "/cgi-bin/dads.cgi", params, headers)
    response = req.getresponse().read()

    return response


def check_data_retrieval(all_tracking_ids):  # IN USE
    """
    Given a list of request IDs, check the retrieval status.

    Parameters:
    -----------
    all_tracking_ids : list
        Running tally of all tracking IDs for all proposals retrieved so far

    Returns:
    --------
    counter : list
        List with boolean retrieval status corresponding to each tracking ID

    not_yet_retrieved : list
        IDs of all requests that have not yet completed
    """
    counter = []

    for tracking_id in all_tracking_ids:
        status, badness = check_id_status(tracking_id)
        counter.append(status)

        if badness:
            print(
                f"!" * 70 + "\n" +
                f"RUH ROH!!! Request {tracking_id} was killed, cannot be connected, or did not yield any data!!!"
            )
            counter.append(badness)

    not_yet_retrieved = [all_tracking_ids[i] for i in range(len(counter)) if not counter[i]]

    return counter, not_yet_retrieved


def check_id_status(tracking_id):  # IN USE
    """
    Check every 15 minutes to see if all submitted datasets have been
    retrieved. Based on code from J. Ely.

    Parameters
    ----------
    tracking_id : str
        A submission ID string

    Returns
    -------
    done : bool
        Boolean specifying if data has been retrieved
    killed : bool
        Boolean specifying if request was killed
    """
    done = False
    killed = False
    status_url = f"http://archive.stsci.edu/cgi-bin/reqstat?reqnum=={tracking_id}"

    # In case connection to URL is down.
    tries = 5

    while tries > 0:
        try:
            urllines0 = urllib.request.urlopen(status_url).readlines()
            urllines = [x.decode("utf-8") for x in urllines0]

        except IOError:
            print(f"Something went wrong connecting to {status_url}.")
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

                if "Request ID" in line and "was not found on the" in line:
                    # e.g. Request ID: jotaylor05221 was not found on the
                    # dmsops1.stsci.edu server
                    killed = True

    return done, killed


# --------------------------------------------------------------------------- #


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("pkl_file",
                        help="Path of pickle file with files to retrieve.")
    parser.add_argument("--prl", dest="prl", action="store_true",
                        default=False, help="Parallelize functions")
    args = parser.parse_args()

    run_all_retrievals(prop_dict=None, pkl_file=args.pkl_file)  # , prl=args.prl)

# --------------------------------------------------------------------------- #
