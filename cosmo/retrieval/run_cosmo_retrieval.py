#! /usr/bin/env python

"""
This module commands all the other modules to run the retrieval process.
There is an entry point set up for retrieve() in the setup for the package
called run_retrieval.
"""

import datetime

from .request_data import run_all_retrievals
from .find_new_cos_data import find_new_cos_data
from .calibrate_data import calibrate_data
from .set_permissions import set_user_permissions, set_grpid


# --------------------------------------------------------------------------- #


def retrieve():
    """
    Main function to run the retrieval process. The steps for this process
    are as follows:
    1. Find the current time and create a pickle file
    2. Open all the permissions of the data directory so the existing files
    (i.e, those have already been retrieved) are accessible to this program
    3. Find what missing data can be copied from the cache and what needs to be
     requested from MAST; copy as much data from the cache as possible
    4. Request and retrieve all the missing data from MAST
    5. Calibrate the newly copied/retrieved data
    6. Set the appropriate ownership for all data--depending on whether
    the data is public or proprietary
    7. Close all the permissions of the data
    """

    # Finding the current time
    now = datetime.datetime.now()

    # Creating a pickle file
    # TODO: do we want to create one single log
    pkl_file = "cosmo_{}.p".format(now.strftime("%Y%m%d_%M%S"))

    # First, change permissions of the base directory so we can modify files.
    set_user_permissions("open", prl=True)

    # Collect all the missing data
    all_missing_data = find_new_cos_data(pkl_it=True, pkl_file=pkl_file)
    # ^^ this probably should return something about if there was any public
    # data copied, otherwise can skip calibration all together

    # Only should request if there is anything missing
    if all_missing_data:
        # Retrieve all the missing data by requesting it from MAST
        run_all_retrievals(prop_dict=all_missing_data, pkl_file=None)

    # Calibrate the new data
    calibrate_data(prl=True)

    # Set the group IDs for ownership based on the proprietary status
    set_grpid(prl=True)

    # Change permissions back to protect data
    set_user_permissions("close", prl=True)


# --------------------------------------------------------------------------- #
