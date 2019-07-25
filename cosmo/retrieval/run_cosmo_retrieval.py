#! /usr/bin/env python

import datetime

from .request_data import run_all_retrievals
from .find_new_cos_data import find_new_cos_data
from .calibrate_data import calibrate_data
from .set_permissions import set_user_permissions, set_grpid


# --------------------------------------------------------------------------- #


def retrieve():
    """
    Main function to run the retrieval process.
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

    # Retrieve all the missing data by requesting it from MAST
    run_all_retrievals(prop_dict=all_missing_data, pkl_file=None, prl=True)

    # Calibrate the new data
    calibrate_data(prl=True)

    # Change permissions back to protect data, and change group ID based on
    # proprietary status.
    set_grpid(prl=True)
    set_user_permissions("close", prl=True)
