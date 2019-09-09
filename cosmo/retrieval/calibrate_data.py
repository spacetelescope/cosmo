#! /usr/bin/env python

from __future__ import print_function, absolute_import, division

"""
This ia a program designed to calibrate COS rawfiles to create CSUMs.

It also sets permissions and group ids appropriately as well
as zipping up any unzipped files to save space.
"""

__author__ = "Jo Taylor"
__date__ = "04-13-2016"
__maintainer__ = "Camellia Magness"

import datetime
import os
import calcos
import argparse

from .manualabor import (handle_nullfiles, gzip_files, get_unprocessed_data,
                         parallelize, copy_outdirs, remove_outdirs, timefunc,
                         clobber_calcos_csumgz)
from .. import SETTINGS

BASE_DIR = SETTINGS["filesystem"]["source"]
CACHE = SETTINGS["retrieval"]["cache"]
CSUM_DIR = "tmp_out"


# --------------------------------------------------------------------------- #


@timefunc
def calibrate_data(prl=True):  # IN USE
    """
    Run all the functions in the correct order.

    Parameters:
    -----------
    prl : bool
        Switch for running functions in parallel
    """
    # Check for the temporary output directories used during calibration,
    # and delete if present. 
    remove_outdirs()

    # Delete any files with program ID = NULL that are not COS files.
    handle_nullfiles() 
    
    # Zip any unzipped files, if they exist.
    gzip_files(prl)

    # Get list of files that need to be processed.
    to_calibrate = get_unprocessed_data(prl)

    # If there are files to calibrate, create csums for them.
    if to_calibrate:
        print("There are {0} file(s) to calibrate, beginning now.".format(
            len(to_calibrate)))
        if prl:
            parallelize(make_csum, to_calibrate)
        else:
            make_csum(to_calibrate)
    
    # Output csums are put in temporary output directories to avoid overwriting
    # intermediate products. Copy only the csums from these directories into 
    # their parent PID directories.
    copy_outdirs()

    # To be safe, check again for unzipped files, and zip them.
    gzip_files(prl)
        
    # Check for the temporary output directories used during calibration,
    # and delete if present. 
    remove_outdirs()

    print("\nFinished at {0}.".format(datetime.datetime.now()))


def make_csum(unzipped_raws):  # IN USE
    """
    Calibrate raw files to produce csum files.

    Parameters:
    -----------
    unzipped_raws : list or str
        A list or string filenames that are unzipped to be calibrated

    NOTE: this parameter claims it needs to be unzipped but as far as i can
    tell, those files are never unzipped before being passed here
    """
    # creating a wrapper function for calcos that clobbers output files
    run_calcos = clobber_calcos_csumgz(calcos.calcos)
    if isinstance(unzipped_raws, str):
        unzipped_raws = [unzipped_raws]
    for item in unzipped_raws:
        dirname = os.path.dirname(item)
        outdirec = os.path.join(dirname, CSUM_DIR)
        if not os.path.exists(outdirec):
            try:
                os.mkdir(outdirec)
            except FileExistsError:
                pass
        try:
            run_calcos(item, outdir=outdirec, verbosity=0,
                       create_csum_image=True, only_csum=True,
                       compress_csum=False)
        except Exception as e:
            if type(e).__name__ == "IOError" and \
               e.args[0] == "Empty or corrupt FITS file":
                print("="*72 + "\n" + "="*72)
                print("The file is empty or corrupt: {0}".format(item))
                print("Deleting file")
                print("="*72 + "\n" + "="*72)
                os.remove(item)
                pass
            else:
                print(e)
                pass


# --------------------------------------------------------------------------- #


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prl", dest="prl", action="store_true",
                        default=False, help="Parallellize functions")
    args = parser.parse_args()

    calibrate_data(args.prl)


# --------------------------------------------------------------------------- #
