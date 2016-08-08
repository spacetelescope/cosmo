#! /usr/bin/env python

from __future__ import print_function, absolute_import, division

'''
This ia a program designed to calibrate COS rawfiles to create CSUMs.

It also sets permissions and group ids appropriately as well
as zipping up any unzipped files to save space.
'''

__author__ = "Jo Taylor"
__date__ = "04-13-2016"
__maintainer__ = "Jo Taylor"

# Import necessary packages.
import os
import glob
import shutil
import gzip
from astropy.io import fits as pf
import calcos
import smtplib
import multiprocessing as mp
import psutil
import argparse
import math
import time
import pdb
import numpy as np
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from .ProgramGroups import *
from .dec_calcos import clobber_calcos

LINEOUT = "#"*75+"\n"
STAROUT = "*"*75+"\n"

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#@log_function
def unzip_mistakes(zipped):
    '''
    Occasionally, files will get zipped without having csums made.
    This function checks if each raw* file has a csum: if it does
    not, it unzips the file to be calibrated in the make_csum()
    function.

    Parameters:
    -----------
        zipped : list
            A list of all files in the base directory that are zipped

    Returns:
    --------
        Nothing
    '''

    if isinstance(zipped, basestring):
        zipped = [zipped]
    for zfile in zipped:
        rootname = os.path.basename(zfile)[:9]
        dirname = os.path.dirname(zfile)
        existence, donotcal= csum_existence(zfile)
        if existence is False and donotcal is False:
            chmod_recurs(dirname, 0755)
            files_to_unzip = glob.glob(zfile)
            uncompress_files(files_to_unzip)
        else:
            unzip_status = False

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#@log_function
def make_csum(unzipped_raws):
    '''
    Calibrate raw files to produce csum files.

    Parameters:
    -----------
        unzipped_raws : list or string
            A list or string filenames that are unzipped to be calibrated.

    Returns:
    --------
        Nothing
    '''
    run_calcos = clobber_calcos(calcos.calcos)
    #logger.info("Creating CSUM files")
    if isinstance(unzipped_raws, basestring):
        unzipped_raws = [unzipped_raws]
    for item in unzipped_raws:
        existence, donotcal = csum_existence(item)
        if existence is False and donotcal is False:
            dirname = os.path.dirname(item)
            try:
                os.chmod(dirname, 0755)
                os.chmod(item, 0755)
            except SyntaxError:
                os.chmod(dirname, 0o755)
                os.chmod(item, 0o755)
            try:
                run_calcos(item, outdir=dirname, verbosity=0,#2
                           create_csum_image=True, only_csum=True,
                           compress_csum=False)

            except Exception as e:
                print(e)
                #logger.exception("There was an error processing {}:".format(item))
                pass

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#@log_function
def fix_perm(base_dir):
    '''
    Walk through all directories in base directory and change the group ids
    to reflect the type of COS data (calbration, GTO, or GO).
    Group ids can be found by using the grp module, e.g.
    > grp.getgrnam("STSCI\cosstis").gr_gid
    User ids can be found using the pwd module e.g.
    > pwd.getpwnam("jotaylor").pw_uid

    Parameters:
    -----------
        base_dir : string
            The base directory to walk through.

    Returns:
    --------
        Nothing
    '''
    user_id = 5026 # jotaylor's user ID
    for root, dirs, files in os.walk(base_dir):
        # This expects the dirtree to be in the format /blah/blah/blah/12345
        pid = root.split("/")[-1]
        if pid in smov_proposals or pid in calib_proposals:
            grp_id = 6045 # gid for STSCI/cosstis group
        elif pid in gto_proposals:
            grp_id = 65546 # gid for STSCI/cosgto group
        else:
            grp_id = 65545 # gid for STSCI/cosgo group
        os.chown(root, user_id, grp_id)
        for filename in files:
            os.chown(os.path.join(root, filename), user_id, grp_id)

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#@log_function
def chmod_recurs(dirname, perm):
    '''
    Edit permissions on a directory and all files in that directory.

    Parameters:
        dirname : string
            A string of the directory name to edit.
        perm : int
            An integer corresponding to the permission bit settings.

    Returns:
        Nothing
    '''

    #[os.chmod(os.path.join(root, filename)) for root,dirs,files in os.walk(DIRECTORY) for filename in files]
    # The above line works fine, but is confusing to read, and is only
    # marginally faster than than an explicit for loop.
    for root, dirs, files in os.walk(dirname):
        os.chmod(root, perm)
        if files:
            for item in files:
                os.chmod(os.path.join(root, item), perm)

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#@log_function
def csum_existence(filename):
    '''
    Check for the existence of a CSUM for a given input dataset.

    Parameters:
    -----------
        filename : string
            A raw dataset name.

    Returns:
    --------
        existence : bool
            A boolean specifying if csums exist or not.

    '''
    rootname = os.path.basename(filename)[:9]
    dirname = os.path.dirname(filename)
    exptype = pf.getval(filename, "exptype")
    # If getting one header keyword, getval is faster than opening.
    # The more you know.
    if exptype != "ACQ/PEAKD" and exptype != "ACQ/PEAKXD":
        donotcal = False
        csums = glob.glob(os.path.join(dirname,rootname+"*csum*"))
        if not csums:
            existence = False
        else:
            existence = True
    else:
        donotcal = True
        existence = False

    return existence, donotcal

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#@log_function
def compress_files(uz_files):
    '''
    Compress unzipped files and delete original unzipped files.

    Paramters:
    ----------
        uz_files : list or string
            Unzipped file(s) to zip

    Returns:
    --------
        Nothing
    '''

    if isinstance(uz_files, basestring):
        uz_files = [uz_files]
    for uz_item in uz_files:
        z_item = uz_item + ".gz"
        with open(uz_item, "rb") as f_in, gzip.open(z_item, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
            print("Compressing {} -> {}".format(uz_item, z_item))
        if os.path.isfile(z_item):
            os.remove(uz_item)
        else:
            print("Something went terribly wrong zipping {}".format(uz_item))

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#@log_function
def uncompress_files(z_files):
    '''
    Uncompress zipped files and delete original zipped files.

    Paramters:
    ----------
        z_files : list
            A list of zipped files to zip

    Returns:
    --------
        Nothing
    '''

    for z_item in z_files:
        uz_item = z_item.split(".gz")[0]
        with gzip.open(z_item, "rb") as f_in, open(uz_item, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
            print("Uncompressing {} -> {}".format(z_item, uz_item))
        if os.path.isfile(uz_item):
            os.remove(z_item)
        else:
            print("Something went terribly wrong unzipping {}".format(z_item))

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#@log_function
def send_email():
    '''
    Send a confirmation email. Currently not used.

    Parameters:
    -----------
        None

    Returns:
    --------
        Nothing
    '''

    msg = MIMEMultipart()
    msg["Subject"] = "Testing"
    msg["From"] = "jotaylor@stsci.edu"
    msg["To"] = "jotaylor@stsci.edu"
    msg.attach(MIMEText("Hello, the script is finished."))
    msg.attach(MIMEText("Testing line 2."))
    s = smtplib.SMTP("smtp.stsci.edu")
    s.sendmail("jotaylor@stsci.edu",["jotaylor@stsci.edu"], msg.as_string())
    s.quit()

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#@log_function
def parallelize(myfunc, mylist):
    '''
    Parallelize a function. Be a good samaritan and CHECK the current usage
    of resources before determining number of processes to use. If usage
    is too high, wait 10 minutes before checking again. Currently only
    supports usage with functions that act on a list. Will modify for
    future to support nargs.

    Parameters:
    -----------
        myfunc : function
            The function to be parallelized.
        mylist : list
            List to be used with function.

    Returns:
    --------
        Nothing
    '''

    # Percentage of cores to eat up.
    playnice = 0.25
    playmean = 0.40

    # Split list into multiple lists if it's large.
    maxnum = 25
    if len(mylist) > maxnum:
        metalist = [mylist[i:i+maxnum] for i in xrange(0, len(mylist), maxnum)]
    else:
        metalist = [mylist]
    for onelist in metalist:
        # Get load average of system and no. of hyper-threaded CPUs on current system.
        loadavg = os.getloadavg()[0]
        ncores = psutil.cpu_count()
        # If too many cores are being used, wait 10 mins, and reasses.
        while loadavg >= (ncores-1):
            time.sleep(600)
        else:
            avail = ncores - math.ceil(loadavg)
            nprocs = int(np.floor(avail * playnice))
        # If, after rounding, no cores are available, default to 1 to avoid
        # pooling with processes=0.
        if nprocs == 0:
            nrpcos = 1
        pool = mp.Pool(processes=nprocs)
        pool.map(myfunc, onelist)
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def only_one_seg(uz_files):
    '''
    If rawtag_a and rawtag_b both exist in a list of raw files, 
    keep only one of the segments. Keep NUV and acq files as is.

    Parameters:
    -----------
        uz_files : list
            List of all unzipped files.

    Returns:
    --------
        uz_files_1seg : list
            List of all unzipped files with only one segment for a given root
            (if applicable).
    '''
    
    bad_inds = []
    uz_files_1seg = []
    for i in xrange(len(uz_files)):
        if i not in bad_inds:
            if "rawtag.fits" in uz_files[i] or "rawacq.fits" in uz_files[i]: # NUV/acq files
                uz_files_1seg.append(uz_files[i])
                continue
            segs = ["_a.fits", "_b.fits"]
            for j in xrange(len(segs)):
                if segs[j] in uz_files[i]:
                    other_seg = uz_files[i].replace(segs[j], segs[j-1])
                    if other_seg in uz_files:
                        bad_inds.append(uz_files.index(other_seg))
                        uz_files_1seg.append(uz_files[i])
                    else:
                        uz_files_1seg.append(uz_files[i])

    return uz_files_1seg

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def work_laboriously(prl):
    '''
    Run all the functions in the correct order.

    Parameters:
    -----------
        prl : Boolean
            Should functions be parallelized?

    Returns:
    --------
        Nothing
    '''

    base_dir = "/grp/hst/cos2/smov_testing/"
    try:
        chmod_recurs(base_dir, 0755) 
    except SyntaxError:
        chmod_recurs(base_dir, 0o755) 
    # using glob is faster than using os.walk
    zipped = glob.glob(os.path.join(base_dir, "?????", "*raw*gz"))
    if zipped:
        if prl:
            parallelize(unzip_mistakes, zipped)
        else:
            unzip_mistakes(zipped)

    unzipped_raws_ab = glob.glob(os.path.join(base_dir, "?????", "*rawtag*fits")) + \
                   glob.glob(os.path.join(base_dir, "?????", "*rawacq.fits"))
    unzipped_raws = only_one_seg(unzipped_raws_ab)
    if unzipped_raws:
        if prl:
            parallelize(make_csum, unzipped_raws)
        else:
            make_csum(unzipped_raws)

    all_unzipped = glob.glob(os.path.join(base_dir, "?????", "*fits"))
    if all_unzipped:
        if prl:
            parallelize(compress_files, all_unzipped)
        else:
            compress_files(all_unzipped)

    fix_perm(base_dir)
    # permission determined by stat.S_ISVTX | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
    chmod_recurs(base_dir, 872)

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

if __name__ == "__main__":
    parser = argparse.ArgumentParse()
    parser.add_argument("-p", dest="prl", action="store_true",
                        default=False, help="Parallellize functions")
    args = parser.parse_args()
    prl = args.prl
    work_laboriously(prl)
