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
import sys
import datetime
import os
import glob
import shutil
import gzip
from astropy.io import fits
import calcos
import smtplib
import multiprocessing as mp
from multiprocessing import Queue, Process, Pool
import psutil
import argparse
import math
import time
import numpy as np
import stat
import subprocess
from collections import defaultdict
from functools import partial
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from itertools import islice

from .ProgramGroups import *
from .dec_calcos import clobber_calcos_csumgz
from .hack_chmod import chmod
from .retrieval_info import BASE_DIR, CACHE

PERM_755 = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
PERM_872 = stat.S_ISVTX | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
CSUM_DIR = "tmp_out"

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def timefunc(func):
    '''
    Decorator to time functions. 
    '''

    def wrapper(*args, **kw):
        t1 = datetime.datetime.now()
        result = func(*args, **kw)
        t2 = datetime.datetime.now()

        print("{0} executed in {1}".format(func.__name__, t2-t1))
        
        return result
    return wrapper

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def combine_2dicts(dict1, dict2):
    combined = defaultdict(list)
    for k,v in dict1.items():
        if k in dict2:
            combined[k] += list(set(v) | set(dict2[k]))
        else:
            combined[k] += list(v)
    for k,v in dict2.items():
        if k not in combined:
            combined[k] += list(v)

    return combined

#def combine_2dicts(dict1, dict2):
#    combined = defaultdict(list)
#    incommon = list(set(dict1) & set(dict2))
#    indict1 = list(set(dict1) - set(dict2))
#    indict2 = list(set(dict2) - set(dict1))
#    for k in incommon:
#        combined[k] += list(set(dict1[k] + dict2[k]))
#    for k in indict1:
#        combined[k] += dict1[k]
#    for k in indict2:
#        combined[k] += dict2[k]
#    return combined


#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@timefunc
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

    if isinstance(zipped, str):
        zipped = [zipped]
    for zfile in zipped:
        rootname = os.path.basename(zfile)[:9]
        dirname = os.path.dirname(zfile)
        calibrate, badness= csum_existence(zfile)
        if badness is True:
            print("="*72 + "\n" + "="*72)
            print("The file is empty or corrupt: {0}".format(item))
            print("Deleting file")
            print("="*72 + "\n" + "="*72)
            os.remove(item)
            continue
        if calibrate is True:
            #chmod_recurs(dirname, PERM_755)
            files_to_unzip = glob.glob(zfile)
            uncompress_files(files_to_unzip)

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def needs_processing(zipped):
    if isinstance(zipped, str):
        zipped = [zipped]
    files_to_calibrate = []
    
    for zfile in zipped:
        calibrate, badness= csum_existence(zfile)
        if badness is True:
            print("="*72 + "\n" + "="*72)
            print("The file is empty or corrupt: {0}".format(item))
            print("Deleting file")
            print("="*72 + "\n" + "="*72)
            os.remove(item)
            continue
        if calibrate is True:
            files_to_calibrate.append(zfile)

    return files_to_calibrate

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@timefunc
def chgrp(mydir):
    '''
    Walk through all directories in base directory and change the group ids
    to reflect the type of COS data (calbration, GTO, or GO).
    Group ids can be found by using the grp module, e.g.
    > grp.getgrnam("STSCI\cosstis").gr_gid
    User ids can be found using the pwd module e.g.
    > pwd.getpwnam("jotaylor").pw_uid

    Parameters:
    -----------
        mydir : string
            The base directory to walk through.

    Returns:
    --------
        Nothing
    '''
    
    user_id = 5026 # jotaylor's user ID
    for root, dirs, files in os.walk(mydir):
        # This expects the dirtree to be in the format /blah/blah/blah/12345
        pid = root.split("/")[-1]
        try:
            pid = int(pid)
        except ValueError:
            continue
        try:
            if pid in smov_proposals or pid in calib_proposals:
                grp_id = 6045 # gid for STSCI/cosstis group
            elif pid in gto_proposals:
                grp_id = 65546 # gid for STSCI/cosgto group
            else:
                grp_id = 65545 # gid for STSCI/cosgo group
            os.chown(root, user_id, grp_id)
        except PermissionError as e:
            print(repr(e))

            for filename in files:
                try:
                    os.chown(os.path.join(root, filename), user_id, grp_id)
                except PermissionError as e:
                    nothing = True
                    print(repr(e))

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def chmod_recurs(dirname, perm):
    '''
    Edit permissions on a directory and all files in that directory.

    Parameters:
        dirname : string
            A string of the directory name to edit.
        perm : int (octal)
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

def chmod_recurs_prl(perm, item):
    '''
    Edit permissions in parallel.
    
    Parameters:
        perm : int (octal)
            An integer corresponding to the permission bit settings.
        item : string
            A string of the directory/file name to edit.

    Returns:
        Nothing
    '''
    
    os.chmod(item, perm)

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@timefunc
def chmod_recurs_sp(rootdir, perm):
    '''
    Edit permissions on a directory and all files in that directory.

    Parameters:
        perm : int (octal)
            An integer corresponding to the permission bit settings.
        rootdir : string
            A string of the directory name to edit.

    Returns:
        Nothing
    '''

    subprocess.check_call(["chmod", "-R", perm, rootdir])
     
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def csum_existence(filename):
    '''
    Check for the existence of a CSUM for a given input dataset.

    Parameters:
    -----------
        filename : string
            A raw dataset name.

    Returns:
    --------
        calibrate : bool
            A boolean, True if the dataset should not be calibrated to create csums. 
        badness : bool
            A boolean, True if the dataset should be deleted (if corrupt or empty) 
    '''

    rootname = os.path.basename(filename)[:9]
    dirname = os.path.dirname(filename)
    # If getting one header keyword, getval is faster than opening.
    # The more you know.
    try:
        exptime = fits.getval(filename, "exptime", 1)
        if exptime == 0:
            return False, False
    except:
        pass

    try:
        exptype = fits.getval(filename, "exptype")
    except KeyError: #EXPTYPE = None
        return False, False 
    except Exception as e:
        if type(e).__name__ == "IOError" and \
           e.args[0] == "Empty or corrupt FITS file":
            return False, True

    bad_exptypes = ["ACQ/PEAKD", "ACQ/PEAKXD", "ACQ/SEARCH"] 
    if exptype not in bad_exptypes: 
        if "_a.fits" in filename:
            csums = glob.glob(os.path.join(dirname, rootname+"*csum_a*"))
        elif "_b.fits" in filename:
            csums = glob.glob(os.path.join(dirname, rootname+"*csum_b*"))
        else:
            csums = glob.glob(os.path.join(dirname, rootname+"*csum*"))
        if not csums:
            calibrate = True
        else:
            calibrate = False
    else:
        calibrate = False

    return calibrate, False

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def compress_files(uz_files, outdir=None, remove_orig=True, verbose=False):
    '''
    Compress unzipped files and delete original unzipped files.

    Paramters:
    ----------
        uz_files : list or string
            Unzipped file(s) to zip
        outdir : list or string
            Directory to place zipped products.

    Returns:
    --------
        Nothing
    '''
    
    if isinstance(uz_files, str):
        uz_files = [uz_files]
    
    if outdir is None:
        outdir = [os.path.dirname(x) for x in uz_files]
    elif isinstance(outdir, str):
        outdir = [outdir for x in uz_files]

    outdir = [os.path.dirname(x) if not os.path.isdir(x) else x for x in outdir]
    
    if len(uz_files) != len(outdir):
        print("ERROR: List uz_files needs to match length of list outdir, {} vs {}, exiting".
              format(len(uz_files), len(outdir)))
        sys.exit()

    for i in range(len(uz_files)):
        z_item = os.path.join(outdir[i], os.path.basename(uz_files[i]) + ".gz")
        with open(uz_files[i], "rb") as f_in, gzip.open(z_item, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
            if verbose is True:
                print("Compressing {} -> {}".format(uz_files[i], z_item))
        if remove_orig is True:
            if os.path.isfile(z_item):
                os.remove(uz_files[i])
            else:
                print("Something went terribly wrong zipping {}".format(uz_files[i]))

    return uz_files

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

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

    if isinstance(z_files, str):
        z_files = [z_files]

    for z_item in z_files:
        uz_item = z_item.split(".gz")[0]
        print("Uncompressing {} -> {}".format(z_item, uz_item))
        with gzip.open(z_item, "rb") as f_in, open(uz_item, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
            print("Uncompressing {} -> {}".format(z_item, uz_item))
        if os.path.isfile(uz_item):
            os.remove(z_item)
        else:
            print("Something went terribly wrong unzipping {}".format(z_item))

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

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

def check_usage(playnice=True):
    """
    Check current system resource usage, and adjust number of requested
    processes accordingly.

    Parameters:
    -----------
        playnice : Bool
            If True, use only 25% of *available* cores. If False use 50%.

    Returns:
    --------
        nprocs : int
            Number of processes to use when multiprocessing.
    """

    # Are we feeling charitable or not?
    if playnice is True:
        core_frac = 0.25
    else:
        core_frac = 0.5 

    # Get load average of system and no. of hyper-threaded CPUs on current system.
    loadavg = os.getloadavg()[0] # number of CPUs in use
    ncores = psutil.cpu_count() # number of CPUs available
    
    # If too many cores are being used, wait 5 mins, and reassess.
    # Check if elapsed time > 15 mins- if so, just use 1 core.
    too_long = 0
    while loadavg >= (ncores-1):
        print("Too many cores in usage, waiting 5 minutes before continuing...")
        time.sleep(300)
        too_long += 300
        if too_long >= 900:
            print("Continuing with one core...")
            nprocs = 1
            break
    else:
        avail = ncores - math.ceil(loadavg)
        nprocs = int(np.floor(avail * core_frac))                       
    
    # If, after rounding, no cores are available, default to 1 to avoid
    # pooling with processes=0.
    if nprocs <= 0:
        nprocs = 1

    return nprocs

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def chunkify_d(d, chunksize):
    it = iter(d)
    chunks = []
    for i in range(0, len(d), chunksize):
        chunks.append({k:d[k] for k in islice(it, chunksize)})
    
    return chunks

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def smart_chunks(nelems, nprocs):
    if nelems <= 10*nprocs:
        chunksize = math.ceil(nelems/nprocs)
    elif 10*nprocs < nelems <= 1000*nprocs:
        chunksize = 10
    else:
        chunksize = 100

    return chunksize

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def parallelize(chunksize, nprocs, func, iterable, *args, **kwargs):
    t1 = datetime.datetime.now()
        
    if len(iterable) == 0:
        return funcout
    
    if nprocs == "check_usage":
        nprocs = check_usage()

    if chunksize == "smart":
        chunksize = smart_chunks(len(iterable), nprocs)
    
    if isinstance(iterable, dict):
        isdict = True 
        chunks = chunkify_d(iterable, chunksize)
    else:
        isdict = False
        chunks = [iterable[i:i+chunksize] for i in range(0, len(iterable), chunksize)]
    
    func_args = [(x,)+args for x in chunks]

    funcout = None

    with Pool(processes=nprocs) as pool:
#        print("Starting the Pool for {} with {} processes...".format(func, nprocs))
        results = [pool.apply_async(func, fargs, kwargs) for fargs in func_args]

        if results[0].get() is not None:
            if isdict:
                funcout = {}
                for d in results:
                    funcout.update(d.get())
            else:
                funcout = [item for innerlist in results for item in innerlist.get()]
    
    t2 = datetime.datetime.now()
    print("parallelize({}) executed in {}".format(func.__name__, t2-t1))

    return funcout

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@timefunc
def parallelize_joe(func, iterable, nprocs, *args, **kwargs):
    if nprocs == None:
        nprocs = check_usage()

    if type(iterable) is dict:
        isdict = True 
        func_args = [ ({k:v},) + args for k,v in iterable.items() ]
    else:
        isdict = False
        func_args = [(iterable,) + args]

    with Pool(processes=nprocs) as pool:
        print("Starting the Pool with {} processes...".format(nprocs))
        results = [pool.apply_async(func, fargs, kwargs) for fargs in func_args]

        if results[0].get() is not None:
            if isdict:
                funcout = {}
                for d in results:
                    funcout.update(d.get())
            else:
                funcout = [item for innerlist in results for item in innerlist.get()]
        else:
            funcout = None

    return funcout

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@timefunc
def parallelize_queue(func, iterable, funcargs=None, nprocs="check_usage", 
                      keep_output=False):
    """
    Run a function in parallel with either multiple processes.

    Parameters:
    -----------
        func : function
            Function to be run in parallel.
        iterable : list, array-like object, or dictionary 
            The series of data that should be processed in parallel with 
            function func.
        funcargs : tuple
            Additional arguments, if any, for the function func.
        nprocs : int
            Number of processes to use during multiprocessing.
        keep_output : Bool
            True if output from function func is to stored and returned.
    
    Returns:
    --------
        resultdict : dictionary
            None if keep_output is False, otherwise a dictionary where each
            key is a single input element from the series iterable, and the
            value is the output from function func for that element. 
    """

    # Handle process usage checks.
    if nprocs == "check_usage":
        playnice = False
        if playnice is True:
            core_frac = 0.25
        else:
            core_frac = 0.5 
        # Get load average of system and no. of hyper-threaded CPUs on current system.
        loadavg = os.getloadavg()[0]
        ncores = psutil.cpu_count()
        # If too many cores are being used, wait 10 mins, and reassess.
        # Check if elapsed time > XX, if so, just use Y cores
        while loadavg >= (ncores-1):
            print("Too many cores in usage, waiting 10 minutes before continuing...")
            time.sleep(600)
        else:
            avail = ncores - math.ceil(loadavg)
            nprocs = int(np.floor(avail * core_frac))                       
        # If, after rounding, no cores are available, default to 1 to avoid
        # pooling with processes=0.
        if nprocs <= 0:
            nprocs = 1

    # If you want the output from the function being parallelized.
    if keep_output:
        out_q = Queue()
    # If there are input arguments (besides iterable) to the function, they must
    # be defined as a tuple for multiprocessing.
    if funcargs is not None:
        if type(funcargs) is not tuple:
            print("ERROR: Arguments for function {} need to be tuple, exiting".
                  format(func))
            sys.exit()

    # Split the iterable up into chunks determined by the number of processes.
    # what if the iterable is less than the number of nprocs?
    chunksize = math.ceil(len(iterable) / nprocs)
    procs = []
    for i in range(nprocs):
        if type(iterable) is dict:
            # try using {k:v for k,v in list(iterable.items())...}
            subset = {k:iterable[k] for k in list(iterable.keys())[chunksize*i:chunksize*(i+1)]}
        else:
            subset = iterable[chunksize*i:chunksize*(i+1)]
        # Multiprocessing requires a tuple as input, so if there are 
        # additional input arguments, concatenate them.
        if funcargs is not None:
            if keep_output:
                inargs = (subset, out_q) + funcargs
            else:
                inargs = (subset,) + funcargs
        else:
            if keep_output:
                inargs = (subset, out_q)
            else:
                inargs = (subset,)
        # Create the process.
        p = Process(target=func, args=inargs)                  
        procs.append(p)
        p.start()

    # Collect the results into a dictionary, if desired.
    if keep_output:
        resultdict = {}
        for i in range(nprocs):
            resultdict.update(out_q.get())
    else:
        resultdict = None

    # Wait for all worker processes to finish.
    for p in procs:
        p.join()
    
    return resultdict

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@timefunc
def parallelize_orig(myfunc, mylist, check_usage=True):
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
    # I don't think this is necessary.
#    maxnum = 25
#    if len(mylist) > maxnum:
#        metalist = [mylist[i:i+maxnum] for i in range(0, len(mylist), maxnum)]
#    else:
#        metalist = [mylist]
#    for onelist in metalist:
    if check_usage:
        # Get load average of system and no. of hyper-threaded CPUs on current system.
        loadavg = os.getloadavg()[0]
        ncores = psutil.cpu_count()
        # If too many cores are being used, wait 10 mins, and reasses.
        while loadavg >= (ncores-1):
            print("Too many cores in usage, waiting 10 minutes before continuing...")
            time.sleep(600)
        else:
            avail = ncores - math.ceil(loadavg)
            nprocs = int(np.floor(avail * playmean))
        # If, after rounding, no cores are available, default to 1 to avoid
        # pooling with processes=0.
        if nprocs == 0:
            nprocs = 1
    else:
        nprocs = 10
    
    print("Starting the Pool with {} processes...".format(nprocs))
    pool = mp.Pool(processes=nprocs)
    pool.map(myfunc, mylist)
    pool.close()
    pool.join()

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@timefunc
def check_disk_space():
    '''
    Determine how much free space there is in BASE_DIR

    Parameters:
    -----------
        None

    Returns:
    --------
#        free_gb : float
#            Number of free GB in BASE_DIR.
    '''
    
    statvfs = os.statvfs(BASE_DIR)
    free_gb = (statvfs.f_frsize * statvfs.f_bfree) / 1e9
    if free_gb < 200:
        print("WARNING: There are {0}GB left on disk".format(free_gb))
        unzipped_csums = glob.glob(os.path.join(BASE_DIR, "*", "*csum*.fits"))
        if not unzipped_csums:
            print("WARNING: Disk space is running very low, no csums to zip") 
        else:
            print("Zipping csums to save space...")
            if prl:
                parallelize("smart", "check_usage", compress_files, unzipped_csums)
            else:
                compress_files(unzipped_csums)

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
    
    roots = [os.path.basename(x)[:9] for x in uz_files]
    uniqinds = []
    uniqroots = []
    for i in range(len(roots)):
        if roots[i] not in uniqroots:
            uniqroots.append(roots[i])
            uniqinds.append(i)
    uz_files_1seg = [uz_files[j] for j in uniqinds]
    
    return uz_files_1seg
    
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def handle_nullfiles():
    '''
    It can be difficult to tell if all files found by find_new_cos_data with
    program ID 'NULL' are actually COS files (e.g. reference files that start
    with 'L' can accidentally be downloaded). Delete any files that are not
    actually COS files

    Parameters:
    -----------
        nullfiles : list
            List of all files in the NULL directory

    Returns:
    --------
        None
    '''

    nullfiles = glob.glob(os.path.join(BASE_DIR, "NULL", "*fits*"))
    if len(nullfiles) == 0:
        return 
    else:
        print("Handling {0} NULL datasets...".format(len(nullfiles)))

    for item in nullfiles:
        with fits.open(item) as hdulist:
            hdr0 = hdulist[0].header
        try:
            instrument = hdr0["instrume"]
            try:
                pid = hdr0["proposid"]
                if pid > 0:
                    pid = str(pid)
                    # These files have program IDs and should be moved.
                    if not os.path.exists(os.path.join(BASE_DIR, pid)):
                        os.mkdir(os.path.join(BASE_DIR, pid)) 
                    shutil.move(item, os.path.join(BASE_DIR, pid))
            except KeyError:
                try:
                    # These files are reference files and should be removed.
                    useafter = hdr0["useafter"]
                    os.remove(item)
                except KeyError:
                    os.remove(item)
        except KeyError:
            # These files are not COS datasets and should be removed.
            os.remove(item)

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def move_files(orig, dest):
    if isinstance(orig, str):
        orig = [orig]
    if isinstance(dest, str):
        dest = [dest]
    for i in range(len(orig)):
        shutil.move(orig[i], dest[i])

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def remove_outdirs():
    tmp_dirs = glob.glob(os.path.join(BASE_DIR, "*", CSUM_DIR))
    if tmp_dirs:
        for bad_dir in tmp_dirs:
            try:
                shutil.rmtree(bad_dir)
            except OSError:
                pass

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def copy_outdirs():
    csums = glob.glob(os.path.join(BASE_DIR, "*", CSUM_DIR, "*csum*"))
    if csums:
        dest_dirs = [os.path.dirname(x).strip(CSUM_DIR) for x in csums]
        move_files(csums, dest_dirs)

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def gzip_files(prl=True):
    # Get a list of all unzipped files and zip them.
    unzipped = glob.glob(os.path.join(BASE_DIR, "*", "*fits"))
    if unzipped:
        print("Zipping {0} unzipped file(s)...".format(len(unzipped)))
        if prl:
            parallelize("smart", "check_usage", compress_files, unzipped)
        else:
            compress_files(unzipped)

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def get_unprocessed_data(prl=True):
    # When calibrating zipped raw files, you need to calibrate both segments 
    # separately since calcos can't find the original 
    zipped_raws = glob.glob(os.path.join(BASE_DIR, "*", "*rawtag*fits.gz")) + \
                  glob.glob(os.path.join(BASE_DIR, "*", "*rawacq*.fits.gz")) + \
                  glob.glob(os.path.join(BASE_DIR, "*", "*rawaccum*.fits.gz"))

    print("Checking which raw files need to be calibrated (this may take a while)...")
    if zipped_raws:
        if prl:
            to_calibrate = parallelize("smart", "check_usage", needs_processing, zipped_raws)
        else:
            to_calibrate = needs_processing(zipped_raws)

    return to_calibrate

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
