#! /usr/bin/env python

from __future__ import print_function, absolute_import, division

"""
This ia a program designed to calibrate COS rawfiles to create CSUMs. It 
also sets permissions and group ids appropriately as well as zipping up any 
unzipped files to save space.
"""

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
from multiprocessing import Pool  # Queue, Process
# noinspection PyPackageRequirements
import psutil
import math
import time
import numpy as np
import stat
from collections import defaultdict
from itertools import islice

from .. import SETTINGS

BASE_DIR = SETTINGS["filesystem"]["source"]
CACHE = SETTINGS["retrieval"]["cache"]
USERNAME = SETTINGS["retrieval"]["username"]

PERM_755 = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
PERM_872 = stat.S_ISVTX | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
CSUM_DIR = "tmp_out"


# --------------------------------------------------------------------------- #


def timefunc(func):  # IN USE
    """
    Decorator to wrap and time functions.

    Parameters
    ----------
    func: func
        Function to be timed

    Returns
    -------
    wrapper: func
        Timed version of the function
    """

    def wrapper(*args, **kw):
        t1 = datetime.datetime.now()
        result = func(*args, **kw)
        t2 = datetime.datetime.now()

        print(f"{func.__name__} executed in {t2 - t1}")

        return result

    return wrapper


def parallelize(func, iterable, chunksize="smart", nprocs="check_usage", *args, **kwargs):  # IN USE
    """
    This function is a decorator to parallelize other functions.

    Parameters
    ----------
    func: function
        Function to be parallelized

    iterable: iterable
        Some iterable the function takes

    chunksize: str, default="smart"
        String to determine how the iterable should be split into chunks

    nprocs: str, default="check_usage"
        String to determine how many processes to use

    args: list
        Arguments that are parameters of the original function

    kwargs: dict
        Keyword arguments that are parameters of the original function

    Returns
    -------
    funcout: replacement function
        Function that has been parallelized
    """
    t1 = datetime.datetime.now()
    funcout = None

    if len(iterable) == 0:
        # nothing to iterate over; return None
        return funcout

    if nprocs == "check_usage":
        # get the number of cores to use for parallelizing based on current
        # server usage
        nprocs = check_usage()

    if chunksize == "smart":
        chunksize = smart_chunks(len(iterable), nprocs)

    if isinstance(iterable, dict):
        isdict = True
        chunks = chunkify_d(iterable, chunksize)

    else:
        isdict = False
        chunks = [iterable[i:i+chunksize] for i in range(0, len(iterable), chunksize)]

    # create function args for the number of chunks
    func_args = [(x,) + args for x in chunks]

    # parallelize the function
    with Pool(processes=nprocs) as pool:
        # print("Starting the Pool for {} with {} processes...".format(func,
        #                                                              nprocs))
        results = [pool.apply_async(func, fargs, kwargs) for fargs in func_args]

        if results[0].get() is not None:

            if isdict:
                funcout = {}
                for d in results:
                    funcout.update(d.get())

            else:
                funcout = [item for innerlist in results for item in innerlist.get()]

    t2 = datetime.datetime.now()
    print(f"\tparallelize({func.__name__}) executed in {t2 - t1}")

    return funcout


def check_usage(playnice=True):  # IN USE
    """
    Check current system resource usage, and adjust number of requested
    processes accordingly.

    Parameters:
    -----------
    playnice : Bool, default=True
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

    # Get load average of system and # of hyper-threaded CPUs on current
    # system.
    loadavg = os.getloadavg()[0]  # number of CPUs in use
    ncores = psutil.cpu_count()  # number of CPUs available

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

    # TODO: make decisions about the fractions being used here and the timing?
    return nprocs


def smart_chunks(nelems, nprocs):  # IN USE
    """
    This function decides what chunksize should be used, based on some
    conditions.

    Parameters
    ----------
    nelems: int
        The number of elements that need to be subdivided
    nprocs: int
        The number of processors that are being used

    Returns
    -------
    chunksize: int
        The size of chunks that should be used for subdividing the elements
    """
    if nelems <= 10 * nprocs:
        chunksize = math.ceil(nelems/nprocs)

    elif 10 * nprocs < nelems <= 1000 * nprocs:
        chunksize = 10

    else:
        chunksize = 100

    return chunksize


def chunkify_d(d, chunksize):  # IN USE
    """
    This function is for turning a dictionary of iterables into chunks of
    dictionaries.

    Parameters
    ----------
    d : iterable, dict
        Dictionary of iterables
    chunksize: int
        Size of chunks for subdividing elements

    Returns
    -------
    chunks: dict
        Iterable dictionary divided into appropriate chunks
    """
    it = iter(d)
    chunks = []
    for i in range(0, len(d), chunksize):
        chunks.append({k: d[k] for k in islice(it, chunksize)})

    return chunks


def combine_2dicts(dict1, dict2):  # IN USE
    """
    This function is a helper function to combine dictionaries.

    Parameters
    ----------
    dict1: dict
        first dictionary to be combined
    dict2: dict
        second dictionary to be combined

    Returns
    -------
    combined: dict
        dictionary with unique union entries for the union of keys in dict1,
        dict2
    """
    combined = defaultdict(list)

    for k, v in dict1.items():
        if k in dict2:
            combined[k] += list(set(v) | set(dict2[k]))

        else:
            combined[k] += list(v)

    for k, v in dict2.items():
        if k not in combined:
            combined[k] += list(v)

    return combined


def remove_outdirs():  # IN USE
    """
    Removes all temporary dirs (read: all dirs) in CSUM_DIR for each PID dir
    in BASE_DIR.
    """
    tmp_dirs = glob.glob(os.path.join(BASE_DIR, "*", CSUM_DIR))

    if tmp_dirs:
        for bad_dir in tmp_dirs:
            try:
                shutil.rmtree(bad_dir)

            except OSError:
                pass


def handle_nullfiles():  # IN USE
    """
    It can be difficult to tell if all files found by find_new_cos_data with
    program ID 'NULL' are actually COS files (e.g. reference files that start
    with 'L' can accidentally be downloaded). Delete any files that are not
    actually COS files.

    Returns:
    --------
    None, but only if there are no null files to delete
    """

    nullfiles = glob.glob(os.path.join(BASE_DIR, "NULL", "*fits*"))

    if len(nullfiles) == 0:
        return

    else:
        print(f"Handling {len(nullfiles)} NULL datasets...")

    for item in nullfiles:
        with fits.open(item) as hdulist:
            hdr0 = hdulist[0].header

        try:
            _ = hdr0["instrume"]

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
                    _ = hdr0["useafter"]
                    os.remove(item)

                except KeyError:
                    os.remove(item)

        except KeyError:
            # These files are not COS datasets and should be removed.
            os.remove(item)


def gzip_files(prl=True):  # IN USE
    """
    Get all the unzipped files in all the PID subdirs of BASE_DIR and zip them.

    Parameters
    ----------
    prl: bool
        Switch to parallelize the compression
    """
    # Get a list of all unzipped files and zip them.
    # everything else should theoretically end in .gz
    unzipped = glob.glob(os.path.join(BASE_DIR, "*", "*fits"))

    if unzipped:
        print(f"Zipping {len(unzipped)} unzipped file(s)...")

        if prl:
            parallelize(compress_files, unzipped)

        else:
            compress_files(unzipped)


def compress_files(uz_files, outdir=None, remove_orig=True, verbose=False):
    # IN USE
    """
    Compress unzipped files and delete original unzipped files.

    Parameters:
    ----------
    uz_files : list or string
        Unzipped file(s) to zip

    outdir : list or string
        Directory to place zipped products
    """

    if isinstance(uz_files, str):
        uz_files = [uz_files]

    if outdir is None:
        outdir = [os.path.dirname(x) for x in uz_files]

    elif isinstance(outdir, str):
        outdir = [outdir for _ in uz_files]  # what

    outdir = [os.path.dirname(x) if not os.path.isdir(x) else x for x in outdir]

    if len(uz_files) != len(outdir):
        print(f"ERROR: List uz_files needs to match length of list outdir, {len(uz_files)} vs {len(outdir)}, exiting")
        sys.exit()

    for i in range(len(uz_files)):
        z_item = os.path.join(outdir[i], os.path.basename(uz_files[i]) + ".gz")

        with open(uz_files[i], "rb") as f_in, gzip.open(z_item, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

            if verbose is True:
                print(f"Compressing {uz_files[i]} -> {z_item}")

        if remove_orig is True:
            if os.path.isfile(z_item):
                os.remove(uz_files[i])

            else:
                print(f"Something went terribly wrong zipping {uz_files[i]}")


def get_unprocessed_data(prl=True):  # IN USE
    """
    This function finds all the raw files in the BASE_DIR and its subdirs,
    under the assumption that these are new files that need to be calibrated.

    Parameters
    ----------
    prl : bool, default=True
        Switch for processing in parallel

    Returns
    -------
    to_calibrate: list
        List of files that need calibrating
    """
    to_calibrate = None
    # When calibrating zipped raw files, you need to calibrate both segments
    # separately since calcos can't find the original
    zipped_raws = (
            glob.glob(os.path.join(BASE_DIR, "*", "*rawtag*fits.gz")) +
            glob.glob(os.path.join(BASE_DIR, "*", "*rawacq*fits.gz")) +
            glob.glob(os.path.join(BASE_DIR, "*", "*rawaccum*.fits.gz"))
    )

    print("Checking which raw files need to be calibrated (this may take a while)...")

    if zipped_raws:
        if prl:
            to_calibrate = parallelize(needs_processing, zipped_raws)

        else:
            to_calibrate = needs_processing(zipped_raws)

    return to_calibrate


def needs_processing(zipped):  # IN USE
    """
    Checks each file in the given list to see if it needs to be calibrated.
    Deletes bad files.

    Parameters
    ----------
    zipped: list
        List of presumably raw, zipped files that need to be calibrated

    Returns
    -------
    files to calibrate: list
        List of files in the given list that do actually need to be
        calibrated and are not bad files
    """
    if isinstance(zipped, str):
        zipped = [zipped]

    files_to_calibrate = []

    for zfile in zipped:
        # figure if the file should be calibrated or deleted
        calibrate, badness = csum_existence(zfile)

        if badness is True:
            print(
                f"="*72 + f"\n" + f"="*72 + f"\n" +
                f"The file is empty or corrupt: {zfile}\n" +
                f"Deleting file\n" +
                f"=" * 72 + f"\n" + f"=" * 72 + "\n"
            )
            os.remove(zfile)

            continue

        if calibrate is True:
            files_to_calibrate.append(zfile)

    return files_to_calibrate


def csum_existence(filename):  # IN USE
    """
    Check for the existence of a CSUM for a given input dataset.

    Parameters:
    -----------
    filename : str
        A raw dataset name.

    Returns:
    --------
    calibrate : bool
        True if the dataset should not be calibrated to create csums

    badness : bool
        True if the dataset should be deleted (if corrupt or empty)
    """
    rootname = os.path.basename(filename)[:9]
    dirname = os.path.dirname(filename)
    calibrate = False
    badness = False

    # If getting one header keyword, getval is faster than opening.
    # The more you know.
    # noinspection PyBroadException
    try:
        exptime = fits.getval(filename, "exptime", 1)

        if exptime == 0:
            return calibrate, badness

    except Exception:
        pass

    exptype = None

    try:
        exptype = fits.getval(filename, "exptype")

    except KeyError:  # EXPTYPE = None
        return calibrate, badness

    except Exception as e:
        if type(e).__name__ == "IOError" and e.args[0] == "Empty or corrupt FITS file":
            badness = True
            return calibrate, badness

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

    return calibrate, badness


def clobber_calcos_csumgz(func):  # IN USE
    """
    This is a decorator to be used to clobber output files from calcos.
    If the output products already exist, they will be deleted and calcos
    re-run for the particular infile.

    Use:
    ----
    This should be imported and used as an explicit decorator:

        import calcos
        from dec_calcos import clobber_calcos
        new_calcos = clobber_calcos(calcos.calcos)
        new_calcos(input...)

    Parameters:
    -----------
    func : func
        The input function to decorate

    Returns:
    --------
    wrapper : func
        A wrapper to the modified function
    """
    # __author__ = "Jo Taylor"
    # __date__ = "02-25-2016"
    # __maintainer__ = "Jo Taylor"
    # __email__ = "jotaylor@stsci.edu"

    import os
    import glob
    from astropy.io import fits

    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)

        except Exception as e:
            # If the reason for crashing is that the output files already exist
            if type(e).__name__ == "RuntimeError" and e.args[0] == "output files already exist":
                # If asntable defined in kwargs or just in args
                if "asntable" in kwargs.keys():
                    asntable = kwargs["asntable"]

                else:
                    asntable = args[0]

                filename = os.path.basename(asntable)

                if "outdir" in kwargs.keys():
                    outdir = kwargs["outdir"]

                elif len(args) >= 2:
                    outdir = args[1]

                else:
                    outdir = None

                # If the asntable is an association file, not rawtag,
                # get the member IDs
                if "asn.fits" in filename:
                    with fits.open(asntable) as hdu:
                        data = hdu[1].data
                        memnames = data["memname"]
                        f_to_remove = [root.lower() for root in memnames]

                    # If an output directory is specified, the asn file is
                    # is copied to there and must be removed as well.
                    if outdir:
                        os.remove(os.path.join(outdir, filename))

                # if rawtag, just get the rawtag rootname to delete
                else:
                    f_to_remove = [filename[:9]]

                # If no outdir specified, it is the current directory
                if not outdir:
                    outdir = "."

                for item in f_to_remove:
                    matching = (
                        glob.glob(os.path.join(outdir, item + "corrtag*fits*")) +
                        glob.glob(os.path.join(outdir, item + "csum*fits*"))
                    )

                    for match in matching:
                        ext = match.split("/")[-1].split("_")[1].split(".")[0]

                        if ext not in ["asn", "pha", "rawaccum", "rawacq", "rawtag", "spt"]:
                            os.remove(match)

                print(
                    f"="*72 + f"\n" + f"="*72 + f"\n" +
                    f"!!!WARNING!!! Deleting products for {asntable} !!!\n" +
                    f"CalCOS will now calibrate {asntable}...\n" +
                    f"=" * 72 + f"\n" + f"=" * 72 + f"\n"
                )
                # actually running calcos here
                func(*args, **kwargs)

            else:
                raise e

    return wrapper


def copy_outdirs():  # IN USE
    """
    Copy the csums from the temp CSUM dirs to the main subdir for each csum.
    """
    csums = glob.glob(os.path.join(BASE_DIR, "*", CSUM_DIR, "*csum*"))

    if csums:
        dest_dirs = [os.path.dirname(x).strip(CSUM_DIR) for x in csums]
        move_files(csums, dest_dirs)


def move_files(orig, dest):  # IN USE
    """
    For each file in the given list, move it to the destination.

    Parameters
    ----------
    orig: list or str
        List of files that need to be moved

    dest: list or str
        Destination(s) for files to be moved to
    """
    # why does this even exist as a function
    if isinstance(orig, str):
        orig = [orig]

    if isinstance(dest, str):
        dest = [dest]

    for i in range(len(orig)):
        shutil.move(orig[i], dest[i])

# --------------------------------------------------------------------------- #
