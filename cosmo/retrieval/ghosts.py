#! /usr/bin/env python

from __future__ import print_function, absolute_import, division

"""
This module is to store legacy functions that were meant to be implemented 
in the retrieval process but are not currently used.
"""


# --------------------------------------------------------------------------- #


# from find_new_cos_data.py

# def connect_cosdb():
#     """
#     Connect to the COS team's database, cos_cci, on the server greendev to
#     determine which COS datasets are currently in the local repository.
#
#     Parameters:
#     -----------
#         None
#
#     Returns:
#     --------
#         all_smov : list
#             All rootnames of all files in the COS greendev database.
#     """
#
#     # Open the configuration file for the COS database connection (MYSQL).
#     config_file = os.path.join(os.environ['HOME'], "configure.yaml")
#     with open(config_file, 'r') as f:
#         SETTINGS = yaml.load(f)
#
#         print("Querying COS greendev database for existing data...")
#         # Connect to the database.
#         Session, engine = load_connection(SETTINGS['connection_string'])
#         sci_files = list(engine.execute("SELECT DISTINCT rootname FROM
#         files "
#                                         "WHERE rootname IS NOT NULL;"))
#         cci_files = list(engine.execute("SELECT DISTINCT name FROM files "
#                                         "WHERE rootname IS NULL AND "
#                                         "LEFT(name,1)='l';"))
#
#         # Store SQLAlchemy results as lists
#         all_sci = [row["rootname"].upper() for row in sci_files]
#         all_cci = [row["name"].strip("_cci.fits.gz").upper() for row in
#         cci_files]
#         all_smov = all_sci + all_cci
#
#         # Close connection
#         engine.dispose()
#
#         return all_smov

# def ensure_no_pending():
#     """
#     Check for any pending archive requests, and if there are any, wait until
#     they finish.
#
#     Parameters:
#     -----------
#         None
#
#     Returns:
#     --------
#         Nothing.
#
#     """
#     num_requests, badness, status_url = check_for_pending()
#     while num_requests > 0:
#         print("There are still {0} requests pending from a previous COSMO "
#               "run, waiting 5 minutes...".format(num_requests))
#         assert (badness,
#                 f"Something went wrong during requests, check {status_url}")
#         time.sleep(300)
#         num_requests, badness, status_url = check_for_pending()
#     else:
#         print("All pending requests finished, moving on!")

# def copy_entire_cache(cos_cache):
#     """
#     In development.
#     """
#     prop_map = {}
#     for item in cos_cache:
#         filename = os.path.basename(item)
#         ippp = filename[:4]
#
#         if not ippp in prop_map.keys():
#             hdr0 = pf.getheader(item,0)
#             proposid = hdr0["proposid"]
#             prop_map[ippp] = proposid
#         else:
#             proposid = prop_map[ippp]
#
#         dest = os.path.join(BASE_DIR, proposid, filename)
#         # By importing pyfastcopy, shutil performance is automatically
#         enhanced
#         shutil.copyfile(item, dest)

# def check_for_pending():
#     """
#     Check the appropriate URL and get the relevant information about pending
#     requests.
#
#     Parameters:
#     -----------
#         None
#
#     Returns:
#     --------
#         num : int
#             Number of pending archive requests (can be zero)
#         badness : Bool
#             True if something went wrong with an archive request
#             (e.g. status=KILLED)
#         status_url : str
#             URL to check for requests
#     """
#
#     status_url = f"http://archive.stsci.edu/cgi-bin/reqstat?reqnum=={USERNAME}"
#
#     tries = 5
#     while tries > 0:
#         try:
#             urllines0 = urllib.request.urlopen(status_url).readlines()
#             urllines = [x.decode("utf-8") for x in urllines0]
#         except IOError:
#             print("Something went wrong connecting to {0}.".format(status_url))
#             tries -= 1
#             time.sleep(30)
#             badness = True
#         else:
#             tries = -100
#             for line in urllines:
#                 mystr = "of these requests are still RUNNING"
#                 if mystr in line:
#                     num_requests = [x.split(mystr) for x in line.split()][0][0]
#                     assert (
#                         num_requests.isdigit()), "A non-number was found in " \
#                                                  "line {0}!".format(line)
#                     num = int(num_requests)
#                     badness = False
#                     break
#                 else:
#                     badness = True
#
#     return num, badness, status_url


# --------------------------------------------------------------------------- #


# from manualabor.py

# def combine_2dicts(dict1, dict2):
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

# def send_email():
#     """
#     Send a confirmation email. Currently not used.
#
#     Parameters:
#     -----------
#         None
#
#     Returns:
#     --------
#         Nothing
#     """
#
#     msg = MIMEMultipart()
#     msg["Subject"] = "Testing"
#     msg["From"] = "jotaylor@stsci.edu"
#     msg["To"] = "jotaylor@stsci.edu"
#     msg.attach(MIMEText("Hello, the script is finished."))
#     msg.attach(MIMEText("Testing line 2."))
#     s = smtplib.SMTP("smtp.stsci.edu")
#     s.sendmail("jotaylor@stsci.edu",["jotaylor@stsci.edu"], msg.as_string())
#     s.quit()

# # TODO: FIGURE OUT WHAT IS GOING ON WITH VARIABLE "ITEM"
# @timefunc
# def unzip_mistakes(zipped):
#     """
#     Occasionally, files will get zipped without having csums made.
#     This function checks if each raw* file has a csum: if it does
#     not, it unzips the file to be calibrated in the make_csum()
#     function.
#
#     Parameters:
#     -----------
#         zipped : list
#             A list of all files in the base directory that are zipped
#
#     Returns:
#     --------
#         Nothing
#     """
#
#     if isinstance(zipped, str):
#         zipped = [zipped]
#     for zfile in zipped:
#         rootname = os.path.basename(zfile)[:9]
#         dirname = os.path.dirname(zfile)
#         calibrate, badness= csum_existence(zfile)
#         if badness is True:
#             print("="*72 + "\n" + "="*72)
#             print("The file is empty or corrupt: {0}".format(item))
#             print("Deleting file")
#             print("="*72 + "\n" + "="*72)
#             os.remove(item)
#             continue
#         if calibrate is True:
#             # chmod_recurs(dirname, PERM_755)
#             files_to_unzip = glob.glob(zfile)
#             uncompress_files(files_to_unzip)

# @timefunc
# def chgrp(mydir):
#     """
#     Walk through all directories in base directory and change the group ids
#     to reflect the type of COS data (calbration, GTO, or GO).
#     Group ids can be found by using the grp module, e.g.
#     > grp.getgrnam("STSCI\cosstis").gr_gid
#     User ids can be found using the pwd module e.g.
#     > pwd.getpwnam("jotaylor").pw_uid
#
#     Parameters:
#     -----------
#         mydir : string
#             The base directory to walk through.
#
#     Returns:
#     --------
#         Nothing
#     """
#
#     user_id = pwd.getpwnam(USERNAME).pw_uid
#     for root, dirs, files in os.walk(mydir):
#         # This expects the dirtree to be in the format /blah/blah/blah/12345
#         pid = root.split("/")[-1]
#         try:
#             pid = int(pid)
#         except ValueError:
#             continue
#         try:
#             if pid in smov_proposals or pid in calib_proposals:
#                 grp_id = 6045  # gid for STSCI/cosstis group
#             elif pid in gto_proposals:
#                 grp_id = 65546  # gid for STSCI/cosgto group
#             else:
#                 grp_id = 65545  # gid for STSCI/cosgo group
#             os.chown(root, user_id, grp_id)
#         except PermissionError as e:
#             print(repr(e))
#
#             for filename in files:
#                 try:
#                     os.chown(os.path.join(root, filename), user_id, grp_id)
#                 except PermissionError as e:
#                     nothing = True
#                     print(repr(e))
#
#
# def chmod_recurs(dirname, perm):
#     """
#     Edit permissions on a directory and all files in that directory.
#
#     Parameters:
#         dirname : string
#             A string of the directory name to edit.
#         perm : int (octal)
#             An integer corresponding to the permission bit settings.
#
#     Returns:
#         Nothing
#     """
#
#     # [os.chmod(os.path.join(root, filename)) for root,dirs,files in
#     # os.walk(DIRECTORY) for filename in files]
#     # The above line works fine, but is confusing to read, and is only
#     # marginally faster than than an explicit for loop.
#     for root, dirs, files in os.walk(dirname):
#         os.chmod(root, perm)
#         if files:
#             for item in files:
#                 os.chmod(os.path.join(root, item), perm)
#
#
# def chmod_recurs_prl(perm, item):
#     """
#     Edit permissions in parallel.
#
#     Parameters:
#         perm : int (octal)
#             An integer corresponding to the permission bit settings.
#         item : string
#             A string of the directory/file name to edit.
#
#     Returns:
#         Nothing
#     """
#
#     os.chmod(item, perm)
#
#
# @timefunc
# def chmod_recurs_sp(rootdir, perm):
#     """
#     Edit permissions on a directory and all files in that directory.
#
#     Parameters:
#         perm : int (octal)
#             An integer corresponding to the permission bit settings.
#         rootdir : string
#             A string of the directory name to edit.
#
#     Returns:
#         Nothing
#     """
#
#     subprocess.check_call(["chmod", "-R", perm, rootdir])
#

#
# def uncompress_files(z_files):
#     """
#     Uncompress zipped files and delete original zipped files.
#
#     Paramters:
#     ----------
#         z_files : list
#             A list of zipped files to zip
#
#     Returns:
#     --------
#         Nothing
#     """
#
#     if isinstance(z_files, str):
#         z_files = [z_files]
#
#     for z_item in z_files:
#         uz_item = z_item.split(".gz")[0]
#         print("Uncompressing {} -> {}".format(z_item, uz_item))
#         with gzip.open(z_item, "rb") as f_in, open(uz_item, "wb") as f_out:
#             shutil.copyfileobj(f_in, f_out)
#             print("Uncompressing {} -> {}".format(z_item, uz_item))
#         if os.path.isfile(uz_item):
#             os.remove(z_item)
#         else:
#             print("Something went terribly wrong unzipping {}".format(z_item))
#

# @timefunc
# def parallelize_joe(func, iterable, nprocs, *args, **kwargs):
#     if nprocs == None:
#         nprocs = check_usage()
#
#     if type(iterable) is dict:
#         isdict = True
#         func_args = [({k: v},) + args for k, v in iterable.items()]
#     else:
#         isdict = False
#         func_args = [(iterable,) + args]
#
#     with Pool(processes=nprocs) as pool:
#         print("Starting the Pool with {} processes...".format(nprocs))
#         results = [pool.apply_async(func, fargs, kwargs) for fargs in
#                    func_args]
#
#         if results[0].get() is not None:
#             if isdict:
#                 funcout = {}
#                 for d in results:
#                     funcout.update(d.get())
#             else:
#                 funcout = [item for innerlist in results for item in
#                            innerlist.get()]
#         else:
#             funcout = None
#
#     return funcout
#
#
# @timefunc
# def parallelize_queue(func, iterable, funcargs=None, nprocs="check_usage",
#                       keep_output=False):
#     """
#     Run a function in parallel with either multiple processes.
#
#     Parameters:
#     -----------
#         func : function
#             Function to be run in parallel.
#         iterable : list, array-like object, or dictionary
#             The series of data that should be processed in parallel with
#             function func.
#         funcargs : tuple
#             Additional arguments, if any, for the function func.
#         nprocs : int
#             Number of processes to use during multiprocessing.
#         keep_output : Bool
#             True if output from function func is to stored and returned.
#
#     Returns:
#     --------
#         resultdict : dictionary
#             None if keep_output is False, otherwise a dictionary where each
#             key is a single input element from the series iterable, and the
#             value is the output from function func for that element.
#     """
#
#     # Handle process usage checks.
#     if nprocs == "check_usage":
#         playnice = False
#         if playnice is True:
#             core_frac = 0.25
#         else:
#             core_frac = 0.5
#         # Get load average of system and # of hyper-threaded CPUs on current
#         # system.
#         loadavg = os.getloadavg()[0]
#         ncores = psutil.cpu_count()
#         # If too many cores are being used, wait 10 mins, and reassess.
#         # Check if elapsed time > XX, if so, just use Y cores
#         while loadavg >= (ncores - 1):
#             print("Too many cores in usage, waiting 10 minutes before "
#                   "continuing...")
#             time.sleep(600)
#         else:
#             avail = ncores - math.ceil(loadavg)
#             nprocs = int(np.floor(avail * core_frac))
#         # If, after rounding, no cores are available, default to 1 to avoid
#         # pooling with processes=0.
#         if nprocs <= 0:
#             nprocs = 1
#
#     # If you want the output from the function being parallelized.
#     if keep_output:
#         out_q = Queue()
#     # If there are input arguments (besides iterable) to the function,
#     # they must be defined as a tuple for multiprocessing.
#     if funcargs is not None:
#         if type(funcargs) is not tuple:
#             print(
#                 "ERROR: Arguments for function {} need to be tuple, "
#                 "exiting".format(
#                     func))
#             sys.exit()
#
#     # Split the iterable up into chunks determined by the number of processes.
#     # what if the iterable is less than the number of nprocs?
#     chunksize = math.ceil(len(iterable) / nprocs)
#     procs = []
#     for i in range(nprocs):
#         if type(iterable) is dict:
#             # try using {k:v for k,v in list(iterable.items())...}
#             subset = {k: iterable[k] for k in
#                       list(iterable.keys())[chunksize * i:chunksize * (i + 1)]}
#         else:
#             subset = iterable[chunksize * i:chunksize * (i + 1)]
#         # Multiprocessing requires a tuple as input, so if there are
#         # additional input arguments, concatenate them.
#         if funcargs is not None:
#             if keep_output:
#                 inargs = (subset, out_q) + funcargs
#             else:
#                 inargs = (subset,) + funcargs
#         else:
#             if keep_output:
#                 inargs = (subset, out_q)
#             else:
#                 inargs = (subset,)
#         # Create the process.
#         p = Process(target=func, args=inargs)
#         procs.append(p)
#         p.start()
#
#     # Collect the results into a dictionary, if desired.
#     if keep_output:
#         resultdict = {}
#         for i in range(nprocs):
#             resultdict.update(out_q.get())
#     else:
#         resultdict = None
#
#     # Wait for all worker processes to finish.
#     for p in procs:
#         p.join()
#
#     return resultdict
#
#
# @timefunc
# def parallelize_orig(myfunc, mylist, check_usage=True):
#     """
#     Parallelize a function. Be a good samaritan and CHECK the current usage
#     of resources before determining number of processes to use. If usage
#     is too high, wait 10 minutes before checking again. Currently only
#     supports usage with functions that act on a list. Will modify for
#     future to support nargs.
#
#     Parameters:
#     -----------
#         myfunc : function
#             The function to be parallelized.
#         mylist : list
#             List to be used with function.
#
#     Returns:
#     --------
#         Nothing
#     """
#
#     # Percentage of cores to eat up.
#     playnice = 0.25
#     playmean = 0.40
#
#     # Split list into multiple lists if it's large.
#     # I don't think this is necessary.
#     #    maxnum = 25
#     #    if len(mylist) > maxnum:
#     #        metalist = [mylist[i:i+maxnum] for i in range(0, len(mylist),
#     #        maxnum)]
#     #    else:
#     #        metalist = [mylist]
#     #    for onelist in metalist:
#     if check_usage:
#         # Get load average of system and # of hyper-threaded CPUs on current
#         # system.
#         loadavg = os.getloadavg()[0]
#         ncores = psutil.cpu_count()
#         # If too many cores are being used, wait 10 mins, and reasses.
#         while loadavg >= (ncores - 1):
#             print("Too many cores in usage, waiting 10 minutes before "
#                   "continuing...")
#             time.sleep(600)
#         else:
#             avail = ncores - math.ceil(loadavg)
#             nprocs = int(np.floor(avail * playmean))
#         # If, after rounding, no cores are available, default to 1 to avoid
#         # pooling with processes=0.
#         if nprocs == 0:
#             nprocs = 1
#     else:
#         nprocs = 10
#
#     print("Starting the Pool with {} processes...".format(nprocs))
#     pool = mp.Pool(processes=nprocs)
#     pool.map(myfunc, mylist)
#     pool.close()
#     pool.join()
#
#
# @timefunc
# def check_disk_space():
#     """
#     Determine how much free space there is in BASE_DIR
#
#     Parameters:
#     -----------
#         None
#
#     Returns:
#     --------
#         free_gb : float
#             Number of free GB in BASE_DIR.
#     """
#
#     statvfs = os.statvfs(BASE_DIR)
#     free_gb = (statvfs.f_frsize * statvfs.f_bfree) / 1e9
#     if free_gb < 200:
#         print("WARNING: There are {0}GB left on disk".format(free_gb))
#         unzipped_csums = glob.glob(os.path.join(BASE_DIR, "*", "*csum*.fits"))
#         if not unzipped_csums:
#             print("WARNING: Disk space is running very low, no csums to zip")
#         else:
#             print("Zipping csums to save space...")
#             if prl:  # TODO: this needs to be addressed
#                 parallelize("smart", "check_usage", compress_files,
#                             unzipped_csums)
#             else:
#                 compress_files(unzipped_csums)
#
#
# def only_one_seg(uz_files):
#     """
#     If rawtag_a and rawtag_b both exist in a list of raw files,
#     keep only one of the segments. Keep NUV and acq files as is.
#
#     Parameters:
#     -----------
#         uz_files : list
#             List of all unzipped files.
#
#     Returns:
#     --------
#         uz_files_1seg : list
#             List of all unzipped files with only one segment for a given root
#             (if applicable).
#     """
#
#     roots = [os.path.basename(x)[:9] for x in uz_files]
#     uniqinds = []
#     uniqroots = []
#     for i in range(len(roots)):
#         if roots[i] not in uniqroots:
#             uniqroots.append(roots[i])
#             uniqinds.append(i)
#     uz_files_1seg = [uz_files[j] for j in uniqinds]
#
#     return uz_files_1seg

# --------------------------------------------------------------------------- #

# this was from dec_calcos.py (which no longer exists)

# def clobber_calcos(func):
#     """
#     This is a decorator to be used to clobber output files from calcos.
#     If the output products already exist, they will be deleted and calcos
#     re-run for the particular infile.
#
#     Use:
#     ----
#         This should be imported and used as an excplicit decorator:
#
#         import calcos
#         from dec_calcos import clobber_calcos
#         new_calcos = clobber_calcos(calcos.calcos)
#         new_calcos(input...)
#
#     Parameters:
#     -----------
#         func : function
#             The input function to decorate.
#
#     Returns:
#     --------
#         wrapper : function
#             A wrapper to the modified function.
#     """
#
#     __author__ = "Jo Taylor"
#     __date__ = "02-25-2016"
#     __maintainer__ = "Jo Taylor"
#     __email__ = "jotaylor@stsci.edu"
#
#     import os
#     import glob
#     from astropy.io import fits
#
#     def wrapper(*args, **kwargs):
#         try:
#             func(*args, **kwargs)
#         except Exception as e:
#             # If the reason for crashing is that the output files already exist
#             if type(e).__name__ == "RuntimeError" and \
#                e.args[0] == "output files already exist":
#                 # If asntable defined in kwargs or just in args
#                 if "asntable" in kwargs.keys():
#                     asntable = kwargs["asntable"]
#                 else:
#                     asntable = args[0]
#                 filename = os.path.basename(asntable)
#                 if "outdir" in kwargs.keys():
#                     outdir = kwargs["outdir"]
#                 elif len(args) >= 2:
#                     outdir = args[1]
#                 else:
#                     outdir = None
#                 # If the asntable is an association file, not rawtag,
#                 # get the member IDs
#                 if "asn.fits" in filename:
#                     with fits.open(asntable) as hdu:
#                         data = hdu[1].data
#                         memnames = data["memname"]
#                         f_to_remove = [root.lower() for root in memnames]
#                     # If an output directory is specified, the asn file is
#                     # is copied to there and must be removed as well.
#                     if outdir:
#                        os.remove(os.path.join(outdir, filename))
#                 # if rawtag, just get the rawtag rootname to delete
#                 else:
#                     f_to_remove = [filename[:9]]
#                 # If no outdir specified, it is the current directory
#                 if not outdir:
#                     outdir = "."
#                 for item in f_to_remove:
#                     matching = glob.glob(os.path.join(outdir,item+"*fits*"))
#                     for match in matching:
#                         ext = match.split("/")[-1].split("_")[1].split(".")[0]
#                         if ext not in ["asn", "pha", "rawaccum", "rawacq",
#                                        "rawtag", "spt"]:
#                             os.remove(match)
#                 print("="*72 + "\n" + "="*72)
#                 print("!!!WARNING!!! Deleting products for {} !!!".format(
#                       asntable))
#                 print("CalCOS will now calibrate {}...".format(asntable))
#                 print("="*72 + "\n" + "="*72)
#                 func(*args, **kwargs)
#             else:
#                 raise e
#     return wrapper


# --------------------------------------------------------------------------- #
