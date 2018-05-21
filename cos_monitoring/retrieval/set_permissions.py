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
import stat
import subprocess
from datetime import datetime as dt
from collections import defaultdict
import sys

from cos_monitoring.retrieval.retrieval_info import BASE_DIR
from cos_monitoring.retrieval.manualabor import parallelize
from cos_monitoring.retrieval.find_new_cos_data import tally_cs, check_proprietary_status

PERM_755 = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
PERM_872 = stat.S_ISVTX | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP

#------------------------------------------------------------------------------#

def set_user_permissions(perm, mydir=BASE_DIR, prl=True):
    print("Opening permissions of {0}..".format(mydir))
    all_files_dirs = glob.glob(os.path.join(mydir, "*")) + \
                     glob.glob(os.path.join(mydir, "*", "*"))
    if prl:
        parallelize(chmod, all_files_dirs, perm)
    else:
        chmod(all_files_dirs, perm)

#------------------------------------------------------------------------------#

def set_grpid(mydir=BASE_DIR, prl=True):
    """
    Given a base directory mydir, determine which datasets are proprietary, 
    and should have the group permissions set to 'cosgo' while all else
    should have the group permissions set to 'cosstis'.
    
    Parameters:
    -----------
        mydir : str
            The base directory to set group permissions on.

    Returns:
    --------
        Nothing
    """
    priv_id = 65545
    pub_id = 6045
            
    existing, existing_filenames, existing_root = tally_cs(mydir, uniq_roots=False)
    propr_status, sql_roots = check_proprietary_status(list(set(existing_root)))
    propr_d = dict(zip(sql_roots, propr_status))

    propr_status = []
    for i in range(len(existing)):
        rootname = existing_root[i].upper()
        try:
            propr_status.append(propr_d[rootname])
        except:
            propr_status.append(pub_id)

    if prl:
        parallelize(chgrp, existing, propr_status)
    else:
        chgrp(existing, propr_status)

    
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def chgrp(files, gid):
    user_id = 5026 # jotaylor's user ID
    
    if isinstance(files, str):
        files = [files]
    else:
        if isinstance(gid, str):
            gid = [gid for x in range(len(files))]

    if isinstance(gid, str):
        gid = [gid]
    assert len(files)==len(gid), "Length of files and gid must be equal"

    for i in range(len(files)):
        os.chown(files[i], gid[i])

    return files

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def chgrp_programgroups(mydir):
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

def chmod(files, perm):
    if isinstance(files, str):
        files = [files]
    else:
        if isinstance(perm, str):
            perm = [perm for x in range(len(files))]
    if isinstance(perm, str):
        perm = [perm]
    assert len(files)==len(perm), "Length of files and gid must be equal"

    for i in range(len(files)):
        os.chmod(files[i], perm[i])

    return files

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

