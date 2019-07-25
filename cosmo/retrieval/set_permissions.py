#! /usr/bin/env python

from __future__ import print_function, absolute_import, division

"""
This is a program designed to calibrate COS raw files to create CSUMs.

It also sets permissions and group ids appropriately as well
as zipping up any unzipped files to save space.
"""

__author__ = "Jo Taylor"
__date__ = "04-13-2016"
__maintainer__ = "Camellia Magness"

# Import necessary packages.
import os
import stat
import pwd
import glob
from tqdm import tqdm

from .. import SETTINGS
from .manualabor import parallelize
from .find_new_cos_data import tally_cs, check_proprietary_status

PERM_755 = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | \
           stat.S_IXOTH
PERM_550 = stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP

BASE_DIR = SETTINGS["filesystem"]["source"]
USERNAME = SETTINGS["retrieval"]["username"]


# --------------------------------------------------------------------------- #


def set_user_permissions(perm, mydir=BASE_DIR, prl=True):
    """
    Function to set file permissions as specified.

    Parameters
    ----------
    perm : str, octal mode
        permission keyword/mode. if "open", uses the 755 octal mode for all
        files and directories. if "close", uses 550 for files and
        directories. otherwise, a custom valid octal mode can be passed.
    mydir : str, default=BASE_DIR
        absolute path to directory to perform the actions on. by default is
        set to be the retrieval directory as specified in the user's
        configuration .yaml file.
    prl: bool, default=True
        keyword to parallelize this function.
    """
    all_dirs = glob.glob(os.path.join(mydir, "*"))
    all_files = glob.glob(os.path.join(mydir, "*", "*"))

    if perm == "open":
        print("Opening permissions of {}...".format(mydir))
        perm_d = {x: PERM_755 for x in tqdm(all_dirs + all_files)}
    elif perm == "close":
        print("Closing permissions of {}...".format(mydir))
        perm_d1 = {x: PERM_550 for x in tqdm(all_files)}
        perm_d2 = {x: PERM_550 | stat.S_ISVTX for x in tqdm(all_dirs)}
        perm_d = {**perm_d1, **perm_d2}
    else:
        if not isinstance(perm, int) or perm < 0 or perm > 0o7777:
            raise ValueError('Invalid permission mode: {}'.format(oct(perm)))
        perm_d = {x: perm for x in tqdm(all_dirs + all_files)}

    if prl:
        parallelize("smart", "check_usage", chmod, perm_d)
    else:
        chmod(perm_d)


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
    # priv_id = 65545
    pub_id = 6045

    existing, existing_filenames, existing_root = tally_cs(mydir,
                                                           uniq_roots=False)
    propr_status, sql_roots = check_proprietary_status(
        list(set(existing_root)))
    propr_d = dict(zip(sql_roots, propr_status))

    propr_status = []
    for i in range(len(existing)):
        rootname = existing_root[i].upper()
        try:
            propr_status.append(propr_d[rootname])
        except:
            propr_status.append(pub_id)
    propr_status_d = dict(zip(existing, propr_status))
    all_dirs = glob.glob(os.path.join(mydir, "*"))
    dir_perm_d = {x: pub_id for x in all_dirs}
    perm_d = {**propr_status_d, **dir_perm_d}

    print("Setting group IDs...")
    if prl:
        parallelize("smart", "check_usage", chgrp, perm_d)
    else:
        chgrp(perm_d)


def chgrp(grp_perm):
    user_id = pwd.getpwnam(USERNAME).pw_uid
    for filename, gid in grp_perm.items():
        os.chown(filename, user_id, gid)

    return grp_perm


def chmod(file_perm):
    for filename, fid in file_perm.items():
        os.chmod(filename, fid)

    return file_perm
