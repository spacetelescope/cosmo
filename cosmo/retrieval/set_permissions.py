#! /usr/bin/env python
"""
This module sets permissions and group ids appropriately.
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


def set_user_permissions(perm, mydir=BASE_DIR, prl=True):  # IN USE
    """
    Function to set file permissions as specified.

    Parameters
    ----------
    perm : str, octal mode
        Permission keyword/mode. if "open", uses the 755 octal mode for all
        files and directories. if "close", uses 550 for files and
        directories. otherwise, a custom valid octal mode can be passed.
    mydir : str, default=BASE_DIR
        Absolute path to directory to perform the actions on. by default is
        set to be the retrieval directory as specified in the user's
        configuration .yaml file.
    prl: bool, default=True
        Keyword to parallelize this function.
    """
    all_dirs = glob.glob(os.path.join(mydir, "*"))
    all_files = glob.glob(os.path.join(mydir, "*", "*"))

    if perm == "open":
        print(f"Opening permissions of {mydir}...")
        perm_d = {x: PERM_755 for x in tqdm(all_dirs + all_files)}

    elif perm == "close":
        print(f"Closing permissions of {mydir}...")
        perm_d1 = {x: PERM_550 for x in tqdm(all_files)}
        perm_d2 = {x: PERM_550 | stat.S_ISVTX for x in tqdm(all_dirs)}
        perm_d = {**perm_d1, **perm_d2}

    else:
        if not isinstance(perm, int) or perm < 0 or perm > 0o7777:
            raise ValueError(f'Invalid permission mode: {oct(perm)}')

        perm_d = {x: perm for x in tqdm(all_dirs + all_files)}

    if prl:
        parallelize(chmod, perm_d)

    else:
        chmod(perm_d)


def chmod(file_perm):  # IN USE
    """
    This function changes the permissions of all files given.

    Parameters
    ----------
    file_perm: dict
        Dictionary of file names and permission mode for each filename
    """
    for filename, fid in file_perm.items():
        os.chmod(filename, fid)


def set_grpid(mydir=BASE_DIR, prl=True):  # IN USE
    """
    Given a base directory mydir, determine which datasets are proprietary, 
    and should have the group permissions set to 'cosgo' while all else
    should have the group permissions set to 'cosstis'.
    
    Parameters:
    -----------
    mydir : str, default=BASE_DIR
        The base directory to set group permissions on.
    """
    # priv_id = 65545
    pub_id = 6045

    # find all the existing data in the BASE_DIR
    existing, existing_filenames, existing_root = tally_cs(mydir, uniq_roots=False)

    # get the proprietary status and sql result rootname for each file
    propr_status, sql_roots = check_proprietary_status(list(set(existing_root)))
    propr_d = dict(zip(sql_roots, propr_status))

    # make a version of the propr_status that matches the order of the
    # existing files
    propr_status = []

    for i in range(len(existing)):
        rootname = existing_root[i].upper()

        # noinspection PyBroadException
        try:
            propr_status.append(propr_d[rootname])

        except Exception:
            propr_status.append(pub_id)

    propr_status_d = dict(zip(existing, propr_status))

    all_dirs = glob.glob(os.path.join(mydir, "*"))

    # make a default public id associated with all dirs
    dir_perm_d = {x: pub_id for x in all_dirs}

    # intersection of the default public id and the derived specific ids
    perm_d = {**propr_status_d, **dir_perm_d}

    # TODO here 2
    print("Setting group IDs...")
    if prl:
        parallelize(chgrp, perm_d)

    else:
        chgrp(perm_d)


def chgrp(grp_perm):  # IN USE
    """
    Change the "ownership" of each file to the correct group that
    corresponds to public or proprietary data.

    Parameters
    ----------
    grp_perm: dict
        Dictionary of file names and group that should own it
    """
    user_id = pwd.getpwnam(USERNAME).pw_uid

    for filename, gid in grp_perm.items():
        os.chown(filename, user_id, gid)

    return grp_perm


# --------------------------------------------------------------------------- #
