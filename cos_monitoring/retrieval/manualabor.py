#! /usr/bin/env python

'''
This ia a program designed to calibrate COS rawfiles to create CSUMs.

It also sets permissions and group ids appropriately as well
as zipping up any unzipped files to save space.
'''

__author__ = "Jo Taylor"
__date__ = "02-23-2016"
__maintainer__ = "Jo Taylor"

# Import necessary packages.
import os
import glob
import shutil
import gzip
from astropy.io import fits as pf
import calcos
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from ProgramGroups import * 
from dec_calcos import clobber_calcos
from logging_dec import log_function

LINEOUT = "#"*75+"\n"
STAROUT = "*"*75+"\n"

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@log_function
def unzip_mistakes(zipped):
    '''
    Occasionally, files will get zipped without having csums made.
    This function checks if each raw* file has a csum: if it does
    not, it unzips the file to be calibrated in the make_csum()
    function.

    Parameters:
        zipped : list
            A list of all files in the base directory that are zipped

    Returns:
        Nothing
    '''
    
# how to include function name in logger output???
#    print LINEOUT,LINEOUT,"\n"
    for zfile in zipped:
        rootname = os.path.basename(zfile)[:9]
        dirname = os.path.dirname(zfile)
        existence = csum_existence(zfile)
        if not existence:
            chmod_recurs(dirname, 0755)
            files_to_unzip = glob.glob(zfile) 
            uncompress_files(files_to_unzip)
        else:
            unzip_status = False

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@log_function
def make_csum(unzipped_raws):
    '''
    Calibrate raw files to produce csum files.

    Parameters:
        unzipped_raws : list
            A list of all filenames that are unzipped to be calibrated.

    Returns:
        Nothing
    '''
    
    run_calcos = clobber_calcos(calcos.calcos)
    logger.info("Creating CSUM files")
    for item in unzipped_raws:
        existence = csum_existence(item)
        if not existence:
            dirname = os.path.dirname(item)
            os.chmod(dirname, 0755)
            os.chmod(item, 0755)
            csum_dir = os.path.join(dirname, "csum")
            try:
                run_calcos(item, outdir=csum_dir, verbosity=2, 
                           create_csum_image=True, only_csum=True, 
                           compress_csum=False)        
                
            except Exception, e:
                logger.exception("There was an error processing {}:".format(item))
                #print("There was an error processing {}:\n{}".format(item,e))
                pass
            if os.path.exists(csum_dir):
                csums = glob.glob(os.path.join(csum_dir, "*csum*"))
                if csums:
                    for csum in csums:
                        shutil.copy(csum, dirname)
                else:
                    logger.error("A csum dir was created but there are no csums: {}".format(csum_dir))
                shutil.rmtree(csum_dir)
                
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@log_function
def fix_perm(base_dir):
    '''
    Walk through all directories in base directory and change the group ids
    to reflect the type of COS data (calbration, GTO, or GO).
    Group ids can be found by using the grp module, e.g. 
    > grp.getgrnam("STSCI\cosstis").gr_gid
    User ids can be found using the pwd module e.g.
    > pwd.getpwnam("jotaylor").pw_uid

    Parameters:
        base_dir : string
            The base directory to walk through.

    Returns:
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

@log_function
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
    
    
#    allfiles = glob.glob(os.path.join(dirname, "*fits"))
#    os.chmod(dirname, perm)
#    for item in allfiles:
#        os.chmod(item, perm)

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@log_function
def csum_existence(filename):
    '''
    Check for the existence of a CSUM for a given input dataset.

    Parameters:
        filename : string
            A raw dataset name.

    Returns:
        existence : bool
            A boolean specifying if csums exist or not.

    '''
    rootname = os.path.basename(filename)[:9]
    dirname = os.path.dirname(filename)
    exptype = pf.getval(filename, "exptype")
    # If getting one header keyword, getval is faster than opening.
    # The more you know.
    if exptype != "ACQ/PEAKD" and exptype != "ACQ/PEAKXD":
        csums = glob.glob(os.path.join(dirname,rootname+"*csum*"))
        if not csums:
            existence = False
        else:
            existence = True
    
    return existence

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

@log_function
def compress_files(uz_files):
    '''
    Compress unzipped files and delete original unzipped files.

    Paramters:
        uz_files : list
            A list of unzipped files to zip

    Returns:
        Nothing
    '''
    
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

@log_function
def uncompress_files(z_files):
    '''
    Uncompress zipped files and delete original zipped files.

    Paramters:
        z_files : list
            A list of zipped files to zip

    Returns:
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

@log_function
def send_email():
    ''' 
    Send a confirmation email. Currently not used.

    Parameters:
        None

    Returns:
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
    
if __name__ == "__main__":
    base_dir = "/grp/hst/cos2/smov_testing/"
    chmod_recurs(base_dir, 0755) 

    # using glob is faster than using os.walk
    zipped = glob.glob(os.path.join(base_dir, "?????", "*raw*gz"))
    if zipped:
        unzip_mistakes(zipped)
    
    unzipped_raws = glob.glob(os.path.join(base_dir, "?????", "*rawtag*fits")) + \
                   glob.glob(os.path.join(base_dir, "?????", "*rawacq.fits"))
    if unzipped_raws:
        make_csum(unzipped_raws)
    
    all_unzipped = glob.glob(os.path.join(base_dir, "?????", "*fits"))
    if all_unzipped:
        compress_files(all_unzipped)
    
    fix_perm(base_dir)
    # permission determined by stat.S_ISVTX | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
    chmod_recurs(base_dir, 872)
