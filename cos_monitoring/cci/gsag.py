
"""Routine to monitor the modal gain in each pixel as a
function of time.  Uses COS Cumulative Image (CCI) files
to produce a modal gain map for each time period.  Modal gain
maps for each period are collated to monitor the progress of
each pixel(superpixel) with time.  Pixels that drop below
a threshold value are flagged and collected into a
gain sag table reference file (gsagtab).

The PHA modal gain threshold is set by global variable MODAL_GAIN_LIMIT.
Allowing the modal gain of a distribution to come within 1 gain bin
of the threshold results in ~8% loss of flux.  Within
2 gain bins, ~4%
3 gain bins, ~2%
4 gain bins, ~1%

However, due to the column summing, a 4% loss in a region does not appear to be so in the extracted spectrum.
"""

__author__ = 'Justin Ely'
__maintainer__ = 'Justin Ely'
__email__ = 'ely@stsci.edu'
__status__ = 'Active'

import os
import shutil
import time
from datetime import datetime
import glob
import sys
from sqlalchemy.engine import create_engine
import logging
logger = logging.getLogger(__name__)

from astropy.io import fits
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from sqlalchemy.sql.functions import concat

from ..database.db_tables import open_settings, load_connection
from ..utils import send_email
from .constants import *  #Shut yo face

#------------------------------------------------------------

def main(data_dir, run_regress=False):
    """ Main driver for monitoring program.
    """
    new_gsagtab = make_gsagtab_db(data_dir, blue=False)
    blue_gsagtab = make_gsagtab_db(data_dir, blue=True)

    old_gsagtab = get_cdbs_gsagtab()

    compare_gsag(new_gsagtab, old_gsagtab, data_dir)

    if run_regress:
        test_gsag_calibration(new_gsagtab)
    else:
        print("Regression set skipped")

    send_forms(data_dir)

#------------------------------------------------------------

def get_index( gsag, segment, dethv ):
    """
    Returns extension index of gsagtab with corresponding
    segment and dethv
    """
    found_ext = -1

    if segment == 'FUVA':
        hv_string = 'HVLEVELA'
    elif segment == 'FUVB':
        hv_string = 'HVLEVELB'

    for i,ext in enumerate( gsag[1:] ):
        if ext.header['segment'] == segment:
            if ext.header[hv_string] == dethv:
                found_ext = (i+1)
                break

    return found_ext

#------------------------------------------------------------

def compare_gsag(new, old, outdir):
    """Compare two gainsag tables to see what has changed
    """
    ### Needs to be tested ###

    print('\n#-------------------#')
    print('Comparing Gsag tables')
    print('#-------------------#')
    print(('New GSAGTAB %s'%( new )))
    print(('Old GSAGTAB %s'%( old )))

    if type(new) == str:
        new = fits.open(new)
    if type(old) == str:
        old = fits.open(old)

    report_file = open(os.path.join(outdir, 'gsag_report.txt'), 'w')

    possible_hv_levels = [0,100] + list(range(100,179))

    for segment,hv_keyword in zip( ['FUVA','FUVB'], ['HVLEVELA','HVLEVELB'] ):
        new_hv = set( [ ext.header[hv_keyword] for ext in new[1:] if ext.header['SEGMENT'] == segment ] )
        old_hv = set( [ ext.header[hv_keyword] for ext in old[1:] if ext.header['SEGMENT'] == segment ] )

        only_new_hv = new_hv.difference( old_hv )
        only_old_hv = old_hv.difference( new_hv )

        both_hv = list( new_hv.union( old_hv ) )
        both_hv.sort()

        print(('#---',segment,'---#'))

        #assert ( len(only_old_hv) == 0 ),'There is an HV value found in the old table that is not found in the new.'
        if len(only_old_hv) > 0:
            print('WARNING: There is an HV value found in the old table that is not found in the new.')

        if len( only_new_hv ):
            print('There is at least one new extension, you should probably deliver this one')
            report_file.write('There is at least one new extension, you should probably deliver this one \n')
            report_file.write('New HV extensions:\n')
            report_file.write(','.join( map(str,np.sort( list(only_new_hv) ) ) ) )
            report_file.write('\n')

        for hv in both_hv:
            report_file.write( '#----- %d \n'%(hv) )

            old_ext_index = get_index( old, segment, hv )
            new_ext_index = get_index( new, segment, hv )

            if old_ext_index == -1:
                print(('%s %d: not found in old table'%( segment, hv )))
                continue

            if new_ext_index == -1:
                print(('WARNING: %s %d: not found in new table'%( segment, hv )))
                continue

            else:
                old_ext = old[ old_ext_index ]
                new_ext = new[ new_ext_index ]

                #Get (y,x,mjd) triple for each flagged region.  Assumes binning is the same
                old_regions = set( [ (y,x,mjd) for x,y,mjd in
                                     zip(old_ext.data['ly'], old_ext.data['lx'], old_ext.data['Date']) ] )

                new_regions = set( [ (y,x,mjd) for x,y,mjd in
                                     zip(new_ext.data['ly'], new_ext.data['lx'], new_ext.data['Date']) ] )

                #Strip triple down to (y,x) coordinate pair to check just locations
                old_coords = set( [ (item[0],item[1]) for item in old_regions ] )
                new_coords = set( [ (item[0],item[1]) for item in new_regions ] )

                only_old_coords = old_coords.difference( new_coords )
                only_new_coords = new_coords.difference( old_coords )
                both_coords = old_coords.union( new_coords )

                N_old = len(only_old_coords)
                N_new = len(only_new_coords)

                #pdb.set_trace()

                if not (N_old or N_new):
                    report_file.write( 'Nothing added or deleted \n')

                if N_old > 0:
                    print('Warning: %s %d: You apparently got rid of %d from the old table'%(segment, hv, N_old))
                    report_file.write( '%d entries have been removed from the old table. \n'%(N_old) )
                    report_file.write( '\n'.join( map(str,only_old_coords) ) )
                    report_file.write('\n\n')

                if N_new > 0:
                    print('%s %d: You added %d to the new table'%( segment, hv, N_new ))
                    report_file.write( '%d entries have been added to the new table. \n'%( N_new ) )
                    report_file.write( '\n'.join( map(str,only_new_coords) )  )
                    report_file.write('\n\n')

                for old_y,old_x,old_mjd in old_regions:
                    coord_pair = (old_y,old_x)
                    if coord_pair not in both_coords: continue

                    ###again, may be confusing syntax
                    for new_y,new_x,new_mjd in new_regions:
                        if (new_y,new_x) == coord_pair:
                            break

                    mjd_difference = old_mjd - new_mjd
                    if mjd_difference:
                        print('MJD difference of %5.7f days'%(mjd_difference))
                        print('at (y,x):  (%d,%d)'%coord_pair)
                        report_file.write( 'MJD difference of %5.7f days at (y,x):  (%d,%d)'%(mjd_difference,coord_pair[0],coord_pair[1]) )

#------------------------------------------------------------

def test_gsag_calibration(gsagtab):
    """Move gsagtab into TEST_DIR and calibrate with CalCOS.

    Any datasets that fail calibration will be emailed to the user.
    """
    print('#-------------------------#')
    print('Calibrating with %s'%(gsagtab))
    print('#-------------------------#')

    os.environ['testdir'] = TEST_DIR
    if not os.path.exists(TEST_DIR):
        os.mkdir(TEST_DIR)
    shutil.copy( gsagtab ,os.path.join(TEST_DIR,'new_gsag.fits') )

    test_datasets = glob.glob( os.path.join(TEST_DIR, '*rawtag_a.fits') )

    #Remove products
    for ext in ('*_counts*.fits','*_flt*.fits','*_x1d*.fits','*lampflash*.fits','*corrtag*.fits'):
        os.system('rm '+TEST_DIR+'/'+ext)

    for item in test_datasets:
        fits.setval( item,'RANDSEED',value=8675309,ext=0 )
        fits.setval( item,'GSAGTAB',value='testdir$new_gsag.fits',ext=0 )

    failed_runs = []
    for item in test_datasets:
        try:
            status = calcos.calcos( item,outdir=TEST_DIR )
            print(("CalCOS exit status is",status))
        except:
            failed_runs.append( item )

        if status != 0:
            failed_runs.append( item )

    if len(failed_runs):
        send_email(subject='GSAGTAB Calibration Error',message='Failed calibration\n\n'+'\n'+'\n'.join(failed_runs) )

    ###Now run some quick test.

#------------------------------------------------------------

def send_forms(data_dir):
    """Compose CDBS delivery form and email a copy to user
    """
    ###Needs some modifications
    today_obj = datetime.today()
    today = str(today_obj.month)+'/'+str(today_obj.day)+'/'+str(today_obj.year)
    message = '1-Name of deliverer: Justin Ely\n'
    message += ' (other e-mail addresses) proffitt@stsci.edu,oliveira@stsci.edu,\n'
    message += 'duval@stsci.edu,sahnow@stsci.edu\n'
    message += '\n'
    message += ' 2-Date of delivery: '+today+'\n'
    message += '\n'
    message += ' 3-Instrument: COS \n'
    message += '\n'
    message += ' 4-Type of file (bias,pht,etc.): gsag \n'
    message += '\n'
    message += ' 5-Has HISTORY section in header [0] been updated to describe in detail\n'
    message += '   why it is being delivered and how the file was created? (yes/no): yes \n'
    message += '\n'
    message += ' 6-USEAFTER, PEDIGREE, DESCRIP, and COMMENT have been checked? yes \n'
    message += '\n'
    message += ' 6a-Was the DESCRIP keyword updated with a summary of why the file was updated or created? \n'
    message += '   (yes/no) yes \n'
    message += '\n'
    message += ' 6b-If the reference files are replacing previous versions, do the new USEAFTER dates \n'
    message += '    exactly match the old ones? yes \n'
    message += '\n'
    message += ' 7-CDBS Verification complete? (fitsverify,certify,etc.): yes \n'
    message += '\n'
    message += ' 8-Should these files be ingested in the OPUS, DADS and CDBS databases? yes \n'
    message += '   (if not indicate it clearly which ones):\n'
    message += '\n'
    message += ' 8a-If files are synphot files, should they be delivered to ETC? N/A\n'
    message += '\n'
    message += ' 9-Files run through CALXXX or SYNPHOT in the IRAF version of STSDAS and the IRAF* \n'
    message += '   version used by the Archive pipeline? (yes/no): yes \n'
    message += '   List the versions used: CalCOS v 2.18.5 \n'
    message += '\n'
    message += ' 10-Does it replace an old reference file? (yes/no): yes \n'
    message += '\n'
    message += ' 10a-If yes, which one? \n'
    message += '     (If the file being replaced is bad, and should not be used with any data, please\n'
    message += '      indicate this here.)\n'
    message += '\n'
    message += ' 11- What is the level of change of the file? (e.g. compared to old file it\n'
    message += '     could be: SEVERE, MODERATE, TRIVIAL, 1\%, 5\% etc.): SEVERE\n'
    message += '\n'
    message += ' 11a-If files are tables, please indicate exactly which rows have changed. Show output \n'
    message += '     of compare_table.pro.\n'
    message += '     Table was compared to the previous by the compare_gsag function in gsag.py as part\n'
    message += '     of the monitor.py routine monitoring program.  This function tests if any HV extensions \n'
    message += '     are added or removed, and if individual flagged regions have changed in each HV extension. \n'
    message += '     A summary difference report is generated and checked to ensure all changes are desired, along\n'
    message += '     with follow-up graphical representations for the table impact on data. \n'
    message += '\n'
    message += ' 12-Please indicate which modes (e.g. all the STIS,FUVMAMA,E140l moes) are affected by the  \n'
    message += '     changes in the file. \n'
    message += '     All COS FUV modes are affected by this file. \n'
    message += '\n'
    message += ' 13-Description of how the files were "tested" for correctness: CalCOS v 2.18.5 was run \n'
    message += '     on a test suite of data to ensure no calibration errors were introduced with this file. \n'
    message += '     Additionally, the regions flagged in this file have been overplotted to modal gain maps\n'
    message += '     of every DETHV level for each week since operations began to ensure that flagged regions\n'
    message += '     always overlay areas of low modal gain\n'
    message += '\n'
    message += ' 14-Additional Considerations: \n'
    message += '\n'
    message += ' 15-Reason for delivery: New regions have been identified as "bad" and need to be flagged and \n'
    message += '     removed in the final extracted spectra.\n'
    message += '\n '
    message += '16-Disk location and name of files:\n'

    initial_dir = os.getcwd()
    os.chdir( data_dir )

    message += os.getcwd()+'\n'
    here = os.getcwd()
    os.system('ls -la gsag_%s.fits > tmp.txt'%(TIMESTAMP) )
    tmp = open('tmp.txt','r')
    for line in tmp.readlines():
        message += line
    os.remove('tmp.txt')

    delivery_form = open( os.path.join( data_dir, 'deliveryform.txt'),'w' )
    delivery_form.write(message)

    try:
        send_email(subject='COS GSAGTAB Delivery Form',message=message)
    except:
        logger.warning("could not send delivery form")

#------------------------------------------------------------

def get_coords(txt):
    """Grabs coordinates from flagged bad txt file
    """
    coords = []
    lines = []
    try:
        lines = np.genfromtxt(txt, dtype=None, skiprows=2)
        coords = [(line[0],line[1]) for line in lines]
    except:
        pass

    return coords,lines

#------------------------------------------------------------

def populate_down(gsag_file):
    """Copies found locations from one HV level to each lower
    HV level if the coords have not already been flagged.

    A regions will always be flagged at the MJD found in that
    HV setting, if it has been found.  Else, the MJD will be
    the time when the region was flagged in the higher HV
    setting. In the event we go back down to a lower voltage,
    and a region is newly flagged as bad in that HV, it is
    possible that the MJD would change from one gsagtab to
    the next.  This does not pose a problem for data as no
    data will have been taken at the lower HV in the
    intervening time.
    """

    print('#---------------------------------------------#')
    print('Populating flagged regions to lower HV settings')
    print('#---------------------------------------------#')
    gsagtab = fits.open(gsag_file)
    for segment,hv_keyword in zip( ['FUVA','FUVB'], ['HVLEVELA','HVLEVELB'] ):
        all_hv = [ (ext.header[hv_keyword],i+1) for i,ext in enumerate(gsagtab[1:]) if ext.header['segment'] == segment ]
        all_hv.sort()

        for current_dethv,current_ext in all_hv:
            current_lines = [ tuple(line) for line in gsagtab[current_ext].data[0].array ]
            current_coords= [ (line[1],line[2]) for line in current_lines ]
            N_changes = 0

            for higher_dethv,higher_ext in all_hv:
                if not (higher_dethv > current_dethv):
                    continue
                higher_lines = [ tuple(line) for line in gsagtab[higher_ext].data[0].array ]
                higher_coords= [ (line[1],line[2]) for line in higher_lines ]

                for coord, line in zip(higher_coords,higher_lines):
                    if not (coord in current_coords):
                        # If coordinate from higher HV is not in current HV, append
                        current_lines.append( line )
                        current_coords.append( (line[1],line[2]) )
                        N_changes += 1

                    else:
                        # If coordinated from higher HV is in current HV,
                        # check to see if MJD is earlier.  If yes, take new value.
                        # MJD is first element in tuple e.g. line[0]
                        index = current_coords.index( coord )
                        current_line = current_lines[index]

                        if line[0] < current_line[0]:
                            current_lines[ index ] = line
                            print(('--Earlier time found',line[0],'-->',current_line[0]))
                            N_changes += 1


            if N_changes:
                print(('Updating %s/%d ext:%d with %d changes'%(segment,current_dethv,current_ext,N_changes)))
                current_lines.sort()
                date = [ line[0] for line in current_lines ]
                lx = [ line[1] for line in current_lines ]
                ly = [ line[2] for line in current_lines ]
                dx = [ line[3] for line in current_lines ]
                dy = [ line[4] for line in current_lines ]
                dq = [ line[5] for line in current_lines ]
                gsagtab[current_ext] = gsagtab_extension(date, lx, dx, ly, dy, dq, current_dethv, hv_keyword, segment)
            else:
                print(('No Changes to %s/%d ext:%d '%(segment,current_dethv,current_ext)))

    gsagtab.writeto(gsag_file,clobber=True)

#------------------------------------------------------------

def gsagtab_extension(date, lx, dx, ly, dy, dq, dethv, hv_string, segment):
    """Creates a properly formatted gsagtab table from input columns
    """
    lx = np.array(lx)
    ly = np.array(ly)
    dx = np.array(dx)
    dy = np.array(dy)
    dq = np.array(dq)
    date_col = fits.Column('DATE','D','MJD',array=date)
    lx_col = fits.Column('LX','J','pixel',array=lx)
    dx_col = fits.Column('DX','J','pixel',array=dx)
    ly_col = fits.Column('LY','J','pixel',array=ly)
    dy_col = fits.Column('DY','J','pixel',array=dy)
    dq_col = fits.Column('DQ','J','',array=dq)
    tab = fits.new_table([date_col,lx_col,ly_col,dx_col,dy_col,dq_col])

    tab.header.add_comment(' ',after='TFIELDS')
    tab.header.add_comment('  *** Column formats ***',after='TFIELDS')
    tab.header.add_comment(' ',after='TFIELDS')
    tab.header.update(hv_string,dethv,after='TFIELDS',comment='High voltage level')
    tab.header.update('SEGMENT',segment,after='TFIELDS')
    tab.header.add_comment(' ',after='TFIELDS')
    tab.header.add_comment('  *** End of mandatory fields ***',after='TFIELDS')
    tab.header.add_comment(' ',after='TFIELDS')

    return tab

#------------------------------------------------------------

def date_string( date_time ):
    """ Takes a datetime object and returns
    a pedigree formatted string.
    """

    day = str(date_time.day)
    month = str(date_time.month)
    year = str(date_time.year)

    if len(day) < 2:
        day = '0' + day

    if len(month) < 2:
        month = '0' + month

    return day + '/' + month + '/' + year

#------------------------------------------------------------

def make_gsagtab():
    """Create GSAGTAB from flagged locations.

    Grabs txt files of flagged bad regions from MONITOR_DIR
    and combines them into a gsagtab.

    Parameters
    ----------
    None

    Returns
    -------
    None

    Products
    --------
    new_gsagtab.fits
    """
    print('Making new GSAGTAB')
    out_fits = os.path.join(MONITOR_DIR,'gsag_%s.fits'%(TIMESTAMP) )
    input_list = glob.glob(os.path.join(MONITOR_DIR,'flagged_bad_??_cci_???.txt'))
    input_list.sort()

    #Populates regions found in HV == X, Segment Y, to any
    #extensions of lower HV for same segment.

    hdu_out=fits.HDUList(fits.PrimaryHDU())
    date_time = str(datetime.now())
    date_time = date_time.split()[0]+'T'+date_time.split()[1]
    hdu_out[0].header.update('DATE',date_time,'Creation UTC (CCCC-MM-DD) date')
    hdu_out[0].header.update('TELESCOP','HST')
    hdu_out[0].header.update('INSTRUME','COS')
    hdu_out[0].header.update('DETECTOR','FUV')
    hdu_out[0].header.update('COSCOORD','USER')
    hdu_out[0].header.update('VCALCOS','2.0')
    hdu_out[0].header.update('USEAFTER','May 11 2009 00:00:00')

    today_string = date_string(datetime.now())
    hdu_out[0].header.update('PEDIGREE','INFLIGHT 25/05/2009 %s'%( today_string  ))
    hdu_out[0].header.update('FILETYPE','GAIN SAG REFERENCE TABLE')

    descrip_string = 'Gives locations of gain-sag regions as of %s'%( str(datetime.now().date() ))
    while len(descrip_string) < 67:
        descrip_string += '-'
    hdu_out[0].header.update('DESCRIP',descrip_string )
    hdu_out[0].header.update('COMMENT',"= 'This file was created by J. Ely'")
    hdu_out[0].header.add_history('Flagged region source files can be found here:')
    for item in input_list:
        hdu_out[0].header.add_history('%s'%(item))
    hdu_out[0].header.add_history('')
    hdu_out[0].header.add_history('Flagged regions in higher voltages have been backwards populated')
    hdu_out[0].header.add_history('to all lower HV levels for the same segment.')
    hdu_out[0].header.add_history('')
    hdu_out[0].header.add_history('A region will be flagged as bad when the detected')
    hdu_out[0].header.add_history('flux is found to drop by 5%.  This happens when')
    hdu_out[0].header.add_history('the measured modal gain of a region falls to ')
    hdu_out[0].header.add_history('%d given current lower pulse height filtering.'%(MODAL_GAIN_LIMIT) )

    possible_hv_strings = ['000','100'] + list(map(str,list(range(142,179))))

    for segment_string in [FUVA_string,FUVB_string]:
        for hv_level_st in possible_hv_strings:
            hv_level = int( hv_level_st )

            if segment_string == FUVA_string:
                segment = 'FUVA'
                HVLEVEL_string ='HVLEVELA'

            elif segment_string == FUVB_string:
                segment = 'FUVB'
                HVLEVEL_string = 'HVLEVELB'

            infile = os.path.join( MONITOR_DIR, "flagged_bad_%s_%s.txt"%(segment_string,hv_level_st) )

            date = []
            lx = []
            dx = []
            ly = []
            dy = []
            dq = []
            if os.path.exists( infile ):
                txt = open(infile,'rU')
                txt.readline()
                txt.readline()
                for line in txt.readlines():
                    line = line.strip().split()
                    for i in range(len(line)):
                        line[i] = float(line[i])

                    if len(line) != 5:
                        print('Skipping')
                        continue

                    lx.append( line[0] )
                    dx.append( line[1] )
                    ly.append( line[2] )
                    dy.append( line[3] )
                    date.append( line[4] )
                    dq.append( 8192 )

            if not len(lx):
                #Extension tables cannot have 0 entries, a
                #region of 0 extent centered on (0,0) is
                #sufficient to prevent CalCOS crash.
                date.append( 0 )
                lx.append( 0 )
                ly.append( 0 )
                dx.append( 0 )
                dy.append( 0 )
                dq.append( 8192 )

            tab = gsagtab_extension( date,lx,dx,ly,dy,dq,hv_level,HVLEVEL_string,segment)

            hdu_out.append(tab)

    hdu_out.writeto(out_fits,clobber=True)
    print(('WROTE: GSAGTAB to %s'%(out_fits)))
    return out_fits

#------------------------------------------------------------

def in_boundary(segment, ly, dy):
    boundary = {'FUVA': 493, 'FUVB': 557}
    padding = 4

    boundary_pix = set(np.arange(boundary[segment]-padding,
                                 boundary[segment]+padding+1))

    affected_pix = set(np.arange(ly, ly+dy+1))

    if affected_pix.intersection(boundary_pix):
        return True

    return False

#------------------------------------------------------------

def make_gsagtab_db(out_dir, blue=False):
    """Create GSAGTAB from flagged locations.

    Grabs txt files of flagged bad regions from MONITOR_DIR
    and combines them into a gsagtab.

    Parameters
    ----------
    None

    Returns
    -------
    None

    Products
    --------
    new_gsagtab.fits
    """

    out_fits = os.path.join(out_dir, 'gsag_%s.fits'%(TIMESTAMP))
    #Populates regions found in HV == X, Segment Y, to any
    #extensions of lower HV for same segment.

    hdu_out=fits.HDUList(fits.PrimaryHDU())
    date_time = str(datetime.now())
    date_time = date_time.split()[0]+'T'+date_time.split()[1]
    hdu_out[0].header.update('DATE',date_time,'Creation UTC (CCCC-MM-DD) date')
    hdu_out[0].header.update('TELESCOP','HST')
    hdu_out[0].header.update('INSTRUME','COS')
    hdu_out[0].header.update('DETECTOR','FUV')
    hdu_out[0].header.update('COSCOORD','USER')
    hdu_out[0].header.update('VCALCOS','2.0')
    hdu_out[0].header.update('USEAFTER','May 11 2009 00:00:00')
    hdu_out[0].header['CENWAVE'] = 'N/A'

    today_string = date_string(datetime.now())
    hdu_out[0].header.update('PEDIGREE','INFLIGHT 25/05/2009 %s'%( today_string  ))
    hdu_out[0].header.update('FILETYPE','GAIN SAG REFERENCE TABLE')

    descrip_string = 'Gives locations of gain-sag regions as of %s'%( str(datetime.now().date() ))
    while len(descrip_string) < 67:
        descrip_string += '-'
    hdu_out[0].header.update('DESCRIP',descrip_string )
    hdu_out[0].header.update('COMMENT',"= 'This file was created by J. Ely'")
    hdu_out[0].header.add_history('Flagged regions in higher voltages have been backwards populated')
    hdu_out[0].header.add_history('to all lower HV levels for the same segment.')
    hdu_out[0].header.add_history('')
    hdu_out[0].header.add_history('A region will be flagged as bad when the detected')
    hdu_out[0].header.add_history('flux is found to drop by 5%.  This happens when')
    hdu_out[0].header.add_history('the measured modal gain of a region falls to ')
    hdu_out[0].header.add_history('%d given current lower pulse height filtering.'%(MODAL_GAIN_LIMIT) )

    possible_hv_strings = ['000', '100'] + list(map(str, list(range(142, 179))))

    #--working on it
    print("Connecting")

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    connection = engine.connect()

    results = connection.execute("""SELECT DISTINCT segment FROM gain WHERE concat(segment,x,y) IS NOT NULL""")

    segments = [item[0] for item in results]

    print(("looping over segments, {}".format(segments)))
    for seg in segments:
        hvlevel_string = 'HVLEVEL' + seg[-1].upper()

        for hv_level in possible_hv_strings:
            date = []
            lx = []
            dx = []
            ly = []
            dy = []
            dq = []

            hv_level = int(hv_level)
            print((seg, hv_level))
            results = connection.execute("""SELECT DISTINCT x,y
                                            FROM flagged WHERE segment='%s'
                                            and dethv>='%s'
                                            and concat(x,y) IS NOT NULL
                                            """
                                            %(seg, hv_level)
                                            )

            coords = [(item[0], item[1]) for item in results]

            for x, y in coords:
                results = connection.execute("""SELECT MJD
                                                FROM flagged
                                                WHERE segment='%s'
                                                AND x='%s'
                                                AND y='%s'
                                                AND dethv>='%s'
                                                AND concat(x,y) IS NOT NULL
                                                """
                                                %(seg, x, y, hv_level)
                                                )

                flagged_dates = [item[0] for item in results]
                if len(flagged_dates):
                    bad_date = min(flagged_dates)
                else:
                    continue

                if blue and in_boundary(seg, y*Y_BINNING, Y_BINNING):
                    print(("Excluding for blue modes: {} {} {}".format(seg, y*Y_BINNING, Y_BINNING)))
                    continue


                lx.append(x*X_BINNING)
                dx.append(X_BINNING)
                ly.append(y*Y_BINNING)
                dy.append(Y_BINNING)
                date.append(bad_date)
                dq.append(8192)

            if not len(lx):
                #Extension tables cannot have 0 entries, a
                #region of 0 extent centered on (0,0) is
                #sufficient to prevent CalCOS crash.
                lx.append(0)
                ly.append(0)
                dx.append(0)
                dy.append(0)
                date.append(0)
                dq.append(8192)

            print((len(date), ' found bad regions'))
            tab = gsagtab_extension(date, lx, dx, ly, dy, dq, hv_level, hvlevel_string, seg)
            hdu_out.append(tab)

    if blue:
        out_fits = out_fits.replace('.fits', '_blue.fits')
        hdu_out[0].header['CENWAVE'] = 'BETWEEN 1055 1097'

        descrip_string = 'Blue-mode gain-sag regions as of %s'%(str(datetime.now().date()))
        while len(descrip_string) < 67:
            descrip_string += '-'
        hdu_out[0].header['DESCRIP'] = descrip_string

    hdu_out.writeto(out_fits, clobber=True)
    print(('WROTE: GSAGTAB to %s'%(out_fits)))
    return out_fits

#------------------------------------------------------------

def get_cdbs_gsagtab():
    """Retrieve most recently delivered GSAGTAB from CDBS
    for comparison with the one just made.
    """
    gsag_tables = glob.glob(os.path.join(os.environ['lref'], '*gsag.fits'))
    creation_dates = np.array([fits.getval(item, 'DATE') for item in gsag_tables])
    current_gsagtab = gsag_tables[creation_dates.argmax()]

    return current_gsagtab

#------------------------------------------------------------
