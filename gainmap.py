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
import sys
import time
from datetime import datetime
import gzip
import pickle
import glob

from astropy.io import fits as pyfits
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import scipy
from scipy.optimize import leastsq, newton, curve_fit
import calcos

import cci_read

import multiprocessing as mp
import subprocess as sp

from ..support import rebin, init_plots, Logger, enlarge, progress_bar, send_email
from constants import *  ## I know this is bad, but shut up.

#------------------------------------------------------------

class CCI_object:
    """Creates a cci_object designed for use in the monitor.
    
    Each COS cumulative image fits file is made into its
    own cci_object where the data is more easily used.
    Takes a while to make, contains a few large arrays and 
    various header keywords from the original file.
    """
    def __init__(self,CCI):
        """Open CCI file and create CCI Object"""
        self.input_file = CCI
        path,cci_file = os.path.split(CCI)
        if cci_file[-3:]=='.gz':
            cci_file = cci_file[:-3]
        file_name,ext = os.path.splitext(cci_file)
        self.input_file_name = file_name
        self.open_fits()
       
    def open_fits(self):
        """Open CCI file and populated attributes with 
        header keywords and data arrays.
        """
        print '\nOpening %s'%(self.input_file_name)

        if self.input_file.endswith('.gz'):
            fits = pyfits.open(gzip.open(self.input_file),memmap = False)
        else:
            fits = pyfits.open(self.input_file)

        assert (fits['SCI',1].data.shape == (Y_UNBINNED,X_UNBINNED)),'ERROR: Input CCI not standard dimensions'

        self.KW_XBINNING = X_BINNING
        self.KW_YBINNING = Y_BINNING

        self.KW_DETECTOR = fits[0].header['DETECTOR']
        self.KW_SEGMENT = fits[0].header['SEGMENT']
        self.KW_OBSMODE = fits[0].header['OBSMODE']
        self.KW_EXPSTART = fits[0].header['EXPSTART']
        self.KW_EXPEND = fits[0].header['EXPEND']
        self.KW_EXPTIME = fits[0].header['EXPTIME']
        self.KW_NUMFILES = fits[0].header['NUMFILES']
        self.KW_COUNTS = fits[0].header['COUNTS']

        try: dethv = fits[0].header['DETHV']
        except: dethv = -999
        self.KW_DETHV = dethv


        if self.KW_EXPSTART:
            hvtab = pyfits.open(HVTAB)
            if self.KW_SEGMENT == 'FUVA':
                hv_string = 'HVLEVELA'
            elif self.KW_SEGMENT == 'FUVB':
                hv_string = 'HVLEVELB'

            dethv_index = np.where(hvtab[self.KW_SEGMENT].data['DATE'] < self.KW_EXPSTART)[0][-1]
            found_DETHV = hvtab[self.KW_SEGMENT].data[hv_string][dethv_index]

            #assert ( found_DETHV == self.DETHV ), 'ERROR:  HVTAB does not agree with DETHV! %d != %d'%(found_DETHV,self.DETHV)
            if found_DETHV != self.KW_DETHV: 
               print 'WARNING, HVTAB does not agree with DETHV'
               print found_DETHV,self.KW_DETHV
        
        self.file_list = [ line['rootname'] for line in fits[1].data ]

        self.make_c_array() 
        #self.make_big_array(fits)
        self.get_counts(self.big_array)
        self.extracted_charge = self.pha_to_coulombs(self.big_array)

        self.modal_gain_array = np.zeros((YLEN,XLEN))
        self.modal_gain_width = np.zeros((YLEN,XLEN))
        
        self.KW_cnt00_00 = len(self.big_array[0].nonzero()[0])
        self.KW_cnt01_01 = len(self.big_array[1].nonzero()[0])
        self.KW_cnt02_30 = len(self.big_array[2:31].nonzero()[0])
        self.KW_cnt31_31 = len(self.big_array[31:].nonzero()[0])

        fits.close()
        del fits

    def make_c_array(self):
        print 'Making 3D array from pha extensions:'
        c_array = cci_read.read( self.input_file ).reshape( (32,1024,16384) )
        self.big_array = np.array( [rebin(c_array[i],bins = (Y_BINNING,X_BINNING)) for i in xrange(32) ] )

    def make_big_array(self,fits):
        """Takes an input COS cci fits file and returns a 3D array
        of x pixel, y pixel, and pulse height amplitude (PHA) counts.

        Takes a very long time.
        """
        print 'Making 3D array from pha extensions:'
        big_array = np.array([rebin(extension.data,bins = (Y_BINNING,X_BINNING)) 
                               for i,extension in enumerate(fits[2:]) if os.write(1,'-')])
        os.write(1,'\n')
        self.big_array = big_array

    def get_counts(self, in_array):
        """collapse pha arrays to get total counts accross all
        PHA bins.  

        Will also search for and add in accum data if any exists.
        """
        print 'Making array of cumulative counts'   
        out_array = np.sum(in_array,axis=0)

        ###Test before implementation
        ###Should only effect counts and charge extensions.
        ### no implications for modal gain arrays or measurements
        if self.KW_SEGMENT == 'FUVA':
            accum_name = self.input_file_name.replace('-00_cci','-02_cci')  ##change when using OPUS data
        elif self.KW_SEGMENT == 'FUVB':
            accum_name = self.input_file_name.replace('-01_cci','-03_cci')  ##change when using OPUS data
        else:
            accum_name = None
            print 'ERROR: name not standard'

        if os.path.exists(accum_name):
            print 'Adding in Accum data'
            accum_data = rebin(pyfits.getdata(CCI_DIR+accum_name, 0),bins=(Y_BINNING,X_BINNING))
            out_array += accum_data
            self.accum_data=accum_data
        else:
            self.accum_data=None

        self.counts = out_array

    def pha_to_coulombs(self, in_array):
        """Convert pha to picocoloumbs to calculate extracted charge.

        Equation comes from D. Sahnow.
        """
        print 'Making array of extracted charge'
        coulomb_value = 1.0e-12*10**((np.array(range(0,32))-11.75)/20.5)
        zlen,ylen,xlen = in_array.shape
        out_array = np.zeros((ylen,xlen))
        for pha,layer in enumerate(in_array):
            out_array += (coulomb_value[pha]*layer)
        return out_array

#------------------------------------------------------------

def make_all_hv_maps():
    for hv in range(150, 179):
        tmp_hdu = pyfits.open( os.path.join( MONITOR_DIR, 'total_gain.fits') )
        for ext in (1, 2):
            tmp_hdu[ext].data -= .393 * (float(178) - hv)
        tmp_hdu.writeto( os.path.join( MONITOR_DIR, 'total_gain_%d.fits' % hv ), clobber=True )

#------------------------------------------------------------

def make_total_gain( segment, start_mjd=55055, end_mjd=70000, min_hv=163, max_hv=175, reverse=False ):
    if segment == 'FUVA':
        ending = '*00_cci_gainmap.fits'
    elif segment == 'FUVB':
        ending = '*01_cci_gainmap.fits'

    all_datasets = [ item for item in glob.glob( os.path.join( MONITOR_DIR, ending) ) ]

    all_datasets.sort()
    if reverse:
        all_datasets = all_datasets[::-1]

    out_data = np.zeros( (YLEN, XLEN) )

    for item in all_datasets:
        cci_hdu = pyfits.open( item )
        if not cci_hdu[0].header['EXPSTART'] > start_mjd: continue
        if not cci_hdu[0].header['EXPSTART'] < end_mjd: continue
        if not cci_hdu[0].header['DETHV'] > min_hv: continue
        if not cci_hdu[0].header['DETHV'] < max_hv: continue
        cci_data = cci_hdu[ 'MOD_GAIN' ].data

        dethv = cci_hdu[0].header['DETHV']
 
        index = np.where( cci_data )
        cci_data[index] += .393 * (float(178) - dethv)

        index_both = np.where( (cci_data > 0) & (out_data > 0) ) 
        mean_data = np.mean( [cci_data, out_data], axis=0 )

        out_data[index] = cci_data[index]
        out_data[index_both] = mean_data[index_both]

    return enlarge(out_data, x=X_BINNING, y=Y_BINNING )


#------------------------------------------------------------

def make_all_gainmaps(processors=1):
    """ Main driver for monitoring program.

    """

    for ending in [FUVA_string,FUVB_string]:
        CCI_list = glob.glob( CCI_DIR + '*'+ending+'*')
        CCI_list.sort()
        if processors == 1:
            for CCI in CCI_list:
                process_cci( CCI )
        else:
            pool = mp.Pool(processes = int(processors))
            pool.map(process_cci,CCI_list)

        add_cumulative_data(ending)

    populate_keywords_mine()

    hdu_out = pyfits.HDUList(pyfits.PrimaryHDU())
    hdu_out.append(pyfits.ImageHDU( data = make_total_gain( 'FUVA', reverse=True ) ) )
    hdu_out[1].header['EXTNAME'] = 'FUVAINIT'
    hdu_out.append(pyfits.ImageHDU( data = make_total_gain( 'FUVB', reverse=True ) ) )
    hdu_out[2].header['EXTNAME'] = 'FUVBINIT'
    hdu_out.append(pyfits.ImageHDU( data = make_total_gain( 'FUVA') ) )
    hdu_out[3].header['EXTNAME'] = 'FUVALAST'
    hdu_out.append(pyfits.ImageHDU( data = make_total_gain( 'FUVB') ) )
    hdu_out[4].header['EXTNAME'] = 'FUVBLAST'
    hdu_out.writeto( os.path.join( MONITOR_DIR, 'total_gain.fits' ), clobber=True )
    hdu_out.close()


    make_all_hv_maps()

#------------------------------------------------------------

def add_cumulative_data(ending):
    """add cumulative counts and charge to each file
    Will overwrite current data, so if files are added
    in middle of list, they should be accounted for.
    """
    data_list = glob.glob( os.path.join(MONITOR_DIR,'*%s*gainmap.fits'%ending) )
    data_list.sort()
    print 'Adding cumulative data to gainmaps for %s'%(ending)    
    shape = pyfits.getdata( data_list[0] ,ext=('MOD_GAIN',1)).shape
    total_counts = np.zeros( shape )
    total_charge = np.zeros( shape )

    for cci_name in data_list:
        fits = pyfits.open(cci_name,mode='update')
        total_counts += fits['COUNTS'].data
        total_charge += fits['CHARGE'].data

        ext_names = [ ext.name for ext in fits ]

        if 'CUMLCNTS' in ext_names:
            fits['CUMLCNTS'].data = total_counts
        else:
            head_to_add = pyfits.Header()
            head_to_add.update('EXTNAME','CUMLCNTS')
            fits.append(pyfits.ImageHDU(header=head_to_add, data=total_counts))

        if 'CUMLCHRG' in ext_names:
            fits['CUMLCHRG'].data = total_charge
        else:
            head_to_add = pyfits.Header()
            head_to_add.update('EXTNAME','CUMLCHRG')
            fits.append(pyfits.ImageHDU(header=head_to_add, data=total_charge))

        fits.flush()
        fits.close()

#------------------------------------------------------------

def process_cci(CCI):
    """Necessary to pass to pool.map()
    for parallelization of the make_gainmap() function
    """
    current = open_cci(CCI)
    if current: make_gainmap(current) 

#------------------------------------------------------------

def perform_fit( distribution, sigma_low=0, sigma_high=10):
    gain_flag = ''

    ###Fit in charge space???
    fit_parameters,success = fit_gaussian( distribution )
    fit_height, fit_center, fit_std = fit_parameters
    variance = fit_std*fit_std

    trial_2, success_2 = fit_gaussian(distribution, b = fit_center - 4)
    trial_3, success_3 = fit_gaussian(distribution, b = fit_center + 4)

    center_2 = trial_2[1]
    center_3 = trial_3[1]

    #---Begin checking for bad fits---#

    if ( (center_2 > 31) | (center_2 < 0 ) ): center_2 = fit_center
    if ( (center_3 > 31) | (center_3 < 0 ) ): center_3 = fit_center
    if ( (fit_center > 31) | (fit_center < 0 ) ): 
        #print 'Center way off'
        gain_flag += 'center_'

    if ( (center_2 < fit_center - 1) | (center_3 > fit_center + 1) ):
        #print 'Warning: More than one fit found'# @ %3.4f %3.4f %3.4f'%(fit_center, center_2, center_3)
        gain_flag += 'peaks_'
    else:
        fit_center = np.median([fit_center,trial_2[1],trial_3[1]])

    if not success: 
        #print 'ERROR: Gaussian fit failed internal to routine'# at (x,y): (%d,%d).  Setting gain to 0'%(x,y)
        gain_flag += 'failure_'

    if (variance < sigma_low) | (variance > sigma_high):
        #print 'ERROR: dist. excluded'# at x=%d, y=%d. Cts: %4.2f Var: %3.2f'%(x,y,distribution.sum(),variance)
        gain_flag += 'variance_'

    if np.isnan(fit_center):
        #print 'ERROR: dist. excluded'# at x=%d, y=%d. Cts: %4.2f Var: %3.2f'%(x,y,distribution.sum(),variance)
        gain_flag += 'nan_'

    #if gain_flag:
    #    fit_center = 0

    return fit_center, gain_flag

#------------------------------------------------------------

def measure_gainimage( data_cube, mincounts=30, phlow=1, phhigh=31 ):
    """ measure the modal gain at each pixel 

    returns a 2d gainmap

    """

    # Suppress certain pharanges
    for i in range(0, phlow+1) + range(phhigh, len(data_cube) ):
        data_cube[i] = 0

    #zsize, ysize, xsize = data_cube.shape
    #out_gain = np.array(map( perform_fit, data_cube.T.reshape( (ysize*xsize,zsize) ) )).reshape( (ysize, xsize) )

    collapsed = np.sum( data_cube, axis=0 )
   
    out_gain = np.zeros( collapsed.shape )
    index = np.where( collapsed >= mincounts )
 
    if not len(index):
        return out_gain

    measurements = [ (perform_fit( data_cube[:,y,x]) ) 
                     for y,x in zip( *index ) ]
    
    gain_fits = [ item[0] for item in measurements ]
    gain_flags = [ item[1] for item in measurements ]

    for y, x, modal_gain, flag in zip(index[0], index[1], gain_fits, gain_flags):
        if not flag:
            out_gain[y, x] = modal_gain
 
    return out_gain
    

#------------------------------------------------------------

def make_gainmap(current):
    """Make modal gainmap for cos cumulative image.

    Modal gain array is created for each cumulative image and
    output to pdf and fits files.  The gain distribution is 
    fit and excluded by various parameters.

    Parameters
    ----------
    current_cci: cci object
        the current cci_object

    Returns
    -------
    None

    Products
    --------
    *gainmap.fits
    *gainmap.png

    """

    print 
    print 'Creating Modal Gain Map'
    print 'for %s'%(current.input_file_name)
    
    if not current.KW_NUMFILES:
        print 'CCI contines no data.  Skipping modal gain measurements'
        write_gainmap( current )
        return 

    dist_output_name = os.path.join( MONITOR_DIR, current.input_file_name + '_dist.txt')
    print 'Distribution file will be written to: ',dist_output_name
    dist_file = open( dist_output_name ,'w')

    previous_list = get_previous(current)


    mincounts = 30
    print 'Adding in previous data to distributions'
    for past_CCI in previous_list:
        print past_CCI.input_file_name
        index = np.where( np.sum( current.big_array[1:31], axis=0 ) <= mincounts )
  
        for y, x in zip( *index ):
            prev_dist = past_CCI.big_array[:, y, x]
            if prev_dist.sum() > mincounts: 
                continue
            else:
                current.big_array[:, y, x] += prev_dist


    current.modal_gain_array = measure_gainimage( current.big_array )
    if current.accum_data:
        current.extracted_charge += current.accum_data*(1.0e-12*10**((modal_gain_array-11.75)/20.5))

    write_gainmap( current )

    """
                if gain_flag == '':
                    gain_flag = 'fine'
                    fit_modal_gain = fit_center
                    fit_gain_width = fit_std

                    current.modal_gain_array[y,x] = fit_modal_gain
                    current.modal_gain_width[y,x] = fit_gain_width

                    if fit_center > 21:
                        print '##########################'
                        print 'WARNING  MODAL GAIN: %3.2f'%(fit_center)
                        print 'Modal gain has been measured to be greater than 21. '
                        print 'PHA upper limit of 23 may cause flux to be lost.'
                        print '##########################'
                        send_email( subject='CCI high modal gain found', 
                                    message='Modal gain of %3.2f found on segment %s at (x,y,MJD) %d,%d,%5.5f'%(fit_center,current.KW_SEGMENT,x,y,current.KW_EXPSTART) )
                        plot_gaussian_fit(gain_dist,fit_parameters,'WARNING_%s_%s_xy_%d_%d_%5.2f.png'%('g21',current.KW_SEGMENT,x,y,current.KW_EXPSTART))

                    
                    ###Used for measuring all the distributions.
                    for item in gain_dist:
                        dist_file.write( str(item)+'   ')
                    dist_file.write( str(fit_modal_gain)+ '   ')
                    dist_file.write( str(fit_gain_width)+ '   ')
                    dist_file.write( str(current.KW_EXPSTART) + '   ')
                    dist_file.write( str(current.KW_SEGMENT) + '   ')
                    dist_file.write( str(x) + '   ')
                    dist_file.write( str(y) + '   ')
                    dist_file.write( '\n')
                else:
                    plot_gaussian_fit(gain_dist,fit_parameters,'WARNING_%s_%s_xy_%d_%d_%5.2f.png'%(gain_flag,current.KW_SEGMENT,x,y,current.KW_EXPSTART))
    """

#------------------------------------------------------------

def populate_keywords_mine():
    """Populated header keywords of gainmap fits files according to HVTAB
    Some of the files dave made were populated incorrectly.  This can probably
    be removed, or modified into just a verification, when using OPUS created
    CCI files.
    """
    print '\nPopulating HV keywords.\nREMOVE WHEN THEY ARE POPULATED THEMSELVES'
    hvtab=pyfits.open(HVTAB)
    for cci_file in glob.glob( os.path.join(MONITOR_DIR,'*_cci_gainmap.fits') ):
        fits=pyfits.open(cci_file,mode='update')
        detector=fits[0].header['DETECTOR']
        old_hv = fits[0].header['DETHV']
        if not detector == 'FUV': 
            print ' skipping, NUV'
            fits.close()
            continue

        if '-00_cci' in cci_file:
            segment = 'FUVA'
            hv_string = 'HVLEVELA'

        elif '-01_cci' in cci_file:
            segment = 'FUVB'
            hv_string = 'HVLEVELB'

        expstart = fits[0].header['EXPSTART']
        if not expstart:
            set_hv = 0
        else:
            dethv_index = np.where(hvtab[segment].data['DATE'] < expstart)[0][-1]
            set_hv = hvtab[segment].data[hv_string][dethv_index]

        if set_hv != old_hv:
            fits[0].header.update('DETHV',set_hv)

        print cci_file,old_hv,'-->',set_hv

        fits.flush()
        fits.close()

#------------------------------------------------------------

def gaussian(p,x):
    """Create a gaussian curve for given input parameters and x range.
    """
    assert len(p) == 3, 'P must be a 3 element object specifying the parameters for the gaussian fit'
    return p[0]*np.exp((-(p[1]-x)**2)/(2*p[2]**2.0))

#------------------------------------------------------------

def fit_gaussian(dist, a = None, b = None, c = None):
    '''
    Fits the input distribution to a gaussian using scipy least-squares routing. 

    Returns the parameters for the fit and SUCCESS/FAILURE True/False flag.
    '''
    dist = np.array(dist)
    x = np.array(range(len(dist)))
    gauss_func = lambda p,x: p[0]*np.exp((-(p[1]-x)**2)/(2*p[2]**2.0))
    err_func = lambda p,x,dist: gauss_func(p,x)-dist
    if not a: a = dist.max()
    if not b: b = np.argmax(dist)
    #if not c: c = dist.std()
    if not c: c = 7
 
    p0 = [a, b, c]
    parameters, success = leastsq(err_func, p0, args=(x, dist))
    found_center = parameters[1]
 
    return parameters,success

#------------------------------------------------------------

def get_previous(current_cci):
    """Populates list of CCI objects.

    A list of cci_objects with the same DETHV and within 
    NUM_DAYS_PREVIOUS before current_cci will be created.

    Parameters
    ----------
    current_cci: cci object
        the current cci_object

    Returns
    -------
    output: list
        list of previous cci_objects
    """

    #---Lookback_time
    NUM_DAYS_PREVIOUS = 7.1  ##Just in case something is really close

    out_list=[]

    print 'Retrieving data from previous CCIs:'
    print '-----------------------------------'

    dethv = current_cci.KW_DETHV
    expstart = current_cci.KW_EXPSTART
    segment = current_cci.KW_SEGMENT
    cci_name = current_cci.input_file

    if ( (not NUM_DAYS_PREVIOUS) or (not expstart) ): 
        print 'None to find'
        return out_list

    path,file_name = os.path.split(cci_name)
    if segment == 'FUVA':
        ending = '*' + FUVA_string + '*'
    elif segment == 'FUVB':
        ending = '*' + FUVB_string + '*'
    else:
        print 'Error, segment error in %s'%(file_name)
        print 'Returning blank list'
        return []

    cci_list = glob.glob(CCI_DIR + ending)
    cci_list.sort()
    current_cci_index = cci_list.index(cci_name)

    for cci_file in cci_list[:current_cci_index][::-1]:
        cci_hv = pyfits.getval(cci_file,'DETHV') 
        cci_expstart = pyfits.getval(cci_file,'EXPSTART')

        if (cci_expstart < (expstart - NUM_DAYS_PREVIOUS) ): 
            break

        if (cci_hv == dethv): 
            out_list.append( CCI_object(cci_file) )

        if len(out_list) >= 2* NUM_DAYS_PREVIOUS:
            print 'Breaking off list.  %d files retrieved'%(2 * NUM_DAYS_PREVIOUS)
            break
   
    print 'Found: %d files'%( len(out_list) )
    print [item.input_file_name for item in out_list]
    print '-----------------------------------'

    return out_list


#------------------------------------------------------------

def open_cci(cci_name):
    """Opens cumulative image fits file and returns an open
    cci_object.

    Should only work, and only be used, on COS FUV TTAG 
    CCI's,though never tested for failure.  If output 
    already exists it will return None.

    Parameters
    ----------
    cci_name: string
        full path to cumulative image

    Returns
    -------
    output: cci_object
        open cci_object
    """

    path,cci_file = os.path.split(cci_name)

    if cci_file[-3:] == '.gz': 
        cci_file = cci_file[:-3]

    file_name,ext = os.path.splitext(cci_file)

    if os.path.exists(MONITOR_DIR+file_name+'_gainmap.fits'):
        print '%s Output already exists, skipping'%(file_name)
        return None

    out_object = CCI_object(cci_name)

    return out_object

#------------------------------------------------------------

def explode( filename ):
    """ Expand an events list into a 3D data cube of images
    for each PHA value 0-32
    
    """

    if isinstance( filename, str ):
        events = pyfits.getdata( filename, ext=('events',1) )
    else:
        raise ValueError( '{} needs to be a filename of a COS corrtag file'.format( filename ) )
    
    out_cube = np.empty( (32, 1024, 16384) )

    for phaval in range(0, 32):
        index = np.where( events['PHA'] == phaval )[0]

        if not len(index):
            out_cube[ phaval ] = 0
            continue

        image, y_range, x_range = np.histogram2d( events['YCORR'][index], 
                                                  events['XCORR'][index], 
                                                  bins=(1024, 16384), 
                                                  range=( (0,1023), (0,16384) ) 
                                                  )
        out_cube[ phaval ] = image

    return out_cube

#------------------------------------------------------------

def write_gainmap( current ):
    '''Write current CCI object to fits file.

    Output files are used in later analysis to determine when
    regions fall below the threshold.
    '''

    out_fits = MONITOR_DIR+current.input_file_name+'_gainmap.fits'
    if os.path.exists(out_fits): return

    #-------Ext=0
    hdu_out=pyfits.HDUList(pyfits.PrimaryHDU())

    hdu_out[0].header.update('TELESCOP','HST')   
    hdu_out[0].header.update('INSTRUME','COS')
    hdu_out[0].header.update('DETECTOR','FUV')
    hdu_out[0].header.update('OPT_ELEM','ANY')
    hdu_out[0].header.update('FILETYPE','GAINMAP')

    for attr,value in current.__dict__.iteritems():
        if attr.startswith('KW_'):
            keyword = attr.strip('KW_')
            assert len(keyword) <= 8, 'Header keyword is too large'
            hdu_out[0].header.update(keyword,value)

    #hdu_out[0].header.update('SRC_FILE',current.input_file_name)
    #hdu_out[0].header.update('LIFE_ADJ',current.LIFE_ADJ)
    #hdu_out[0].header.update('SEGMENT',current.SEGMENT)
    #hdu_out[0].header.update('EXPSTART',current.EXPSTART)
    #hdu_out[0].header.update('EXPEND',current.EXPEND)
    #hdu_out[0].header.update('EXPTIME',current.EXPTIME)
    #hdu_out[0].header.update('NUMFILES',current.NUMFILES)
    #hdu_out[0].header.update('COUNTS',current.COUNTS)
    #hdu_out[0].header.update('DETHV',current.DETHV)
    #hdu_out[0].header.update('cnt00_00',current.cnt00_00)
    #hdu_out[0].header.update('cnt01_01',current.cnt01_01)
    #hdu_out[0].header.update('cnt02_30',current.cnt02_30)
    #hdu_out[0].header.update('cnt31_31',current.cnt31_31)

    #-------EXT=1
    included_files = np.array( current.file_list )
    files_col = pyfits.Column('files','24A','rootname',array=included_files)
    tab = pyfits.new_table( [files_col] )

    hdu_out.append( tab )
    hdu_out[1].header.update('EXTNAME','FILES')

    #-------EXT=2
    hdu_out.append(pyfits.ImageHDU(data=current.modal_gain_array))
    hdu_out[2].header.update('EXTNAME','MOD_GAIN')

    #-------EXT=3
    hdu_out.append(pyfits.ImageHDU(data=current.counts))
    hdu_out[3].header.update('EXTNAME','COUNTS')

    #-------EXT=4
    hdu_out.append(pyfits.ImageHDU(data=current.extracted_charge))
    hdu_out[4].header.update('EXTNAME','CHARGE')

    #-------EXT=5
    hdu_out.append(pyfits.ImageHDU(data=current.big_array[0]))
    hdu_out[5].header.update('EXTNAME','cnt00_00')

    #-------EXT=6
    hdu_out.append(pyfits.ImageHDU(data=current.big_array[1]))
    hdu_out[6].header.update('EXTNAME','cnt01_01')

    #-------EXT=7
    hdu_out.append(pyfits.ImageHDU(data=np.sum( current.big_array[2:31],axis=0) ))
    hdu_out[7].header.update('EXTNAME','cnt02_30')
 
    #-------EXT=8
    hdu_out.append(pyfits.ImageHDU(data=current.big_array[31]))
    hdu_out[8].header.update('EXTNAME','cnt31_31')


    #-------Write to file
    hdu_out.writeto(out_fits)
    hdu_out.close()

    print 'WROTE: %s'%(out_fits)

#------------------------------------------------------------

def plot_gaussian_fit(gain_dist,fit_parameters,title):
    """Plots distribution and fit to distribution.  Title
    indicates reasons why distribution was excluded (or not).

    Plots are output to MONITOR_DIR
    """
    fig = plt.figure(figsize=(8,5))
    ax = fig.add_subplot(1,1,1)

    ax.bar( range( len(gain_dist) ),gain_dist,align='center')

    plot_range = np.linspace(0,31,125)
    fit = gaussian(fit_parameters,plot_range)

    ax.plot(plot_range,fit,'r',lw=3)

    ax.set_xlim(0,32)
    ax.set_xlabel('PHA')
    ax.set_ylabel('Count')
    ax.set_title('%3.2f %3.2f %3.2f'%(fit_parameters[0],fit_parameters[1],fit_parameters[2]))

    fig.savefig( os.path.join(MONITOR_DIR,title) )
    plt.close(fig)   
    
#------------------------------------------------------------
