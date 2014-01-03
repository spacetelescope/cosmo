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
import gzip
import pickle
import glob
import sys

import pyfits
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import scipy
from scipy.optimize import leastsq, newton, curve_fit

from ..support import rebin,init_plots,Logger,enlarge,progress_bar,send_email
from constants import *  #Iknow I know
    
#---------------------------------------------------------------------------

def time_trends():
    '''
    Finds when each pixel (superpixel) will go bad based on how its
    modal gain has decreased with time.

    Inputs:
    None

    Outputs:
    proj_bad.fits

    Returns:
    None

    '''
    ###TODO
    ###Make this only run on a list of HV values, so only run on the HV that data has been taken on.

    #SLOPE_MAX = 1e-4
    SLOPE_MAX = 12  ###Testing, does it make a difference

    print '\n#----------------------#'
    print 'Finding trends with time'
    print '#----------------------#'
    
    print 'Cleaning previous products'
    for item in glob.glob( os.path.join(MONITOR_DIR,'flagged_bad_??_cci_*.txt') ):
        os.remove(item)

    for item in glob.glob( os.path.join(MONITOR_DIR,'*.pkl') ):
        os.remove(item)

    for item in glob.glob( os.path.join(MONITOR_DIR,'proj_bad_??_cci*.fits') ):
        os.remove(item)

    for item in glob.glob( os.path.join(MONITOR_DIR,'cumulative_gainmap_*.png') ):
        os.remove(item)
  

    anomoly_out = open( os.path.join( MONITOR_DIR,'found_jumps.txt'), 'w' )
    anomoly_out.write('bad locations\n')


    for ending in [FUVA_string,FUVB_string]:
        possible_dethv = get_possible_hv(ending)

        for dethv in possible_dethv:
            print '\n\nSegment: %s  HV: %d'%(ending,dethv)
            data_dict = compile_gain_maps(ending,dethv)
            if not data_dict: 
                print 'No measured locations found for this HV setting'
                continue
            date_list = data_dict['date_list']

            current_index = np.where( (date_list > 0) )[0]

            date_list = date_list[current_index]
            gain_all = data_dict['gain_array_list'][current_index]
            charge_all = data_dict['charge_array_list'][current_index]
            counts_all = data_dict['counts_array_list'][current_index]

            cumulative_gain = np.zeros((YLEN,XLEN))
            for gain_array in gain_all:
                non_zero = np.where(gain_array > 0)
                cumulative_gain[non_zero] = gain_array[non_zero]

            fit_intercept_array = np.zeros( (YLEN,XLEN) )
            fit_slope_array = np.zeros( (YLEN,XLEN) )

            date_bad_array = np.zeros((YLEN,XLEN))
            bad_locations=[]

            print 'Looking for pixels gone bad\n'

            for y in range(Y_VALID[0],Y_VALID[1]):
                for x in range(X_VALID[0],X_VALID[1]):
                    date_bad = 0  #Initialize incase we can't find better
                    gain_dist = gain_all[:,y,x]
                    charge_dist = charge_all[:,y,x]
                    counts_dist = counts_all[:,y,x]

                    valid_index = np.where(gain_dist > 0)[0]
                    if len(valid_index) < 5: continue

                    dates_to_fit = date_list[valid_index]
                    gain_to_fit = gain_dist[valid_index]
                    counts_to_fit = counts_dist[valid_index]
                    charge_to_fit = charge_dist[valid_index]

                    x_fit = dates_to_fit
                    y_fit = gain_to_fit
                    #y_fit=charge_to_fit

                    sorted_index = x_fit.argsort()
                    x_fit = x_fit[sorted_index]
                    y_fit = y_fit[sorted_index]
 
                    if len(x_fit) > 10:
                        fit,parameters,success = time_fitting(x_fit,y_fit)
                        if success:
                            fit_intercept_array[y,x] = parameters[1]
                            fit_slope_array[y,x] = parameters[0]
                            slope = parameters[0]

                            f = lambda x,a,b: a * x + b - MODAL_GAIN_LIMIT
                            fprime = lambda x,a,b: a

                            try:
                                date_bad = newton(f,x_fit[-1],fprime,args=tuple(parameters),tol=1e-5,maxiter=1000)
                            except RuntimeError:
                                date_bad = 0

                    #--Check modal gain vs. time for rapid changes in gain.
                    anomoly_dates = check_rapid_changes(x_fit,y_fit)
                    for date in anomoly_dates:
                        anomoly_out.write( '%s  %d  %d  %5.7f \n' % (ending, y, x, date) )

                    below_thresh = np.where(y_fit <= MODAL_GAIN_LIMIT)[0]
                    for point_index in below_thresh:
                        n_last_few_points = len(np.where(y_fit[point_index-4:point_index] <= (MODAL_GAIN_LIMIT + .5))[0])
                        if (n_last_few_points >= 3): 
                            first_bad_index = point_index
                            break

                    if ( len(below_thresh) and (n_last_few_points >= 3) ):# and (slope < SLOPE_MAX) ): ###Testing
                        MJD_bad = round( x_fit[first_bad_index],3)
                        bad_locations.append((x,y,MJD_bad))

                        #if MAKE_PLOTS:
                        """
                        plt.title(str(date_bad)+'_vs_'+str(MJD_bad))
                        plt.plot(x_fit,y_fit,'o')
                        plt.axvline(x = MJD_bad,color='r',linestyle='--',lw=3,zorder = 10)
                        plt.axhline(y = MODAL_GAIN_LIMIT, color = 'r', linestyle='-',lw=3,zorder=9)
                        plt.savefig(MONITOR_DIR+'BAD_NOW_'+str(y)+'_'+str(x)+'_'+ending+'_'+str(dethv)+'.png')
                        plt.cla()
                        """
                        
                    elif ( (len(below_thresh)) ):
                        first_bad_index = below_thresh[0]
                        MJD_bad = x_fit[first_bad_index]
                        #if MAKE_PLOTS:
                        """
                        plt.plot(x_fit,y_fit,'o')
                        plt.title(str(date_bad)+'_vs_'+str(MJD_bad)+'_pts_'+str(n_last_few_points))
                        plt.savefig(MONITOR_DIR+'BAD_NOW_NOT_'+str(y)+'_'+str(x)+'_'+ending+'_'+str(dethv)+'_not.png')
                        plt.cla()
                        """

                    if ( (date_bad > date_list[0]) and (date_bad < date_list[-1] + 30) ):
                        #print '##########################'
                        #print 'WARNING Region project to go bad within 30 days'
                        #print '##########################'
                        
                        #--Cool, but takes FOREVER becuase it makes a lot of plots.  Maybe shrink the range, and exclude already bad ones.--#
                        """
                        if MAKE_PLOTS:
                        plt.figure(figsize = (12,8))
                        plt.title(str(y)+' '+str(x))
                        plt.plot(x_fit,y_fit,marker='o',color='b',linestyle='',label='Modal Gain History')
                        fit = scipy.polyval(parameters,x_fit)
                        plt.plot(x_fit,fit,marker='x',color='g')
                        plt.plot(date_bad,MODAL_GAIN_LIMIT,marker='x',color='r',label='Projected Bad',markersize=15)
                        plt.xlabel('MJD')
                        plt.ylabel('Modal Gain')
                        plt.axhline(y=MODAL_GAIN_LIMIT,color='r',lw=2,linestyle='--',label='PHA = %d'%(MODAL_GAIN_LIMIT))
                        plt.axvline(x=date_bad,color='r',lw=2,linestyle='-.',label='Date Bad = %5.2f'%(date_bad))
                        plt.xlim(date_list[0],date_list[-1])
                        if ( (fit.mean()<31) & (fit.mean()>1) ):
                            plt.ylim(0,20)
                        else:
                            pass
                        plt.legend(shadow=True,numpoints = 1)
                        plt.savefig(MONITOR_DIR+'BAD_SOON_'+str(y)+'_'+str(x)+'_'+ending+'_'+str(dethv)+'.png')
                        plt.cla()
                        """

                    date_bad_array[y,x] = date_bad

            #data_dict['cumulative_gain'] = cumulative_gain
            #data_dict['date_bad_array'] = date_bad_array
            data_dict['slopes'] = fit_slope_array
            #data_dict['all_intercepts'] = fit_intercept_array

            write_bad_pixels(bad_locations,ending,dethv)
            write_projection(date_bad_array,cumulative_gain,data_dict,ending,dethv)
            #save_arrays(data_dict, ending, dethv)
        print '\nfinished time trending'
    anomoly_out.close()
 
#------------------------------------------------------------

def time_fitting(x_fit,y_fit):
    """Fit a linear relation to the x_fit and y_fit parameters

    Returns the actual fit and the parameters of the fit,
    """
    import numpy as np
    x_fit = np.array( x_fit )
    y_fit = np.array( y_fit )
    
    ###First fit iteration and remove outliers
    POLY_FIT_ORDER = 1

    slope,intercept = scipy.polyfit(x_fit,y_fit,POLY_FIT_ORDER)
    fit = scipy.polyval((slope,intercept),x_fit)
    fit_sigma = fit.std()
    include_index = np.where(np.abs(fit-y_fit) < 1.5*fit_sigma)[0]

    if len(include_index) < 4:
        return None,None,False

    ###Final Fit
    x_fit_clipped = x_fit[include_index]
    y_fit_clipped = y_fit[include_index]
    
    parameters = scipy.polyfit(x_fit_clipped,y_fit_clipped,POLY_FIT_ORDER)
    fit = scipy.polyval(parameters,x_fit)

    return fit,parameters,True

#------------------------------------------------------------

def compile_gain_maps(ending,hv_value):
    """Compiles a large dictionary of arrays from gainmaps

    Parameters
    ----------
    ending: string
        string designating FUVA or FUVB in the files

    Returns
    -------
    out_dict: dictionary
        dictionary of arrays

    """
    out_dict = {}

    file_list = glob.glob(MONITOR_DIR+'*'+ending+'*gainmap.fits')
    if len(file_list) < 1: return False
    file_list.sort()

    gain_list = []
    counts_list = []
    charge_list = []
    cumulative_counts_list = []
    cumulative_charge_list = []
    date_list = []

    print 'Compiling %d files.'%(len(file_list))
    for i,gainmap in enumerate(file_list):
        progress_bar( i,len(file_list) )

        current_hv = pyfits.getval( gainmap, 'DETHV', ext=0 )
        if current_hv != hv_value:
            continue

        hdu = pyfits.open(gainmap,memmap=False) 

        charge_data = hdu['CHARGE'].data

        date_list.append( hdu[0].header['EXPSTART'] )
        gain_list.append( hdu['MOD_GAIN'].data )
        counts_list.append( hdu['COUNTS'].data )
        charge_list.append( hdu['CHARGE'].data )
        cumulative_counts_list.append( hdu['CUMLCNTS'].data )
        cumulative_charge_list.append( hdu['CUMLCHRG'].data )

        hdu.close()

    if not len(date_list):
        print
        return False

    out_dict['gain_array_list'] = np.array(gain_list)
    out_dict['counts_array_list'] = np.array(cumulative_counts_list)
    out_dict['charge_array_list'] = np.array(cumulative_charge_list)
    out_dict['date_list'] = np.array(date_list)

    out_dict['cumulative_counts'] = np.array(cumulative_counts_list[-1])
    out_dict['cumulative_charge'] = np.array(cumulative_charge_list[-1])
    print
    return out_dict

#------------------------------------------------------------

def get_possible_hv(ending):
    """Retrieves all possible HV values for a given segment
    as given in HVTAB

    Returns:
        all_hv: list of hv values
    """
    hvtab = pyfits.open(HVTAB)
    if ending == FUVA_string:
        segment = 'FUVA'
        hv_column = 'HVLEVELA'
    elif ending == FUVB_string:
        segment = 'FUVB'
        hv_column = 'HVLEVELB'
    
    all_hv = list(set(hvtab[segment].data[hv_column]))
    all_hv.sort()
        
    return all_hv

#------------------------------------------------------------

def check_rapid_changes(x_values,y_values):
    """Check for rapid changes in the values.

    An email will be sent if any rapid dip or jump is seen.
    """
    gain_thresh = 5  
    mjd_thresh = 28
    #Set initial values at ridiculous numbers
    previous_gain = y_values[0]
    previous_mjd = x_values[0]

    jumps = []
    for gain,mjd in zip(y_values,x_values):
        gain_diff = np.abs( previous_gain - gain )
        mjd_diff = np.abs( previous_mjd - mjd )
        if (gain_diff > gain_thresh) and (mjd_diff < mjd_thresh):
            jumps.append(mjd)

        previous_gain = gain
        previous_mjd = mjd

    return jumps

#------------------------------------------------------------

def write_bad_pixels(locations,ending,dethv):
   """ Writes out locations flagged as bad to txt files.

   Bad locations will be written out in superpixel sized
   blocks, but in unbinned detector coordinates, 
   in the same format used by the gain sag table,

   Output table format:
   X   DX   Y   DY  MJD
   
   Parameters
   ----------
   locations: cci object
       the current cci_object

   ending: string
       string designating FUVA or FUVB in the files

   dethv: int
       detector hv setting

   Returns
   -------
   None

   Products
   --------
   flagged_bad*.txt
   """
   bpix = open( os.path.join(MONITOR_DIR,'flagged_bad_%s_%.3d.txt'%(ending,dethv) ),'w')
   bpix.write('%5.5s  %3.3s  %4.4s  %3.3s  %5.3s\n'%('x','dx','y','dy','mjd'))
   bpix.write('#------------------------------#\n')
   for location in locations:
       x = location[0] * X_BINNING
       y = location[1] * Y_BINNING
       dx = X_BINNING
       dy = Y_BINNING
       mjd = location[2]
       bpix.write('%5.1i  %3.1i  %4.1i  %3.1i  %5.3f\n'%(x,dx,y,dy,mjd))
   
   bpix.close()
   print 'Bad pixel locations found: %d'%(len(locations))

#------------------------------------------------------------

def write_projection(date_bad_array,cumulative_gain,data_dict,ending,dethv):
    """Writs a fits file with information useful for post-monitoring analysis
    and checking.
    """
    print 'Writing projection file'
    hdu_out=pyfits.HDUList(pyfits.PrimaryHDU())
    hdu_out[0].header.update('TELESCOP','HST')
    hdu_out[0].header.update('INSTRUME','COS')
    hdu_out[0].header.update('DETECTOR','FUV')
    hdu_out[0].header.update('OPT_ELEM','ANY')
    hdu_out[0].header.update('FILETYPE','PROJ_BAD')
    hdu_out[0].header.update('DETHV',dethv)
    if ending == FUVA_string:
        segment = 'FUVA'
    elif ending == FUVB_string:
        segment = 'FUVB'
    else:
        segment == '???'
    hdu_out[0].header.update('SEGMENT',segment)

    #---Ext 1
    hdu_out.append(pyfits.ImageHDU(data = date_bad_array))
    hdu_out[1].header.update('EXTNAME','PROJBAD')

    #---Ext 2
    hdu_out.append(pyfits.ImageHDU(data = cumulative_gain))
    hdu_out[2].header.update('EXTNAME','PROJGAIN')

    #---Ext 3
    hdu_out.append(pyfits.ImageHDU(data = data_dict['cumulative_counts']))
    hdu_out[3].header.update('EXTNAME','CUMLCNTS')

    #---Ext 4
    hdu_out.append(pyfits.ImageHDU(data = data_dict['cumulative_charge']))
    hdu_out[4].header.update('EXTNAME','CUMLCHRG')

    #---Ext 4
    hdu_out.append(pyfits.ImageHDU(data = data_dict['slopes']))
    hdu_out[5].header.update('EXTNAME','SLOPE')

    #---Writeout
    hdu_out.writeto(MONITOR_DIR+'proj_bad_'+ending+'_'+str(dethv)+'.fits',clobber=True)
    hdu_out.close()

#------------------------------------------------------------

def save_arrays(data_dict, ending, hv_value):
    """Pickles array dictionary for later interactive analysis

    One array is created for each HV setting and detector combo.  
    """

    print 'Pickling array dictionary'

    out_pickle = os.path.join(MONITOR_DIR,'pickled_arrays_%s_%d.pkl'%(ending,hv_value))

    out_open = file(out_pickle,'w')
    pickle.dump(data_dict,out_open)
    out_open.close()

    print 'WROTE: arrays to %s'%(out_pickle)

#------------------------------------------------------------
