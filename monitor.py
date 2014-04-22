#!/usr/bin/python

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

import argparse
import os
import sys
from astropy.io import fits as pyfits
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import glob
import numpy as np

from ..support import init_plots, Logger, enlarge, send_email
import gainmap
import findbad
import gsag
import phaimage
from constants import *

#------------------------------------------------------------

def make_quicklooks(clobber=False):
    """Output some nice plots for quick-look analysis and
    for the webpage in the future
    """
    print '#-----------------#'
    print 'Making pretty plots'
    print '#-----------------#'
    plt.ioff()

    all_gainmaps = glob.glob( os.path.join(MONITOR_DIR, '*gainmap*.fits') )
    all_gainmaps.sort()
    for gainmap in all_gainmaps:
        out_image_file = gainmap.replace('gainmap.fits', 'quicklook.png')
        if os.path.exists( out_image_file ): 
            if clobber: 
                pass
            else:
                continue

        fits = pyfits.open(gainmap)

        image = enlarge( fits['MOD_GAIN'].data, y=Y_BINNING, x=X_BINNING )

        DETHV = fits[0].header['DETHV']
        EXPSTART = fits[0].header['EXPSTART']
        SEGMENT = fits[0].header['SEGMENT']

        if SEGMENT == 'FUVA':
            lower_ext = 1
            upper_ext = 2
            head = FUVA_string
        elif SEGMENT == 'FUVB':
            lower_ext = 3
            upper_ext = 4
            head = FUVB_string
        pha_name = gainmap.replace('_gainmap.fits', '_phf.fits').replace(head, 'phaimage' )
        print pha_name 
        phaimage = pyfits.open(pha_name)

        has_gain = np.zeros( image.shape )
        index = np.where( image > 0 )
        has_gain[index] += 1
        collapsed = np.sum( has_gain, axis=1 )
        if collapsed.sum() == 0:
            peak = 400
        else:
            peak = 100 + collapsed[100:600].argmax()

        row_gain = image[peak]
        row_pha_lower = phaimage[lower_ext].data[peak]
        row_pha_upper = phaimage[upper_ext].data[peak]

        #------Plotting-----------#
        fig = plt.figure(figsize=(22, 10))
        rectangle = np.array([.1, .1, .8, .8])
        ax = fig.add_axes( rectangle )

        cax = ax.imshow(image, aspect='auto', cmap=mpl.cm.get_cmap('hot_r')  )
        
        plot_flagged(ax, SEGMENT, DETHV, mjd=EXPSTART, color='blue')
        ax.set_xlim(0, 16384)
        ax.set_ylim(0, 1024)

        ax.set_title('MJD: %5.5f'% (EXPSTART))
        cax.set_clim(0, 20)
        ax.grid(False)
        ax.xaxis.set_major_locator( mpl.ticker.MaxNLocator(10) )
        ax.set_xlabel('XCORR Pixel')
        ax.set_ylabel('YCORR Pixel')

        plt.text(100, 1000, s='DETHV: %d'% (DETHV), bbox = dict(boxstyle="round",  fc="0.9")  )

        new_rect = rectangle.copy()
        new_rect[1] = 7 * new_rect[3] / 8.
        new_rect[3] /= 4.
        new_rect = [.82, .18, .1, .8]
        cax_holder = fig.add_axes(new_rect, frameon=False, visible=False)
        cax_holder.set_xticklabels( ['' for item in cax_holder.get_xticklabels() ] )
        cax_holder.set_yticklabels( ['' for item in cax_holder.get_yticklabels() ] )
        fig.colorbar(cax, ax=cax_holder, ticks=range(0, 21), shrink=.7)

        new_rect = rectangle.copy()
        new_rect[3] /= 4.0
        ax2 = fig.add_axes( new_rect , frameon=False)
        ax2.plot(row_gain, color='b', lw=3)
        ax2.plot(row_pha_lower, color='y', lw=2, label='PHF Limits')
        ax2.plot(row_pha_upper, color='y', lw=2)
        ax2.axhline(y=2, color='r', label='PHA 2')
        ax2.axhline(y=3, color='r', ls='--',  label='PHA 3')
        ax2.set_title('Gain and Limits at Y = %d'% peak)
        ax2.set_xticklabels( ['' for item in ax2.get_xticklabels() ] )
        ax2.yaxis.tick_right()
        ax2.yaxis.set_label_position('right')
        ax2.set_ylabel('PHA Gain')
        ax2.set_xlim(0, 16384)
        ax2.set_ylim(0, 24)
        ax2.legend(numpoints=1, shadow=True)

        fig.savefig(out_image_file)
        plt.close(fig)
        print 'WROTE: %s'% (out_image_file)

#------------------------------------------------------------

def make_cumulative_plots():
    """
    Make plots showing cumulative gain for each HV setting.
    
    """

    print 'Making cumulative gainmaps'
    for filename in glob.glob( os.path.join( MONITOR_DIR, '*proj_bad*.fits') ):
        fits = pyfits.open( filename )

        dethv = fits[0].header['DETHV']
        segment = fits[0].header['SEGMENT']
 
        fig = plt.figure(figsize = (25, 14))
        ax = fig.add_subplot(1, 1, 1)
        gain_image = enlarge( fits['PROJGAIN'].data,
                              y=1024/fits['PROJGAIN'].header['NAXIS2'],
                              x=16384/fits['PROJGAIN'].header['NAXIS1'] )
        cax = ax.imshow( gain_image, aspect='auto')
        #plot_flagged(ax, segment, dethv, color='white')
        ax.set_xlim(0, 16384)
        ax.set_ylim(0, 1024)
        ax.grid(False)
        cax.set_clim(0, 16)
        fig.colorbar(cax)
        print segment, dethv
        fig.savefig( os.path.join( MONITOR_DIR, 'cumulative_gainmap_'+segment+'_'+str(dethv)+'.png') )
        plt.close(fig)

        fits.close()

#------------------------------------------------------------

def plot_flagged(ax, segment, hv, mjd=60000, color='r'):
    """
    Plot a box at each flagged location

    """
    
    gsagtab_filename = '/grp/hst/cos/Monitors/CCI/gsag_%s.fits'% (TIMESTAMP)
    if os.path.exists( gsagtab_filename ):
        gsagtab = pyfits.open('/grp/hst/cos/Monitors/CCI/gsag_%s.fits'% (TIMESTAMP))
    else:
        all_gsagtables = glob.glob( os.path.join(MONITOR_DIR, 'gsag*.fits') )
        all_gsagtables.sort()
        gsagtab = pyfits.open( all_gsagtables[-1] )
    
    if segment == 'FUVA':
        hv_keyword = 'HVLEVELA'
    elif segment == 'FUVB':
        hv_keyword = 'HVLEVELB'

    regions = []
    for ext in gsagtab[1:]:
        if ext.header['SEGMENT'] == segment:
            if ext.header[hv_keyword] == hv:
                regions = ext.data
                break

    for line in regions:
        if line['Date'] > mjd: continue
        lx = line['lx']
        dx = line['dx']
        ly = line['ly']
        dy = line['dy']
    
        x_values = [lx, lx+dx, lx+dx, lx, lx]
        y_values = [ly, ly, ly+dy, ly+dy, ly]
        ax.plot(x_values, y_values, color)

#------------------------------------------------------------

def check_new_files():
    """Compares the number of made gainmaps to the number of 
    available CCI files and returns the number.
    """

    N_CCI = len( glob.glob( os.path.join( CCI_DIR, '*'+FUVA_string+'*' ) ) 
                 + glob.glob( os.path.join( CCI_DIR, '*'+FUVB_string+'*' ) ) )

    N_GAINMAPS = len( glob.glob( os.path.join( MONITOR_DIR,  '*'+FUVA_string+'*gainmap.fits' ) ) 
                      + glob.glob( os.path.join( MONITOR_DIR,  '*'+FUVB_string+'*gainmap.fits' ) ) )

    N_new = N_CCI - N_GAINMAPS

    return N_new

#------------------------------------------------------------

def main( args ):
    """ Main driver for monitoring program.
    """

    os.system('clear')
    init_plots()
    plt.ioff()
    
    if args.check_for_new:
        N_NEW_CCI = check_new_files()
        if not N_NEW_CCI:  sys.exit('No new CCI files to include')
        else:  print 'Found %d New CCI files to include'% (N_NEW_CCI)

    logname = os.path.join( MONITOR_DIR, 'run_%s.txt'% (TIMESTAMP) )
    sys.stdout = Logger( logname ) 

    print '#------------------#'
    print TIMESTAMP
    print __file__
    for attr, value in args.__dict__.iteritems():
        print '%s:  %s'% (attr, value)
    print '#------------------#'
    print
    
    
    gainmap.make_all_gainmaps( args.n_processors )
   
    phaimage.make_phaimages(True)

    findbad.time_trends()
     
    gsag.main( args.regress )

    make_quicklooks()

    make_cumulative_plots()

    message = 'CCI Monitor run for %s complete.  \n'% (TIMESTAMP)
    message += '\n'
    message += 'Calibration with CalCOS has finished \n '
    message += 'Check over the gsagtab comparison log and see if we need to deliver this file.\n\n\n'
    message += 'Sincerely,\n %s'% (__file__)
    send_email( subject='CCI Monitor complete', message=message )

#------------------------------------------------------------

if __name__ == "__main__":
    main( parse_args() )
