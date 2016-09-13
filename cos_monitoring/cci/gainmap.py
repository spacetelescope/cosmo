from __future__ import absolute_import

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
import shutil
import sys
import time
from datetime import datetime
import gzip
import glob

from astropy.io import fits as pyfits
from astropy.modeling import models, fitting
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import scipy
from scipy.optimize import leastsq, newton, curve_fit
import fitsio

from ..utils import rebin, enlarge
from .constants import *  ## I know this is bad, but shut up.
#from db_interface import session, engine, Gain

if sys.version_info.major == 2:
    from itertools import izip as zip

#------------------------------------------------------------

class CCI:
    """Creates a cci_object designed for use in the monitor.

    Each COS cumulative image fits file is made into its
    own cci_object where the data is more easily used.
    Takes a while to make, contains a few large arrays and
    various header keywords from the original file.
    """

    def __init__(self, filename, **kwargs):
        """Open filename and create CCI Object"""

        self.input_file = filename

        self.xbinning = kwargs.get('xbinning', 1)
        self.ybinning = kwargs.get('ybinning', 1)
        self.mincounts = kwargs.get('mincounts', 30)

        path, cci_name = os.path.split(filename)
        cci_name, ext = os.path.splitext(cci_name)
        #-- trim off any remaining extensions: .fits, .gz, .tar, etc
        while not ext == '':
            cci_name, ext = os.path.splitext(cci_name)

        self.cci_name = cci_name
        self.open_fits()

        print('Measuring Modal Gain Map')

        if not self.numfiles:
            print('CCI contains no data.  Skipping modal gain measurements')
            return

        gainmap, counts, std = measure_gainimage(self.big_array)
        self.gain_image = gainmap
        self.std_image = std

        if kwargs.get('only_active_area', True):
            brftab = os.path.join(os.environ['lref'], self.brftab)
            left, right, top, bottom = read_brftab(brftab, self.segment)

            left //= self.xbinning
            right //= self.xbinning

            top //= self.ybinning
            bottom //= self.ybinning

            self.gain_image[:bottom] = 0
            self.gain_image[top:] = 0
            self.gain_image[:, :left] = 0
            self.gain_image[:, right:] = 0

        if kwargs.get('ignore_spots', True):
            ### Dynamic when delivered to CRDS
            spottab = os.path.join(MONITOR_DIR, '2015-10-20_spot.fits')
            if os.path.exists(spottab):
                regions = read_spottab(spottab,
                                       self.segment,
                                       self.expstart,
                                       self.expend)

                for lx, ly, dx, dy in regions:
                    lx //= self.xbinning
                    dx //= self.xbinning

                    ly //= self.ybinning
                    dy //= self.ybinning

                    #-- pad the regions by 1 bin in either direction
                    lx -= 1
                    dx += 2
                    ly -= 1
                    dy += 2
                    #--

                    self.gain_image[ly:ly+dy, lx:lx+dx] = 0

        self.gain_index = np.where(self.gain_image > 0)
        self.bad_index = np.where((self.gain_image <= 3) &
                                  (self.gain_image > 0))

    def open_fits(self):
        """Open CCI file and populated attributes with
        header keywords and data arrays.
        """
        print('\nOpening %s'%(self.cci_name))

        hdu = fitsio.FITS(self.input_file)
        primary = hdu[0].read_header()

        assert (hdu[2].read().shape == (Y_UNBINNED, X_UNBINNED)), 'ERROR: Input CCI not standard dimensions'

        self.detector = primary['DETECTOR']
        self.segment = primary['SEGMENT']
        self.obsmode = primary['OBSMODE']
        self.expstart = primary['EXPSTART']
        self.expend = primary['EXPEND']
        self.exptime = primary['EXPTIME']
        self.numfiles = primary['NUMFILES']
        self.counts = primary['COUNTS']
        self.dethv = primary.get('DETHV', -999)
        try:
            self.brftab = primary['brftab'].split('$')[1]
        except:
            self.brftab = 'x1u1459il_brf.fits'

        if self.expstart:
            #----Finds to most recently created HVTAB
            hvtable_list = glob.glob(os.path.join(os.environ['lref'], '*hv.fits'))
            HVTAB = hvtable_list[np.array([pyfits.getval(item, 'DATE') for item in hvtable_list]).argmax()]

            hvtab = pyfits.open(HVTAB)

            if self.segment == 'FUVA':
                hv_string = 'HVLEVELA'
            elif self.segment == 'FUVB':
                hv_string = 'HVLEVELB'

        self.file_list = [line[0].decode("utf-8") for line in hdu[1].read()]

        self.big_array = np.array([rebin(hdu[i+2].read(), bins=(self.ybinning, self.xbinning)) for i in range(32)])
        self.get_counts(self.big_array)
        self.extracted_charge = self.pha_to_coulombs(self.big_array)

        self.gain_image = np.zeros((YLEN, XLEN))
        self.modal_gain_width = np.zeros((YLEN, XLEN))

        self.cnt00_00 = len(self.big_array[0].nonzero()[0])
        self.cnt01_01 = len(self.big_array[1].nonzero()[0])
        self.cnt02_30 = len(self.big_array[2:31].nonzero()[0])
        self.cnt31_31 = len(self.big_array[31:].nonzero()[0])

    def get_counts(self, in_array):
        """collapse pha arrays to get total counts accross all
        PHA bins.

        Will also search for and add in accum data if any exists.
        """

        print('Making array of cumulative counts')
        out_array = np.sum(in_array, axis=0)

        ###Test before implementation
        ###Should only effect counts and charge extensions.
        ### no implications for modal gain arrays or measurements
        if self.segment == 'FUVA':
            accum_name = self.cci_name.replace('00_','02_')  ##change when using OPUS data
        elif self.segment == 'FUVB':
            accum_name = self.cci_name.replace('01_','03_')  ##change when using OPUS data
        else:
            accum_name = None
            print('ERROR: name not standard')

        if os.path.exists(accum_name):
            print('Adding in Accum data')
            accum_data = rebin(pyfits.getdata(CCI_DIR+accum_name, 0),bins=(Y_BINNING,self.xbinning))
            out_array += accum_data
            self.accum_data = accum_data
        else:
            self.accum_data = None

        self.counts_image = out_array

    def pha_to_coulombs(self, in_array):
        """Convert pha to picocoloumbs to calculate extracted charge.

        Equation comes from D. Sahnow.
        """

        print('Making array of extracted charge')

        coulomb_value = 1.0e-12*10**((np.array(range(0,32))-11.75)/20.5)
        zlen, ylen, xlen = in_array.shape
        out_array = np.zeros((ylen, xlen))

        for pha,layer in enumerate(in_array):
            out_array += (coulomb_value[pha]*layer)

        return out_array

    def write(self):
        '''Write current CCI object to fits file.

        Output files are used in later analysis to determine when
        regions fall below the threshold.
        '''

        out_fits = MONITOR_DIR + self.cci_name+'_gainmap.fits'
        if os.path.exists(out_fits):
            print("not clobbering existing file")
            return

        #-------Ext=0
        hdu_out = pyfits.HDUList(pyfits.PrimaryHDU())

        hdu_out[0].header['TELESCOP'] = 'HST'
        hdu_out[0].header.update('INSTRUME','COS')
        hdu_out[0].header.update('DETECTOR','FUV')
        hdu_out[0].header.update('OPT_ELEM','ANY')
        hdu_out[0].header.update('FILETYPE','GAINMAP')

        hdu_out[0].header['XBINNING'] = self.xbinning
        hdu_out[0].header['YBINNING'] = self.ybinning
        hdu_out[0].header.update('SRC_FILE', self.cci_name)
        hdu_out[0].header.update('SEGMENT', self.segment)
        hdu_out[0].header.update('EXPSTART', self.expstart)
        hdu_out[0].header.update('EXPEND', self.expend)
        hdu_out[0].header.update('EXPTIME', self.exptime)
        hdu_out[0].header.update('NUMFILES', self.numfiles)
        hdu_out[0].header.update('COUNTS', self.counts)
        hdu_out[0].header.update('DETHV', self.dethv)
        hdu_out[0].header.update('cnt00_00', self.cnt00_00)
        hdu_out[0].header.update('cnt01_01', self.cnt01_01)
        hdu_out[0].header.update('cnt02_30', self.cnt02_30)
        hdu_out[0].header.update('cnt31_31', self.cnt31_31)

        #-------EXT=1
        included_files = np.array(self.file_list)
        files_col = pyfits.Column('files', '24A', 'rootname', array=included_files)
        tab = pyfits.new_table([files_col])

        hdu_out.append(tab)
        hdu_out[1].header.update('EXTNAME', 'FILES')

        #-------EXT=2
        hdu_out.append(pyfits.ImageHDU(data=self.gain_image))
        hdu_out[2].header.update('EXTNAME', 'MOD_GAIN')

        #-------EXT=3
        hdu_out.append(pyfits.ImageHDU(data=self.counts_image))
        hdu_out[3].header.update('EXTNAME', 'COUNTS')

        #-------EXT=4
        hdu_out.append(pyfits.ImageHDU(data=self.extracted_charge))
        hdu_out[4].header.update('EXTNAME', 'CHARGE')

        #-------EXT=5
        hdu_out.append(pyfits.ImageHDU(data=self.big_array[0]))
        hdu_out[5].header.update('EXTNAME', 'cnt00_00')

        #-------EXT=6
        hdu_out.append(pyfits.ImageHDU(data=self.big_array[1]))
        hdu_out[6].header.update('EXTNAME', 'cnt01_01')

        #-------EXT=7
        hdu_out.append(pyfits.ImageHDU(data=np.sum(self.big_array[2:31],axis=0)))
        hdu_out[7].header.update('EXTNAME', 'cnt02_30')

        #-------EXT=8
        hdu_out.append(pyfits.ImageHDU(data=self.big_array[31]))
        hdu_out[8].header.update('EXTNAME', 'cnt31_31')


        #-------Write to file
        hdu_out.writeto(out_fits)
        hdu_out.close()

        print('WROTE: %s'%(out_fits))

#------------------------------------------------------------

def rename(input_file, mode='move'):
    """Rename CCI file from old to new naming convention

    Parameters
    ----------
    input_file : str
        Old-style CCI file
    mode : str, optional
        if 'move', the original file will be removed.  Otherwise, simply the new
        name will be printed and returned.

    Returns
    -------
    outname : str
        Name in the new naming convention.
    """

    options = ['copy', 'move', 'print']
    if not mode in options:
        raise ValueError("mode: {} must be in {}".format(mode, options))

    with pyfits.open(input_file) as hdu:
        path, name = os.path.split(input_file)
        name_split = name.split('_')

        dethv = int(hdu[0].header['DETHV'])

        time_str = name_split[1]
        filetype = name_split[0][3:]

        ext = ''
        if '.fits' in name:
            ext += '.fits'
        if '.gz' in name:
            ext += '.gz'


        if hdu[0].header['DETECTOR'] == 'FUV':
            if dethv == -1:
                dethv = 999
            out_name = 'l_{}_{}_{}_cci{}'.format(time_str, filetype, int(dethv), ext)

        elif hdu[0].header['DETECTOR'] == 'NUV':
            out_name = 'l_{}_{}_cci{}'.format(time_str, filetype, ext)

        out_file = os.path.join(path, out_name)

        if mode == 'copy':
            hdu.writeto(out_name)

    if mode == 'print':
        print(out_name)
    elif mode == 'move':
        print("{} --> {}".format(input_file, out_name))
        shutil.move(input_file, out_name)

    return out_name

#-------------------------------------------------------------------------------

def read_brftab(filename, segment):
    """Parse Baseline Reference Table for needed information

    Reads the active area for the specified segment from the COS BRFTAB
    (Baseline Reference Table).  The four corners (left, right, top, bottom)
    of the active area are returned.

    Parameters
    ----------
    filename : str
        Input BRFTAB
    segment : str
        'FUVA' or 'FUVB', which segment to parse from

    Returns
    -------
    corners : tuple
        left, right, top, bottom corners of the active area
    """

    with pyfits.open(filename) as hdu:
        index = np.where(hdu[1].data['segment'] == segment)[0]

        left = hdu[1].data[index]['A_LEFT']
        right = hdu[1].data[index]['A_RIGHT']
        top = hdu[1].data[index]['A_HIGH']
        bottom = hdu[1].data[index]['A_LOW']

    return left[0], right[0], top[0], bottom[0]

#-------------------------------------------------------------------------------

def read_spottab(filename, segment, expstart, expend):
    """Parse the COS spottab

    Parameters
    ----------
    filename : str
        Input SPOTTAB fits file
    segment : str
        'FUVA' or 'FUVB', which segment to parse from
    expstart : float, int
        return only rows with STOP > expstart
    expend : float, int
        return only rows with START < expend

    Returns
    -------


    """
    with pyfits.open(filename) as hdu:
        index = np.where((hdu[1].data['SEGMENT'] == segment) &
                         (hdu[1].data['START'] < expend) &
                         (hdu[1].data['STOP'] > expstart))[0]

        rows = hdu[1].data[index]

        return zip(rows['LX'], rows['LY'], rows['DX'], rows['DY'])

#-------------------------------------------------------------------------------

def make_all_hv_maps():
    for hv in range(150, 179):
        tmp_hdu = pyfits.open( os.path.join( MONITOR_DIR, 'total_gain.fits') )
        for ext in (1, 2):
            tmp_hdu[ext].data -= .393 * (float(178) - hv)
        tmp_hdu.writeto( os.path.join( MONITOR_DIR, 'total_gain_%d.fits' % hv ), clobber=True )
        print('WRITING  total_gain_{}.fits TO {}'.format(hv, MONITOR_DIR))

#-------------------------------------------------------------------------------

def make_total_gain(gainmap_dir=None, segment='FUV', start_mjd=55055, end_mjd=70000, min_hv=163, max_hv=175, reverse=False):
    if segment == 'FUVA':
        search_string = 'l_*_01_???_cci_gainmap.fits'
    elif segment == 'FUVB':
        search_string = 'l_*_01_???_cci_gainmap.fits'

    all_datasets = [item for item in glob.glob(os.path.join(gainmap_dir, search_string))]
    all_datasets.sort()

    print("Combining {} datasets".format(len(all_datasets)))

    if reverse:
        all_datasets = all_datasets[::-1]

    out_data = np.zeros( (YLEN, XLEN) )

    for item in all_datasets:
        cci_hdu = pyfits.open(item)
        if not cci_hdu[0].header['EXPSTART'] > start_mjd: continue
        if not cci_hdu[0].header['EXPSTART'] < end_mjd: continue
        if not cci_hdu[0].header['DETHV'] > min_hv: continue
        if not cci_hdu[0].header['DETHV'] < max_hv: continue
        cci_data = cci_hdu['MOD_GAIN'].data

        dethv = cci_hdu[0].header['DETHV']

        index = np.where(cci_data)
        cci_data[index] += .393 * (float(178) - dethv)

        index_both = np.where((cci_data > 0) & (out_data > 0))
        mean_data = np.mean([cci_data, out_data], axis=0)

        out_data[index] = cci_data[index]
        out_data[index_both] = mean_data[index_both]

    return enlarge(out_data, x=X_BINNING, y=Y_BINNING)

#------------------------------------------------------------

def make_all_gainmaps_entry():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f",
                        '--filename',
                        type=str,
                        default='total_gain.fits',
                        help="Filename to write gain file to")

    parser.add_argument("-d",
                        '--dir',
                        type=str,
                        default='/grp/hst/cos/Monitors/CCI/',
                        help="directory containing the gainmaps")

    parser.add_argument("-s",
                        '--start',
                        type=float,
                        default=55055.0,
                        help="MJD of the first gainmap to include")

    parser.add_argument("-e",
                        '--end',
                        type=float,
                        default=70000,
                        help="MJD of the last gainmap to include")

    parser.add_argument('--hvmin',
                        type=int,
                        default=163,
                        help="Minimum DETHVS of gainmaps to include")


    parser.add_argument('--hvmax',
                        type=int,
                        default=175,
                        help="Maximum DETHV of gainmaps to include.")


    args = parser.parse_args()

    print("Creating all gainmaps using:")
    print(args)
    make_all_gainmaps(filename=args.filename,
                      gainmap_dir=args.dir,
                      start_mjd=args.start,
                      end_mjd=args.end,
                      min_hv=args.hvmin,
                      max_hv=args.hvmax)

#------------------------------------------------------------

def make_all_gainmaps(filename, gainmap_dir, start_mjd=55055, end_mjd=70000, min_hv=163, max_hv=175):
    """

    """

    #add_cumulative_data(ending)

    hdu_out = pyfits.HDUList(pyfits.PrimaryHDU())
    hdu_out.append(pyfits.ImageHDU(data=make_total_gain(gainmap_dir, 'FUVA', start_mjd, end_mjd, min_hv, max_hv, reverse=True)))
    hdu_out[1].header['EXTNAME'] = 'FUVAINIT'
    hdu_out.append(pyfits.ImageHDU(data=make_total_gain(gainmap_dir, 'FUVB', start_mjd, end_mjd, min_hv, max_hv,  reverse=True)))
    hdu_out[2].header['EXTNAME'] = 'FUVBINIT'
    hdu_out.append(pyfits.ImageHDU(data=make_total_gain(gainmap_dir, 'FUVA', start_mjd, end_mjd, min_hv, max_hv)))
    hdu_out[3].header['EXTNAME'] = 'FUVALAST'
    hdu_out.append(pyfits.ImageHDU(data=make_total_gain(gainmap_dir, 'FUVB', start_mjd, end_mjd, min_hv, max_hv)))
    hdu_out[4].header['EXTNAME'] = 'FUVBLAST'
    hdu_out.writeto(filename, clobber=True)
    hdu_out.close()

    print('Making ALL HV Maps')
    ###make_all_hv_maps()


#------------------------------------------------------------

def add_cumulative_data(ending):
    """add cumulative counts and charge to each file
    Will overwrite current data, so if files are added
    in middle of list, they should be accounted for.
    """
    data_list = glob.glob(os.path.join(MONITOR_DIR,'*%s*gainmap.fits'%ending))
    data_list.sort()
    print('Adding cumulative data to gainmaps for %s'%(ending))
    shape = pyfits.getdata(data_list[0], ext=('MOD_GAIN', 1)).shape
    total_counts = np.zeros(shape)
    total_charge = np.zeros(shape)

    for cci_name in data_list:
        fits = pyfits.open(cci_name, mode='update')
        #-- Add nothing if extension.data is None
        try:
            fits['counts'].data
            fits['charge'].data
            print("Skipping")
        except AttributeError:
            continue

        total_counts += fits['COUNTS'].data
        total_charge += fits['CHARGE'].data

        ext_names = [ext.name for ext in fits]

        if 'CUMLCNTS' in ext_names:
            fits['CUMLCNTS'].data = total_counts
        else:
            head_to_add = pyfits.Header()
            head_to_add.update('EXTNAME', 'CUMLCNTS')
            fits.append(pyfits.ImageHDU(header=head_to_add, data=total_counts))

        if 'CUMLCHRG' in ext_names:
            fits['CUMLCHRG'].data = total_charge
        else:
            head_to_add = pyfits.Header()
            head_to_add.update('EXTNAME', 'CUMLCHRG')
            fits.append(pyfits.ImageHDU(header=head_to_add, data=total_charge))

        fits.flush()
        fits.close()

#------------------------------------------------------------

def measure_gainimage(data_cube, mincounts=30, phlow=1, phhigh=31):
    """ measure the modal gain at each pixel

    returns a 2d gainmap

    """

    # Suppress certain pharanges
    for i in list(range(0, phlow+1)) + list(range(phhigh, len(data_cube))):
        data_cube[i] = 0

    counts_im = np.sum(data_cube, axis=0)

    out_gain = np.zeros(counts_im.shape)
    out_counts = np.zeros(counts_im.shape)
    out_std = np.zeros(counts_im.shape)

    index_search = np.where(counts_im >= mincounts)
    if not len(index_search):
        return out_gain, out_counts, out_std

    for y, x in zip(*index_search):
        dist = data_cube[:, y, x]

        g, fit_g, success = fit_distribution(dist)

        if not success:
            continue

        #-- double-check
        if g.mean.value <= 3:
            sub_dist = dist - g(np.arange(len(dist)))
            sub_dist[sub_dist < 0] = 0

            g2, fit2_g, success = fit_distribution(sub_dist, start_mean=15)

            if success and abs(g2.mean.value - g.mean.value) > 1:
                continue

        out_gain[y, x] = g.mean.value
        out_counts[y, x] = dist.sum()
        out_std[y, x] = g.stddev.value


    return out_gain, out_counts, out_std

#------------------------------------------------------------

def fit_ok(fit, fitter, start_mean, start_amp, start_std):

    #-- Check for success in the LevMarLSQ fitting
    if not fitter.fit_info['ierr'] in [1, 2, 3, 4]:
        return False

    #-- If the peak is too low
    if fit.amplitude.value < 12:
        return False

    if not fit.stddev.value:
        return False

    #-- Check if fitting stayed at initial
    if not (start_mean - fit.mean.value):
        return False
    if not (start_amp - fit.amplitude.value):
        return False

    #-- Not sure this is possible, but checking anyway
    if np.isnan(fit.mean.value):
        return False
    if (fit.mean.value <= 0) or (fit.mean.value >= 31):
        return False

    return True

#-------------------------------------------------------------------------------

def write_and_pull_gainmap(cci_name):
    """Make modal gainmap for cos cumulative image.

    """

    """
    #-- Disabling lookback
    previous_list = []###get_previous(current)

    mincounts = 30
    print 'Adding in previous data to distributions'
    for past_CCI in previous_list:
        print past_CCI.cci_name
        index = np.where(np.sum(current.big_array[1:31], axis=0) <= mincounts)

        for y, x in zip(*index):
            prev_dist = past_CCI.big_array[:, y, x]
            if prev_dist.sum() > mincounts:
                continue
            else:
                current.big_array[:, y, x] += prev_dist

    """

    current = CCI(cci_name, xbinning=X_BINNING, ybinning=Y_BINNING)
    current.write()

    index = np.where(current.gain_image > 0)

    info = {'segment': current.segment,
            'dethv': int(current.dethv),
            'expstart': round(current.expstart, 5)}

    if not len(index[0]):
        yield info
    else:
        for y, x in zip(*index):
            info['gain'] = round(float(current.gain_image[y, x]), 3)
            info['counts'] = round(float(current.counts_image[y, x]), 3)
            info['std'] = round(float(current.std_image[y, x]), 3)
            info['x'] = int(x)
            info['y'] = int(y)

            yield info


    """
    if current.accum_data:
        current.extracted_charge += current.accum_data*(1.0e-12*10**((gain_image-11.75)/20.5))

                if gain_flag == '':
                    gain_flag = 'fine'
                    fit_modal_gain = fit_center
                    fit_gain_width = fit_std

                    current.gain_image[y,x] = fit_modal_gain
                    current.modal_gain_width[y,x] = fit_gain_width

                    if fit_center > 21:
                        print '##########################'
                        print 'WARNING  MODAL GAIN: %3.2f'%(fit_center)
                        print 'Modal gain has been measured to be greater than 21. '
                        print 'PHA upper limit of 23 may cause flux to be lost.'
                        print '##########################'
                        send_email( subject='CCI high modal gain found',
                                    message='Modal gain of %3.2f found on segment %s at (x,y,MJD) %d,%d,%5.5f'%(fit_center,current.KW_SEGMENT,x,y,current.KW_EXPSTART) )
    """

#-------------------------------------------------------------------------------

def fit_distribution(dist, start_mean=None, start_amp=None, start_std=None):

    x_vals = np.arange(len(dist))

    start_mean = start_mean or dist.argmax()
    start_amp = start_amp or int(max(dist))
    start_std = start_std or 1.05

    g_init = models.Gaussian1D(amplitude=start_amp,
                               mean=start_mean,
                               stddev=start_std,
                               bounds={'mean': [1, 30]})
    g_init.stddev.fixed = True

    fit_g = fitting.LevMarLSQFitter()
    g = fit_g(g_init, x_vals, dist)

    success = fit_ok(g, fit_g, start_mean, start_amp, start_std)

    return g, fit_g, success

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

    print('Retrieving data from previous CCIs:')
    print('-----------------------------------')

    dethv = current_cci.KW_DETHV
    expstart = current_cci.KW_EXPSTART
    segment = current_cci.KW_SEGMENT
    cci_name = current_cci.input_file

    if ((not NUM_DAYS_PREVIOUS) or (not expstart)):
        print('None to find')
        return out_list

    path,file_name = os.path.split(cci_name)
    if segment == 'FUVA':
        ending = '*' + FUVA_string + '*'
    elif segment == 'FUVB':
        ending = '*' + FUVB_string + '*'
    else:
        print('Error, segment error in %s'%(file_name))
        print('Returning blank list')
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
            out_list.append(CCI(cci_file, xbinning=X_BINNING, ybinning=Y_BINNING))

        if len(out_list) >= 2*NUM_DAYS_PREVIOUS:
            print('Breaking off list.  %d files retrieved'%(2 * NUM_DAYS_PREVIOUS))
            break

    print('Found: %d files' % (len(out_list)))
    print([item.cci_name for item in out_list])
    print('-----------------------------------')

    return out_list

#-------------------------------------------------------------------------------

def explode(filename):
    """Expand an events list into a 3D data cube of PHA images

    This function bins event lists into the 3D datacube with a format
    like the CSUMs.  1 image containing the events with each integer PHA value
    will be stacked into the output datacube.

    Parameters
    ----------
    filename : str
        name of the COS corrtag file

    Returns
    -------
    out_cube : np.ndarray
        3D array of images for each PHA

    """

    if isinstance(filename, str):
        events = pyfits.getdata(filename, ext=('events', 1))
    else:
        raise ValueError('{} needs to be a filename of a COS corrtag file'.format(filename))

    out_cube = np.empty((32, 1024, 16384))

    for phaval in range(0, 32):
        index = np.where(events['PHA'] == phaval)[0]

        if not len(index):
            out_cube[phaval] = 0
            continue

        image, y_range, x_range = np.histogram2d(events['YCORR'][index],
                                                 events['XCORR'][index],
                                                 bins=(1024, 16384),
                                                 range=((0,1023), (0,16384))
                                                 )
        out_cube[phaval] = image

    return out_cube

#-------------------------------------------------------------------------------
