"""
Classes and functions to create PH filtering images for COS FUV observations
( PHAIMAGE )

"""

__author__ = 'Justin Ely'
__maintainer__ = 'Justin Ely'
__email__ = 'ely@stsci.edu'
__status__ = 'Active'

from datetime import datetime
import os
import sys
import glob
import pyfits
import numpy as np

from ..utils import enlarge, rebin
from constants import * #It's already been said

#------------------------------------------------------------

class Phaimage:
    """Creates a Phaimage object designed for use in the monitor.

    """

    def __init__(self,gainmap):
        """Open CCI file and create CCI Object"""

        self.out_fits = self.outfile(gainmap)
        self.a_file, self.b_file = self.inputs(gainmap)

        self.open_fits()

        ###self.a_image = self.fill_gaps( self.a_image, 'FUVA', self.DETHVA )
        ###self.b_image = self.fill_gaps( self.b_image, 'FUVB', self.DETHVB )

        self.make_phaimages()


    @classmethod
    def outfile(cls, gainmap):
        """ return the pulse height image name from the input gainmap

        """

        gainmap_path, gainmap_name = os.path.split(gainmap)
        segment = pyfits.getval(gainmap, 'SEGMENT')
        dethv = int(pyfits.getval(gainmap, 'DETHV'))

        if segment == 'FUVA':
            seg_string = FUVA_string
        elif segment == 'FUVB':
            seg_string = FUVB_string

        phf_name = gainmap_name.replace(seg_string, '_phaimage_').replace('gainmap.fits', 'phf.fits').replace('_{}_'.format(dethv), '_')

        return os.path.join(gainmap_path, phf_name)


    @classmethod
    def inputs(cls, gainmap):
        """ return the pulse height image name from the input gainmap

        """

        gainmap_path, gainmap_name = os.path.split(gainmap)
        segment = pyfits.getval(gainmap, 'SEGMENT')
        dethv = int(pyfits.getval(gainmap, 'DETHV'))

        both_inputs = [gainmap]

        if segment == 'FUVA':
            other_root = gainmap.replace(FUVA_string, FUVB_string).replace('_{}_'.format(dethv), '_???_')
        elif segment == 'FUVB':
            other_root = gainmap.replace(FUVB_string, FUVA_string).replace('_{}_'.format(dethv), '_???_')

        other_gainmap = glob.glob(other_root)
        if len(other_gainmap) != 1:
            raise IOError("too many gainmaps found {}".format(other_gainmap))
        else:
            other_gainmap = other_gainmap[0]

        both_inputs.append(other_gainmap)
        both_inputs.sort()

        return tuple(both_inputs)


    def open_fits(self):
        """Open CCI file and populated attributes with
        header keywords and data arrays.
        """

        a_hdu = pyfits.open( self.a_file )
        b_hdu = pyfits.open( self.b_file )

        self.DETECTOR = a_hdu[0].header['DETECTOR']
        ###Maybe I need individual ones for A and B?
        self.EXPSTART = min( a_hdu[0].header['EXPSTART'], b_hdu[0].header['EXPSTART'] )
        self.EXPEND = max( a_hdu[0].header['EXPEND'], b_hdu[0].header['EXPEND'] )
        self.EXPTIME = max( a_hdu[0].header['EXPTIME'], b_hdu[0].header['EXPTIME'] )
        self.DETHVA = a_hdu[0].header['DETHV']
        self.DETHVB = b_hdu[0].header['DETHV']

        self.a_image = a_hdu['MOD_GAIN'].data.copy()
        self.b_image = b_hdu['MOD_GAIN'].data.copy()



    def fill_gaps(self, image, segment, dethv ):
        """ Fill in gaps with available accumulated and extrapolated maps """

        dethv = int( dethv )

        extname = '{}INIT'.format( segment )
        fill_data = pyfits.getdata( os.path.join( MONITOR_DIR, 'total_gain.fits' ),
                                    ext=(extname, 1 ) )

        fill_data = rebin( fill_data, bins=(Y_BINNING, X_BINNING) ) / float((Y_BINNING * X_BINNING))

        index = np.where( fill_data > 0 )
        fill_data[ index ] += .393 * (dethv - 178)

        if not fill_data.shape == image.shape:
            raise IOError( 'Input shapes not equal' )

        fill_index = np.where( image <= 0 )
        image[ fill_index ] = fill_data[ fill_index ]
        #np.where( image < 0, 0, image )

        return image


    def make_phaimages(self):
        """Creates *_phf.fits reference files for each CCI period

        """

        self.a_low = self.set_limits( self.a_image, 'low' )
        self.a_high = self.set_limits( self.a_image, 'high' )
        self.b_low = self.set_limits( self.b_image, 'low' )
        self.b_high = self.set_limits( self.b_image, 'high' )

        assert not np.any( self.a_low < 0 ), 'low image contains negative values'
        assert not np.any( self.a_high > 23 ), 'high image contains too high values'
        assert not np.any( self.b_low < 0 ), 'low image contains negative values'
        assert not np.any( self.b_high > 23 ), 'high image contains too high values'

    def set_limits(self, gain_array, direction):
        """Sets the upper and lower pha limits
        in the *_phf.fits reference files."""
        lower_stop = 2
        upper_stop = 23
        drop_gain = -7
        raise_gain = 3

        phf_array = gain_array.copy()
        if direction == 'high':
            phf_array = np.where( phf_array > 0, phf_array + raise_gain, upper_stop )
            phf_array = np.where( ((phf_array > upper_stop) | (phf_array < 0 ))
                                  , upper_stop, phf_array )

        elif direction == 'low':
            phf_array = np.where( phf_array > 0, phf_array + drop_gain, lower_stop )
            phf_array = np.where( phf_array < lower_stop, lower_stop, phf_array )

        return phf_array

    def extrapolate_arrays(self):
        ### Could be used to expand outward from the farthest reaches
        ### but that may get dangerous.

        pass

    def writeout(self,out_fits=None, clobber=False):
        """
        Writes output phaimage fits file from input arrays.
        """

        out_fits = out_fits or self.out_fits

        # data should be unsigned integer as data can only be 0-31.  np dtype == 'u1'
        pha_low_a = enlarge( self.a_low.astype( np.dtype('u1') ) , y=Y_BINNING, x=X_BINNING )
        pha_high_a = enlarge( self.a_high.astype( np.dtype('u1') ), y=Y_BINNING, x=X_BINNING )
        pha_low_b = enlarge( self.b_low.astype( np.dtype('u1') ), y=Y_BINNING, x=X_BINNING )
        pha_high_b = enlarge( self.b_high.astype( np.dtype('u1') ), y=Y_BINNING, x=X_BINNING )

        #-------Ext=0
        hdu_out=pyfits.HDUList(pyfits.PrimaryHDU())

        date_time = str(datetime.now())
        date_time = date_time.split()[0]+'T'+date_time.split()[1]
        hdu_out[0].header.update('DATE',date_time,'Creation UTC (CCCC-MM-DD) date')
        hdu_out[0].header.update('TELESCOP','HST')
        hdu_out[0].header.update('INSTRUME','COS')
        hdu_out[0].header.update('DETECTOR','FUV')
        hdu_out[0].header.update('COSCOORD','USER')
        hdu_out[0].header.update('VCALCOS','2.14')
        hdu_out[0].header.update('USEAFTER',self.EXPSTART)
        hdu_out[0].header.update('DETHVA',self.DETHVA)
        hdu_out[0].header.update('DETHVB',self.DETHVB)
        hdu_out[0].header.update('OBSSTART',self.EXPSTART)
        hdu_out[0].header.update('OBSEND',self.EXPEND)
        hdu_out[0].header.update('PEDIGREE','INFLIGHT')
        hdu_out[0].header.update('FILETYPE','PULSE HEIGHT THRESHOLD REFERENCE IMAGE')
        #hdu_out[0].header.update('SRC_FILE',)
        hdu_out[0].header.update('DESCRIP','Gives pulse height thresholds for %10.5f to %10.5f'%(self.EXPSTART,self.EXPEND) )
        hdu_out[0].header.update('COMMENT',"= 'This file was created by J. Ely'")
        hdu_out[0].header.add_history('The history can be found here.')

        #-------EXT=1
        hdu_out.append(pyfits.ImageHDU( data = pha_low_a) )
        hdu_out[1].header.update('EXTNAME', 'FUVA')
        hdu_out[1].header.update('EXTVER', 1)

        #-------EXT=2
        hdu_out.append(pyfits.ImageHDU( data = pha_high_a) )
        hdu_out[2].header.update('EXTNAME', 'FUVA')
        hdu_out[2].header.update('EXTVER', 2)

        #-------EXT=3
        hdu_out.append(pyfits.ImageHDU( data = pha_low_b) )
        hdu_out[3].header.update('EXTNAME', 'FUVB')
        hdu_out[3].header.update('EXTVER', 1)

        #-------EXT=4
        hdu_out.append(pyfits.ImageHDU( data = pha_high_b) )
        hdu_out[4].header.update('EXTNAME', 'FUVB')
        hdu_out[4].header.update('EXTVER', 2)

        #-------Write to file
        hdu_out.writeto(out_fits, clobber=clobber)
        hdu_out.close()

        print 'WROTE: %s'% (out_fits)

#------------------------------------------------------------

def make_phaimages(clobber=False):
    """Creates *_phf.fits reference files for each CCI period
    """
    print '\n\n#--------------------#'
    print '#--Making PHAIMAGES--#'
    print '#--------------------#'

    all_gainmaps = glob.glob(MONITOR_DIR + '*gainmap.fits')
    all_gainmaps.sort()

    for gainmap in all_gainmaps:
        if os.path.exists( Phaimage.outfile(gainmap) ) and not clobber:
            print Phaimage.outfile(gainmap), 'Already exists. Skipping'
        else:
            try:
                inputs = Phaimage.inputs(gainmap)
            except:
                continue

            for item in inputs:
                if not os.path.exists(item):
                    print 'Missing input: {}'.format(item)
                    continue

            phaimage = Phaimage(gainmap)
            phaimage.writeout(clobber=clobber)

#------------------------------------------------------------
