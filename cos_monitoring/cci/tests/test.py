import sys
import os
import numpy as np
from astropy.io import fits

from ..constants import MONITOR_DIR
from ...cci import findbad, gainmap

PRECISION = sys.float_info.epsilon
ONES_FILE =  os.path.join(MONITOR_DIR, 'tests/ones_cci.fits')

#------------------------------------------------------------------

def test_CCI_object():
    cci = gainmap.CCI(ONES_FILE)

    ONES_TOTAL = 32*1024*16384

    assert cci.counts.sum() == ONES_TOTAL, "Counts in CCI object doesn't sum correctly. %d vs. %d"%(cci_counts.sum(),ONES_TOTAL)
    assert cci.big_array.sum( dtype=np.float64 ) == ONES_TOTAL, "Big array in CCI object doesn't sum correctly. %d vs. %d"%(cci.big_array.sum(),ONES_TOTAL)

#------------------------------------------------------------------

def test_time_fitting():
    TOLERANCE = 10E-10

    x_to_fit = np.array( range(10) )
    y_to_fit = np.array( range(10) )
    fit,parameters,success = findbad.time_fitting( x_to_fit,y_to_fit )
    fit_diff = fit - y_to_fit
    param_diff = parameters - np.array( [1,0] )

    assert np.all( fit_diff < TOLERANCE ),"Fitting didn't yield a straight line"
    assert np.all( param_diff < TOLERANCE),"Fitting didn't yield the right parameter tuple"
    assert success == True,"Fitting should have succeeded on this simple test"

#------------------------------------------------------------------

def test_rename():
    """Test the CCI renaming script
    """


    test_data = os.path.join(os.getcwd(), 'lft01_2013007232606_cci.fits.gz')
    hdu_out = fits.HDUList(fits.PrimaryHDU())
    hdu_out[0].header['DETHV'] = '167'
    hdu_out.writeto(test_data, clobber=True)

    assert gainmap.rename(test_data, 'print') == 'l_2013007232606_01_167_cci.fits.gz', \
        'Renaming not functioning properly'


#------------------------------------------------------------------
