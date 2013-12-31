import sys
sys.path.insert(0, '../')
import os
import numpy as np

from constants import MONITOR_DIR

import findbad
import gainmap
import cci_read

PRECISION = sys.float_info.epsilon
ONES_FILE =  os.path.join( MONITOR_DIR,'tests/ones_cci.fits' )

#------------------------------------------------------------------

def test_cci_read():
    data = cci_read.read( ONES_FILE ).reshape( (32,1024,16384) )
    for plane in data:
        assert plane.sum() == 16384*1024, "CCI array not read in correctly"

#------------------------------------------------------------------

def test_CCI_object():
    cci = gainmap.CCI_object( ONES_FILE )

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

def test_suppress_background():
    distribution = np.array( [10] * 32 )
    suppressed = distribution.copy()
    suppressed[0] = 0
    suppressed[-1] = 0
    
    assert ( gainmap.suppress_background( distribution ) == suppressed ).all(),"Distributions not suppressed correctly"

#------------------------------------------------------------------
