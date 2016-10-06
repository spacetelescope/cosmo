import sys
import os
import numpy as np
from astropy.io import fits

from ..constants import MONITOR_DIR
from ...cci import findbad, gainmap

PRECISION = sys.float_info.epsilon

#-------------------------------------------------------------------------------

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

#-------------------------------------------------------------------------------
