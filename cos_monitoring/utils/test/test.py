import numpy as np

from ..utils import rebin

#-------------------------------------------------------------------------------

def test_rebin_default():

    data = np.ones((6, 6))
    out = np.ones((3, 3)) * 4

    assert np.array_equal(rebin(data, (2, 2)), out), "Failure on simple integer array"


#-------------------------------------------------------------------------------
