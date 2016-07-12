import numpy as np

from model_usage import count_sagged

#-------------------------------------------------------------------------------

def test_count_sagged():
    
    data = np.ones( (1024, 16384) ) * 5
    data[500] = 3
    assert count_sagged( data, thresh=3 ) == (16384, 16384), \
        'single full now not counted right'

    data[501, 0:8192] = 3
    assert count_sagged( data, thresh=3 ) == (24576, 16384), \
        'single full row and one half row not counted right'
    
    assert count_sagged( data, thresh=2 ) == (0, 0), \
        'nothing bad not counted right'

#-------------------------------------------------------------------------------
