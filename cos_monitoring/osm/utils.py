from astropy.io import fits
import numpy as np


__all__ = ['check_shifts']

#-------------------------------------------------------------------------------

class OSMError(Exception):
    """Custom exception"""
    pass

#-------------------------------------------------------------------------------

def check_shifts(filename):
    """Check if the found shifts are consistent

    """

    limits = {'FUV' : 50,
              'NUV' : 5}

    hdu = fits.open(filename)
    detector = hdu[0].header['DETECTOR']

    shifts = np.array([hdu[1].header[key] for key in
                      ['SHIFT1A', 'SHIFT1B', 'SHIFT1C']
                      if key in hdu[1].header])

    shifts = np.abs(shifts)
    check_index = np.where(shifts > 0)[0]
    shifts = shifts[check_index]

    if len(shifts) <= 1:
        return

    if np.any(shifts.mean() - shifts > limits[detector]):
        raise OSMError('{} has discrepant shift values {}'.format(filename,
                                                                  shifts))

#-------------------------------------------------------------------------------
