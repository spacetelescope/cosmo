from __future__ import absolute_import, division

from astropy.io import fits
import numpy as np

#-------------------------------------------------------------------------------

class InputError(Exception):
    pass

#-------------------------------------------------------------------------------

class SuperDark(object):
    def __init__(self, obs_list=[]):
        self._check_input(obs_list)

        hdu = fits.open(obs_list[0])

        self.detector = hdu[0].header['DETECTOR']
        self.segment = hdu[0].header['SEGMENT']
        self.source_files = obs_list[:]

        if self.detector == 'FUV':
            self.shape = (1024, 16384)
        elif self.detector == 'NUV':
            self.shape = (1024, 1024)
        else:
            raise ValueError('Detector {} not understood'.format(detector))


    def _check_input(self, obs_list):
        """Verify that input datasets are all the same detector, segment"""

        if len(obs_list) == 0:
            raise InputError('Please supply a list of inputs')

        for keyword in ['DETECTOR']:
            firstval = fits.getval(obs_list[0], keyword, 0)

            for obs in obs_list[1:]:
                if fits.getval(obs, keyword, 0) != firstval:
                    raise InputError("Multiple values of {} found"
                                     .format(keyword))

#-------------------------------------------------------------------------------

class NUVDark(SuperDark):
    def __init__(self, obs_list):
        SuperDark.__init__(self, obs_list)

        self.xlim = (0, 1024)
        self.ylim = (0, 1024)
        self.dark = np.zeros(self.shape)

#-------------------------------------------------------------------------------

class FUVDark(SuperDark):
    def __init__(self, obs_list):
        SuperDark.__init__(self, obs_list)

        xlim = {'FUVA':(1200, 15099),
                'FUVB':(950, 15049)}

        ylim = {'FUVA':(380, 680),
                'FUVB':(440, 720)}

        pha_lim = {'FUVA':(2, 23),
                   'FUVB':(2, 23)}

        self.pha = pha_lim[self.segment]
        self.xlim = xlim[self.segment]
        self.ylim = ylim[self.segment]

        self.dark_a = np.zeros(self.shape)
        self.dark_b = np.zeros(self.shape)

#-------------------------------------------------------------------------------
