from __future__ import absolute_import

from astropy.io import fits

__all__ = ['check_stim_global']

STIM_KEYWORDS = {'FUVA':['STIMA_LX',
                         'STIMA_LY',
                         'STIMA_RX',
                         'STIMA_RY'],
                 'FUVB':['STIMB_LX',
                         'STIMB_LY',
                         'STIMB_RX',
                         'STIMB_RY']}

STIM_KEYWORDS['BOTH'] = STIM_KEYWORDS['FUVA'] + STIM_KEYWORDS['FUVB']

#-------------------------------------------------------------------------------

class StimError(Exception):
    pass

#-------------------------------------------------------------------------------

def check_stim_global(filename, verbose=0):
    """ Check for stims missing from the entire observation

    Checks for a negative value indicating the stim was not found internally
    by CalCOS.

    """

    if verbose:
        print "Checking for STIMS as found by CalCOS"

    hdu = fits.open(filename)

    if not hdu[0].header['DETECTOR'] == 'FUV':
        raise ValueError('Filename {} must be FUV data'.format(filename))

    segment = hdu[0].header['SEGMENT']
    if verbose: print "Segment: {}".format(segment)

    missing_stims = []
    for keyword in STIM_KEYWORDS[segment]:
        if 'A_' in keyword: n_events = hdu[1].header['NEVENTSA']
        if 'B_' in keyword: n_events = hdu[1].header['NEVENTSB']

        if (hdu[1].header[keyword] < 0) and (n_events > 0):
            missing_stims.append(keyword)

        if verbose: print "{} @ {} n_events: {}".format(keyword,
                                                        hdu[1].header[keyword],
                                                        n_events)

    if len(missing_stims):
        raise StimError('{} has missing stims: {}'.format(filename,
                                                          missing_stims))

#-------------------------------------------------------------------------------
