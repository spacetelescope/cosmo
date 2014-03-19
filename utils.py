from astropy.io import fits

__all__ = ['check_stim_global']

STIM_KEYWORDS = ['STIMA_LX',
                 'STIMA_LY',
                 'STIMA_RX',
                 'STIMA_RY',
                 'STIMB_LX',
                 'STIMB_LY',
                 'STIMB_RX',
                 'STIMB_RY']

#-------------------------------------------------------------------------------

class StimError(Exception):
    pass

#-------------------------------------------------------------------------------

def check_stim_global(filename):
    """ Check for stims missing from the entire observation

    CalCOS populates -1 if a stim is missing, this will check for that value

    """

    hdu = fits.open(filename)    

    if not hdu[0].header['DETECTOR'] == 'FUV':
        raise ValueError('Filename {} must be FUV data'.format(filename))

    missing_stims = []
    for keyword in STIM_KEYWORDS:
        if hdu[1].header[keyword] == -1:
            missing_stims.append(keyword)


    if len(missing_stims):
        raise StimError('{} has missing stims: {}'.format(filename, 
                                                          missing_stims))

#-------------------------------------------------------------------------------
