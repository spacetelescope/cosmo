from astropy.io import fits
import numpy as np
from calcos import ccos

#-------------------------------------------------------------------------------

def corrtag_image(in_data,xtype='XCORR',ytype='YCORR',pha=(2,30),bins=(1024,16384),times=None,ranges=((0,1023),(0,16384)),binning=(1,1),NUV=False):
    try: histogram2d
    except NameError: from numpy import histogram2d,where,zeros
    try: getdata
    except NameError: from pyfits import getdata
    try: events=getdata(in_data,1)
    except: events=in_data

    xlength = (ranges[1][1]+1)
    ylength = (ranges[0][1]+1)
    xbinning = binning[1]
    ybinning = binning[0]

    if NUV:
        bins = (1024,1024)
        pha = (-1,1)
        ranges = ( (0,1023), (0,1023) )

    if times != None:
        index = where( (events['TIME']>=times[0]) & (events['TIME'] <= times[1]) )
        events=  events[index]

    index = where((events['PHA']>=pha[0])&(events['PHA']<=pha[1]))

    if len(index[0]):
        image,y_r,x_r = histogram2d(events[ytype][index],events[xtype][index],bins=bins,range=ranges)
    else:
        image = zeros( (bins[0]//binning[0],bins[1]//binning[1]) )

    return image

#-------------------------------------------------------------------------------

def bin_corrtag(corrtag_list, xtype='XCORR', ytype='YCORR', times=None):
    """Bin corrtag event lists into a 2D image.

    Filtering of unwanted events will be done before binning.  SDQFLAGS with
    the addition of PHA out of bounds (512), pixel outside active area (128),
    and bad times (2048) will be discarded.

    Parameters
    ----------
    corrtag_list : list
        list of datasets to combine into an image
    xtype : str, optional
        X-coordinate type
    ytype : str, optional
        Y-coordinate type

    Returns
    -------
    image : np.ndarray
        2D image of the corrtag

    """

    if not isinstance(corrtag_list, list):
        corrtag_list = [corrtag_list]

    final_image = np.zeros((1024, 16384)).astype(np.float32)

    for filename in corrtag_list:
        image = np.zeros((1024, 16384)).astype(np.float32)
        hdu = fits.open(filename)
        events = hdu['events'].data

        #-- No COS observation has data below ~923
        data_index = np.where((hdu[1].data['time'] >= times[0]) &
                              (hdu[1].data['time'] <= times[1]))[0]

        if not len(data_index):
            return image
        else:
            events = events[data_index]

        # Call for this is x_values, y_values, image to bin to, offset in x
        # ccos.binevents(x, y, array, x_offset, dq, sdqflags, epsilon)
        ccos.binevents(events[xtype].astype(np.float32),
                       events[ytype].astype(np.float32),
                       image,
                       0,
                       events['dq'],
                       0)

        final_image += image

    return final_image
