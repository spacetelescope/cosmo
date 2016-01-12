#---------------------------------------------------------------------------

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
