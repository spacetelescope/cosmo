import os

from astropy.io import fits
import numpy as np
from calcos import ccos
from BeautifulSoup import BeautifulSoup
import urllib2
import re

#-------------------------------------------------------------------------------
def scrape_cycle(asn_id):

    if asn_id == 'NONE' or asn_id is None:
        return None

    url = 'http://archive.stsci.edu/cgi-bin/mastpreview?mission=hst&dataid={}'.format(asn_id)
    page = urllib2.urlopen(url)
    soup = BeautifulSoup(page)

    soup_text = soup.text
    regex = r"Cycle \d{2}"

    match = re.search(regex, soup_text)
    match = match.group(0)

    cycle_number = re.sub("[^0-9]", "", match)

    return cycle_number
#-------------------------------------------------------------------------------

def remove_if_there(filename):
    if os.path.exists(filename):
        os.remove(filename)

#-------------------------------------------------------------------------------

def enlarge(a, x=2, y=None):
    """Enlarges 2D image array a using simple pixel repetition in both dimensions.
    Enlarges by factor x horizontally and factor y vertically.
    If y is left as None, uses factor x for both dimensions."""
    import numpy as np
    a = np.asarray(a)
    assert a.ndim == 2
    if y == None:
        y = x
    for factor in (x, y):
        assert factor.__class__ == int
        assert factor > 0
    return a.repeat(y, axis=0).repeat(x, axis=1)

#-------------------------------------------------------------------------------

def rebin(a, bins=(2,2), mode='weird'):
    '''
    Rebins input array using array slices
    '''
    assert mode in ('slice','direct','weird','avg')
    y,x=a.shape
    ybin=bins[0]
    xbin=bins[1]
    assert (x%bins[1]==0),'X binning factor not factor of x.'
    assert (y%bins[0]==0),'Y binning factor not factor of y.'

    if mode=='slice':
        #From Phil
        a=a[0:y-1:ybin]+a[1:y:ybin]
        a=a[:,0:x-1:xbin]+a[:,1:x:xbin]

    elif mode=='direct':
        #Not tested, from me
        out_array=numpy.zeros((y,x))
        for i in range(y):
            for j in range(x):
                for k in range(bins[0]):
                    for l in range(bins[1]):
                        out_array[i,j]+=a[bins[0]*i+k,bins[1]*j+l]
        a=out_array

    elif mode=='avg':
        #Not tested, from online
        try: sometrue
        except: from numpy import sometrue,mod
        assert len(a.shape) == len(newshape)
        assert not sometrue(mod( a.shape, newshape ))
        slices = [ slice(None,None, old/new) for old,new in zip(a.shape,newshape) ]
        a=a[slices]

    elif mode=='weird':
        #not tested, from internet
        shape=(y/bins[0],x/bins[1])
        sh = shape[0],a.shape[0]//shape[0],shape[1],a.shape[1]//shape[1]
        a= a.reshape(sh).sum(-1).sum(1)

    return a

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

#-------------------------------------------------------------------------------

def send_email(subject=None, message=None, from_addr=None, to_addr=None):
    '''
    Send am email via SMTP server.
    This will not prompt for login if you are alread on the internal network.
    '''
    import os
    import getpass
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    users_email=getpass.getuser()+'@stsci.edu'

    if not subject:
	subject='Message from %s'%(__file__)
    if not message:
	message='You forgot to put a message into me'
    if not from_addr:
        from_addr=users_email
    if not to_addr:
        to_addr=users_email

    svr_addr='smtp.stsci.edu'
    msg = MIMEMultipart()
    msg['Subject']=subject
    msg['From']=from_addr
    msg['To']=to_addr
    msg.attach(MIMEText(message))
    s = smtplib.SMTP(svr_addr)
    s.sendmail(from_addr, to_addr, msg.as_string())
    s.quit()
    print '\nEmail sent to %s \n' %(from_addr)
