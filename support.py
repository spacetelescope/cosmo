import sys, os  #fix this sometime  Remove this and run Dark_Monitor.py and see what is wrong
#--------------------,-------------------------------------------------------
#---------------------Plotting
#---------------------------------------------------------------------------

def init_plots(figsize=(20,8.5)):
    try: matplotlib
    except NameError: import matplotlib
    '''
    Sets plotting defaults to make things pretty
    '''
    matplotlib.rcParams['lines.markeredgewidth'] = .001
    matplotlib.rcParams['lines.linewidth']=2
    matplotlib.rcParams['patch.edgecolor']='grey'
    matplotlib.rcParams['font.size']=15.0
    matplotlib.rcParams['figure.figsize']=figsize
    matplotlib.rcParams['figure.subplot.left']=.1
    matplotlib.rcParams['figure.subplot.right']=.9
    matplotlib.rcParams['figure.subplot.top']=.92
    matplotlib.rcParams['figure.subplot.bottom']=.1
    matplotlib.rcParams['figure.subplot.wspace']=.2
    matplotlib.rcParams['figure.subplot.hspace']=.2
    matplotlib.rcParams['figure.facecolor']='white'
    matplotlib.rcParams['axes.facecolor']='white'
    matplotlib.rcParams['axes.edgecolor']='black'
    matplotlib.rcParams['axes.linewidth']=1
    matplotlib.rcParams['axes.grid']=True
    matplotlib.rcParams['xtick.major.size']=7
    matplotlib.rcParams['ytick.major.size']=7
    matplotlib.rcParams['xtick.minor.size']=4
    matplotlib.rcParams['ytick.minor.size']=4

#---------------------------------------------------------------------------

def format_axis(axis=None,precision='%.1f',ticksize=1):
    '''
    from matplotlib.ticker import *
    formats axis label precision
    '''
    import pylab
    from matplotlib.ticker import FormatStrFormatter
    ax=pylab.gca()
    if axis in ('x','X'): ax.xaxis.set_major_formatter(FormatStrFormatter(precision))
    elif axis in ('y','Y'): ax.yaxis.set_major_formatter(FormatStrFormatter(precision))

    for line in ax.xaxis.get_ticklines(): line.set_markeredgewidth(ticksize)
    for line in ax.yaxis.get_ticklines(): line.set_markeredgewidth(ticksize)

#---------------------------------------------------------------------------

def big_ticks(width=1):
    '''
    #for each axis or whichever axis you want you should
    Increases major tick size
    '''
    ax = pylab.gca()
    for line in ax.xaxis.get_ticklines(): line.set_markeredgewidth(width)
    for line in ax.yaxis.get_ticklines(): line.set_markeredgewidth(width)

#---------------------------------------------------------------------------
#---------------------Instrument Specific
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

#---------------------------------------------------------------------------
#---------------------SQL
#---------------------------------------------------------------------------

def createXmlFile(ftp_dir=None,set=None, file_type="all", archive_user=None, archive_pwd=None, email=None, host='science3.stsci.edu', ftp_user=None, ftp_pwd=None):

    import sys, traceback, os, string, errno, glob, httplib, urllib, time

    exposure_template = string.Template('\
<?xml version=\"1.0\"?> \n \
<!DOCTYPE distributionRequest SYSTEM \"http://archive.stsci.edu/ops/distribution.dtd\"> \n \
  <distributionRequest> \n \
    <head> \n \
      <requester userId = \"$archive_user\" email = \"$email\" archivePassword = \"$archive_pwd" source = "starview"/> \n \
      <delivery> \n \
        <ftp hostName = \"$host\" loginName = \"$ftp_user\" loginPassword = \"$ftp_pwd\" directory = \"$ftp_dir\" secure = \"true\" /> \n \
      </delivery>    <process compression = \"none\"/> \n \
    </head> \n \
    <body> \n \
      <include> \n \
        <select> \n \
          $types \n \
        </select> \n \
        $datasets \n \
      </include> \n \
    </body> \n \
  </distributionRequest> \n' )

    print archive_user,archive_pwd,ftp_user,ftp_pwd,ftp_dir,email

    if not archive_user: archive_user = raw_input('I need your archive username: ')
    if not archive_pwd: archive_pwd = raw_input('I need your archive password: ')
    if not ftp_user: ftp_user = raw_input('I need the ftp username: ')
    if not ftp_pwd: ftp_pwd = raw_input('I need the ftp password: ')
    if not ftp_dir: ftp_dir = raw_input('Where would you like the files delivered: ')
    if not email: email = raw_input('Please enter the email address for the notification: ')
    datasets = "\n"
	
    if file_type == "all":
        types = "<suffix name=\"*\" />"
    elif file_type == "caldq":
        types = "<retrievalKeyword name=\"DataQuality\" /> \n           <retrievalKeyword name=\"JitterFiles\" />"
    elif file_type == "lpf":
        types = "<suffix name=\"LPF\" /> \n           <suffix name=\"SPT\" />"
    elif file_type == "raw":
        types = "<retrievalKeyword name=\"Uncalibrated\" />"
    elif file_type == "raw+dq+jit":
        types = "<retrievalKeyword name=\"Uncalibrated\" />\n           <retrievalKeyword name=\"JitterFiles\" /> \n           <retrievalKeyword name=\"DataQuality\" /> \n           <suffix name=\"MMD\" />\n           <suffix name=\"SPT\" />"  
    elif file_type == "crj":
        types = "<suffix name=\"CRJ\" />\n"
    else:
        types = "<suffix name=\"*\" />\n"
	
    request_template = exposure_template
    request_str = request_template.safe_substitute( archive_user = archive_user, \
                                                        archive_pwd = archive_pwd, \
                                                        email = email, \
                                                        host = host, \
                                                        ftp_dir = ftp_dir, \
                                                        ftp_user = ftp_user, \
                                                        ftp_pwd = ftp_pwd, \
                                                        types = types)
    request_template = string.Template(request_str)
    for item in set:
        datasets = datasets+"          <rootname>%s</rootname>\n" % (item)
    xml_file = request_template.safe_substitute(datasets = datasets)
    return xml_file

def submitXmlFile(xml_file,server):
    import sys, traceback, os, string, errno, httplib, urllib	

    if server == "dmsops1.stsci.edu":
        params = urllib.urlencode({'dadshost':'dmsops1.stsci.edu','dadsport': 4703, 'request': xml_file})
    else:
        params = urllib.urlencode({'dadshost':'sthubbins.stsci.edu','dadsport': 4703, 'request': xml_file})

    headers = {"Accept": "text/html", "User-Agent":"%sDADSAll" % os.environ.get("USER")}
    req = httplib.HTTPSConnection("archive.stsci.edu")
    req.request("POST", "/cgi-bin/dads.cgi", params, headers)
    resp = req.getresponse()
    textlines = resp.read()
    print 'Request Submitted'
    return textlines

class SybaseInterface:
        import sys
	def __init__(self,server,database,user="cosstis",fields="",tables="",where="",order="",distinct=False):
                import sys
		self.server = server
		self.dbname = database
		self.fields_txt = fields
		self.tables_txt = tables
		self.where_txt = where
		self.order_txt = order
		self.query_result = []
		self.query_dict = {}
		self.distinct = distinct
		self.user = user
	
	#find these on a solaris machine in /usr/local/sybase/stbin/{username}.dat
	passwords = {"ROBBIE": "janawtmaxi", "ZEPPO": "omwaddgazeba" , "HARPO": "W@@s6L9Np@ub"}
	
	def findLongest(self,name,strings):
		theLongest = len(name)
		for string in strings:
			if len(string) > theLongest:
				theLongest = len(string)
		return theLongest

	def csvPrint(self,result=None):
		if result == None:
			result = self.resultAsDict()
		columns = result["COLUMNS"]
		str = columns[0]
		for name in columns:
			str += "," + name
		str += "\n"
		for i,name in enumerate(result[columns[0]]):
			str += result[columns[0]][i]
			for cname in columns[1:]:
				str += "," + result[cname][i]
			str += "\n"
		return str
	
	def prettyPrint(self,result=None):
		if result == None:
			result = self.resultAsDict()
		columns = result["COLUMNS"]
		longest_values = []
		str = "|"
		for name in columns:
			longest_values.append(self.findLongest(name,result[name]))
			str = str + "%-*s|" % (longest_values[-1],name)
		str = str + "\n"
		for i,name in enumerate(result[columns[0]]):
			str = str + "|"
			for j,cname in enumerate(columns):
				str = str + "%-*s|" % (longest_values[j],result[cname][i])
			str = str + "\n"
		return str
	
	def setDistinct(self,distinct):
		self.distinct = distinct
	
	def setFieldsText(self,fields):
		self.fields_txt = fields
	
	def setFields(self,fields):
		self.fields_txt = fields[0]
		for field in fields[1:]:
			self.fields_txt = "%s,%s" % (self.fields_txt,field)
	
	def setTablesText(self,tables):
		self.tables_txt = tables
	
	def setTables(self,tables):
		self.tables_txt = "%s" % (self.convertTables(tables))

	def setOrder(self,order):
		self.order_txt = "%s" % (order[0])
	
	def convertTables(self,item):
		if item[0] == "TABLE":
			text_str = item[1]
		else:
			operation = item[0]
			table_a = self.convertTables(item[1])
			table_b = self.convertTables(item[2])
			on_list = self.getList(item[3])
			text_str = "%s %s %s ON %s" % (table_a,operation,table_b,on_list)
		return text_str

	def getList(self,item):
		text_str = "%s=%s" % (item[0][0],item[0][1])
		for pair in item[1:]:
			text_str = text_str + " AND %s=%s" % (pair[0],pair[1])
		return text_str
	
	def setWhereText(self,where):
		self.where_txt = where
	
	def setWhere(self,where):
		self.where_txt = "(%s)" % (self.convertWhere(where))

	def convertWhere(self,item):
		if item[0] == "ATOM":
			keyword = item[1]
			test = item[2]
			if test != "BETWEEN":
				if item[3] == 'T':
					value = "'%s'" % (item[4])
				elif item[3] == 'D':
					value = "%d" % (item[4])
				elif item[3] == 'F':
					value = "%e" % (item[4])
				else:
					value = "%s" % (item[4])
			else:
				if item[3] == 'D':
					value = "%d AND %d" % (item[4],item[5])
				elif item[3] == 'F':
					value = "%e AND %e" % (item[4],item[5])
			text_str = "%s %s %s" % (keyword,test,value)
		else:
			join_str = item[0]
			text_str = "(%s)" % (self.convertWhere(item[-1]))
			for i in range(len(item)-2):
				j = len(item) - i - 2
				text_str = "(%s) %s %s" % (self.convertWhere(item[j]),join_str,text_str)
		return text_str

	def resultAsText(self):
		if sys.platform == "darwin" or sys.platform == "linux2":
			result_txt = self.prettyPrint()
		elif sys.platform == "sunos5":
			result_txt = ""
			for line in self.query_result:
				result_txt = "%s%s\n" % (result_txt,line)
		else:
			result_txt = self.prettyPrint()
		return result_txt

	def resultAsDict(self):
		names = []
		if sys.platform == "darwin" or sys.platform == "linux2":
			for item in self.query_result[0].split("|||"):
				names.append(item.strip())
		elif sys.platform == "sunos5":
			for item in self.query_result[0].split("|")[1:-1]:
				names.append(item.strip())
			lengths = []
			for item in self.query_result[1].split("|")[1:-1]:
				lengths.append(len(item))
		mappings = {"COLUMNS":names}
		for name in names:
			mappings[name] = []
		if sys.platform == "darwin" or sys.platform == "linux2":
			for i in range(len(self.query_result[1:])):
				j = i + 1
				items = self.query_result[j].split("|||")
				for k in range(len(names)):
					mappings[names[k]].append(items[k])
		elif sys.platform == "sunos5":
			for i in range(len(self.query_result[2:])):
				j = i + 2
				items = self.lineSplit(self.query_result[j],lengths)
				for k in range(len(names)):
					mappings[names[k]].append(items[k])
		return mappings

	def lineSplit(self,line,lengths):
		counter = 1
		items = []
		for length in lengths:
			items.append(line[counter:(counter+length)])
			counter = counter + length + 1
		for i in range(len(items)):
			items[i] = items[i].strip()
		return items
	
	def buildString(self):
		query_string = "SELECT "
		if self.distinct:
			query_string = query_string + "DISTINCT "
		query_string = query_string + "%s FROM %s" % (self.fields_txt,self.tables_txt)
		if self.where_txt != "":
			query_string = query_string + " WHERE %s" % (self.where_txt)
		if self.order_txt != "":
			query_string = query_string + " ORDER BY %s" % (self.order_txt)
		return query_string
	
	def doQuery(self,query=""):
                if not sys.platform.startswith('linux'): 
                        errmsg='New data can only be retrieved from a linux system.\n'
                        errmsg+='\n'
                        errmsg+='Please move to a linux systen or get the data manually and run again \n'
                        errmsg+='with the --no_collect option.'
                        sys.exit(errmsg)

		if query:
			query_string = query
		else:
			query_string = self.buildString()
#		print query_string
		if sys.platform == "darwin" or sys.platform == "linux2":
			query_string = query_string + "\ngo\n"
			transmit,receive,error = os.popen3("tsql -S%s -D%s -U%s -P%s -t'|||'" % (self.server,self.dbname,self.user,self.passwords[self.server]))
		elif sys.platform == "sunos5":
			query_string = query_string + "\ngo\n"
			transmit,receive,error = os.popen3("/usr/local/syb_12.5/OCS-12_5/bin/isql -S%s -D%s -w50000 -s'|'" % (self.server,self.dbname))
		else:
			query_string = query_string + ";\n"
			transmit,receive,error = os.popen3("isql -S%s -D%s -s'|||' -w50000 -b" % (self.server,self.dbname))
		transmit.write(query_string)
		transmit.close()
		self.query_result = receive.readlines()
		receive.close()
		self.error_report = error.readlines()
		error.close()
#		print self.query_result
		for i in range(len(self.query_result)):
			self.query_result[i] = self.query_result[i].strip()
		if sys.platform == "darwin" or sys.platform == "linux2":
			self.query_result = self.query_result[4:-1]
			if sys.platform == "darwin":
				self.query_result = self.query_result[2:]
#			print self.query_result
			if "affected" in self.query_result[-1]:
				self.query_result = self.query_result[:-1]
			self.query_result[0] = self.query_result[0][6:]
		elif sys.platform == "sunos5":
			self.query_result = self.query_result[:-2]


#---------------------------------------------------------------------------
#---------------------Math
#---------------------------------------------------------------------------

def shift_fractional(in_array,xshift,yshift):
    ylen,xlen=in_array.shape

#---------------------------------------------------------------------------

def boxcar(x,n):
    from numpy import zeros
    x_smooth = []
    L = len(x)
    store = zeros((n,1),float)
    for u in range(L-n):
        for v in range(n):
            store[v] = x[u+v]
        av = float(sum(store)) / n
        x_smooth.append(av)
    for u in range(L-n,L):
        for v in range(L-u-1):
            store[v] = x[u+v]
        av = float(sum(store)) / n
        x_smooth.append(av)
    return x_smooth

#---------------------------------------------------------------------------

def congrid(a, newdims, method='linear', centre=False, minusone=False):
    import numpy as n
    import scipy.interpolate
    import scipy.ndimage
    '''Arbitrary resampling of source array to new dimension sizes.
    Currently only supports maintaining the same number of dimensions.
    To use 1-D arrays, first promote them to shape (x,1).
    
    Uses the same parameters and creates the same co-ordinate lookup points
    as IDL''s congrid routine, which apparently originally came from a VAX/VMS
    routine of the same name.

    method:
    neighbour - closest value from original data
    nearest and linear - uses n x 1-D interpolations using
                         scipy.interpolate.interp1d
    (see Numerical Recipes for validity of use of n 1-D interpolations)
    spline - uses ndimage.map_coordinates

    centre:
    True - interpolation points are at the centres of the bins
    False - points are at the front edge of the bin

    minusone:
    For example- inarray.shape = (i,j) & new dimensions = (x,y)
    False - inarray is resampled by factors of (i/x) * (j/y)
    True - inarray is resampled by(i-1)/(x-1) * (j-1)/(y-1)
    This prevents extrapolation one element beyond bounds of input array.
    '''
    if not a.dtype in [n.float64, n.float32]:
        a = n.cast[float](a)

    m1 = n.cast[int](minusone)
    ofs = n.cast[int](centre) * 0.5
    old = n.array( a.shape )
    ndims = len( a.shape )
    if len( newdims ) != ndims:
        print "[congrid] dimensions error. " \
              "This routine currently only support " \
              "rebinning to the same number of dimensions."
        return None
    newdims = n.asarray( newdims, dtype=float )
    dimlist = []

    if method == 'neighbour':
        for i in range( ndims ):
            base = n.indices(newdims)[i]
            dimlist.append( (old[i] - m1) / (newdims[i] - m1) \
                            * (base + ofs) - ofs )
        cd = n.array( dimlist ).round().astype(int)
        newa = a[list( cd )]
        return newa

    elif method in ['nearest','linear']:
        # calculate new dims
        for i in range( ndims ):
            base = n.arange( newdims[i] )
            dimlist.append( (old[i] - m1) / (newdims[i] - m1) \
                            * (base + ofs) - ofs )
        # specify old dims
        olddims = [n.arange(i, dtype = n.float) for i in list( a.shape )]

        # first interpolation - for ndims = any
        mint = scipy.interpolate.interp1d( olddims[-1], a, kind=method )
        newa = mint( dimlist[-1] )

        trorder = [ndims - 1] + range( ndims - 1 )
        for i in range( ndims - 2, -1, -1 ):
            newa = newa.transpose( trorder )

            mint = scipy.interpolate.interp1d( olddims[i], newa, kind=method )
            newa = mint( dimlist[i] )

        if ndims > 1:
            # need one more transpose to return to original dimensions
            newa = newa.transpose( trorder )

        return newa
    elif method in ['spline']:
        oslices = [ slice(0,j) for j in old ]
        oldcoords = n.ogrid[oslices]
        nslices = [ slice(0,j) for j in list(newdims) ]
        newcoords = n.mgrid[nslices]

        newcoords_dims = range(n.rank(newcoords))
        #make first index last
        newcoords_dims.append(newcoords_dims.pop(0))
        newcoords_tr = newcoords.transpose(newcoords_dims)
        # makes a view that affects newcoords

        newcoords_tr += ofs

        deltas = (n.asarray(old) - m1) / (newdims - m1)
        newcoords_tr *= deltas

        newcoords_tr -= ofs

        newa = scipy.ndimage.map_coordinates(a, newcoords)
        return newa
    else:
        print "Congrid error: Unrecognized interpolation type.\n", \
              "Currently only \'neighbour\', \'nearest\',\'linear\',", \
              "and \'spline\' are supported."
        return None

#---------------------------------------------------------------------------

def collapse_array(array,axis=0):
    try: operator
    except: import operator
    from numpy import rot90
    if axis in ('y','Y',1,'1'):
        array=rot90(array)
    collapsed=reduce(operator.add,array)
    return collapsed

#---------------------------------------------------------------------------

def chi_sq(fit,y,err):
    '''
    Returns chi_sq estimate for the fit.
    '''
    import numpy
    X_sq=numpy.sum(((y-fit)/err)**2)
    nu=len(y)-2.0
    sigma=numpy.sum(y-fit)/nu
    return X_sq

#---------------------------------------------------------------------------

def direct_correlate(a,b,maxdelay):
    try: numpy
    except: import numpy
    import pylab
    assert len(a)==len(b),'Input arrays are not equal lengh'
    n_elements=len(a)
    mean_a=a.mean()
    mean_b=b.mean()

    denom_a=(a-mean_a)*(a-mean_a)
    denom_b=(b-mean_b)*(b-mean_b)

    denom=numpy.sqrt(denom_a.sum()*denom_b.sum())

    r_values=[]
    for delay in range(-maxdelay,maxdelay):
        sxy=0
        for i in range(n_elements):
            j=i+delay
            if (j<0 | j>=n):
                continue
            else:
                sxy+=(a[i]-mean_a)*(b[i]-mean_b)
        r=sxy/denom
        print delay,sxy,denom
        r_values.append(r)
    pylab.plot(r_values)
    raw_input()

#---------------------------------------------------------------------------

def cross_corr(y1,y2):
    import scipy
    import scipy.optimize
    import pylab
    pylab.ion()

    # make x data
    num = 1000
    x = scipy.linspace(-10, 10, num=num)
    distancePerLag = x[1]-x[0]
# compute the cross-correlation between y1 and y2
    ycorr = scipy.correlate(y1, y2, mode='full')
    xcorr = scipy.linspace(0, len(ycorr)-1, num=len(ycorr))

    # define a gaussian fitting function where
    # p[0] = amplitude
    # p[1] = mean
    # p[2] = sigma
    fitfunc = lambda p, x: p[0]*scipy.exp(-(x-p[1])**2/(2.0*p[2]**2))
    errfunc = lambda p, x, y: fitfunc(p,x)-y

    # guess some fit parameters
    p0 = scipy.c_[max(ycorr), scipy.where(ycorr==max(ycorr))[0], 5]
    # fit a gaussian to the correlation function
    p1, success = scipy.optimize.leastsq(errfunc, p0.copy()[0],args=(xcorr,ycorr))
    # compute the best fit function from the best fit parameters
    corrfit = fitfunc(p1, xcorr)

    # get the mean of the cross-correlation
    xcorrMean = p1[1]

    # convert index to lag steps
    # the first point has index=0 but the largest (negative) lag
    # there is a simple mapping between index and lag
    nLags = xcorrMean-(len(y1)-1)

    # convert nLags to a physical quantity
    # note the minus sign to ensure that the
    # offset is positive for y2 is shifted to the right of y1
    # a negative offset means that y2 is shifted to the left of y1
    # I don't know what the standard notation is (if there is one)
    offsetComputed = -nLags*distancePerLag

    # see how well you have done by comparing the actual
    # to the computed offset
    print 'xcorrMean, nLags = ', \
         xcorrMean, ', ', nLags
    print offsetComputed
    # visualize the data
    # plot the initial functions
    pylab.subplot(211)
    pylab.plot(y1, 'ro')
    pylab.plot(y2, 'bx')
 
    # plot the correlation and fit to the correlation
    pylab.subplot(212)
    pylab.plot(xcorr, ycorr, 'k.')
    pylab.plot(xcorr, corrfit, 'r-')
    pylab.plot([xcorrMean, xcorrMean], [0, max(ycorr)], 'g-')
    raw_input()
    return offsetComputed

#---------------------------------------------------------------------------

def fft_correlate(a,b,alims=(0,-1),blims=None,wavelength=None):
    try: scipy
    except: import scipy
    try: argmax
    except: from numpy import argmax
    if blims==None: blims=alims
    c=(scipy.ifft(scipy.fft(a[alims[0]:alims[1]])*scipy.conj(scipy.fft(b[blims[0]:blims[1]])))).real
    shift=argmax(c)

    if wavelength:
	if len(wavelength)>1:
	    shift=shift*(wavelength[1]-wavelength[0])
	else:
	    shift=shift*wavelength
    #import pylab
    #print alims,blims
    #pylab.plot(a[alims[0]:alims[1]])
    #pylab.plot(b[alims[0]:alims[1]])
    #pylab.plot(c)
    #raw_input()
    return shift

#---------------------------------------------------------------------------
 
def find_fit(x,y,err=None,which='gaussian'):
    '''
    Performs fit to input data.  Computes function (gauss or lorentz) with 
    a spread of function parameters, then returns the parameters for the
    function with the smalles chi squared value.
    Returns the fit to optimal parameters.
    '''
    #try: scipy
    #except: import scipy
    import numpy
    y=y/numpy.max(y)
    max_index=numpy.argmax(y)
    x_0=x[numpy.argmax(y)]
    chi_all=[]
    alpha_all=[]
    x_0_all=[]
    if which=='gaussian':
        print 'Gaussian'
        for x_0 in numpy.linspace(x[max_index-len(x)/2],x[max_index+len(x)/2],80): #iterate from four points below and four points above the peak in 40 steps.
            for alpha in numpy.linspace(1,30,60):
                if which=='gaussian':
                    fit=(1.0/alpha)*numpy.sqrt(numpy.log(2)/numpy.pi)*numpy.exp((-1*numpy.log(2)*(x-x_0)**2)/alpha**2)
                elif which=='lorentzian':
                    fit=(1.0/numpy.pi)*(alpha)/((x-x_0)**2+alpha**2)
                if err==None:
                    err=numpy.fabs(y-fit)
                #pylab.plot(fit)
                #raw_input()
                chi_all.append(chi_sq(fit,y,err))
                alpha_all.append(alpha)
                x_0_all.append(x_0)
        alpha=alpha_all[numpy.argmin(chi_all)]   #find index of minimum in array
        x_0=x_0_all[numpy.argmin(chi_all)]       #find index of minimum in array
        if which=='gaussian':
            fit=(1.0/alpha)*numpy.sqrt(numpy.log(2)/numpy.pi)*numpy.exp((-1*numpy.log(2)*(x-x_0)**2)/alpha**2)
        elif which=='lorentzian':
            fit=(1.0/numpy.pi)*(alpha)/((x-x_0)**2+alpha**2)
    fit=numpy.array(fit)
    nu=len(y)-2.0
    sigma=float(numpy.sum(y-fit))/nu  #estimate error
    return fit

#---------------------------------------------------------------------------
    
def gauss_kern(size, sizey=None):
    """ Returns a normalized 2D gauss kernel array for convolutions """
    import scipy
    size = int(size)
    if not sizey:
        sizey = size
    else:
        sizey = int(sizey)
    x, y = scipy.mgrid[-size:size+1, -sizey:sizey+1]
    g = scipy.exp(-(x**2/float(size)+y**2/float(sizey)))
    return g / g.sum()

#---------------------------------------------------------------------------

def blur_image(im, n, ny=None):
    """ blurs the image by convolving with a gaussian kernel of typical
    size n. The optional keyword argument ny allows for a different
    size in the y direction.
    """
    import scipy.signal
    from scipy.signal import convolve
    g = gauss_kern(n, sizey=ny)
    improc = scipy.signal.convolve(im,g, mode='same')
    return improc

#---------------------------------------------------------------------------

def decimal_year(month,day,year):
    '''
    Returns decimal day for given mm,dd,yyyy
    '''
    day=int(day)
    month=int(month)
    year=int(year)
    if month==1: decimal_day=day+0
    elif month==2: decimal_day=day+31
    elif month==3: decimal_day=day+59
    elif month==4: decimal_day=day+90
    elif month==5: decimal_day=day+120
    elif month==6: decimal_day=day+151
    elif month==7: decimal_day=day+181
    elif month==8: decimal_day=day+212
    elif month==9: decimal_day=day+243
    elif month==10: decimal_day=day+273
    elif month==11: decimal_day=day+304
    elif month==12: decimal_day=day+334
    else:
        print 'Input Error'
    decimal_day=decimal_day/365.0
    decimal_date=year+decimal_day
    return decimal_date

#---------------------------------------------------------------------------

def day_of_year(month,day,year):
    '''
    Returns number of days since Jan. 1 1997
    '''
    if month==1: DOY=day
    elif month==2: DOY=day+31
    elif month==3: DOY=day+59
    elif month==4: DOY=day+90
    elif month==5: DOY=day+120
    elif month==6: DOY=day+151
    elif month==7: DOY=day+181
    elif month==8: DOY=day+212
    elif month==9: DOY=day+243
    elif month==10: DOY=day+273
    elif month==11: DOY=day+304
    elif month==12: DOY=day+334
    if ((year%4==0) and (year%100==0) and (year%100==0)):
        if DOY>60:
            DOY+=1
    DOY=DOY+(year-1997)*365
    return DOY

#---------------------------------------------------------------------------

def oneD(array):
    out=[]
    for row in array:
        for element in row:
            out.append(element)
    return out

#---------------------------------------------------------------------------

def sigma_clip(input_array,sigma=5,iterations=30,print_message=False):
    import os, numpy
    try: median
    except NameError: from numpy import where,median
    input_array=numpy.array(input_array)
    if len(input_array.shape)>1: 
    	input_array=input_array.flatten()
    for iteration in xrange(iterations):
    	if print_message:
    	    os.write(1,'\b\b\b\b\b\b\b\b\bPass: %d'%(iteration))
	std=input_array.std()
	ceiling=numpy.median(input_array)+5*std
	floor=numpy.median(input_array)-5*std
        index=numpy.where((input_array<ceiling) & (input_array>floor))[0]
        if len(index)==len(input_array):
            #print 'Done'
            break
        else:
	    input_array=input_array[index]
    if print_message: 
        print '\n'
    return numpy.median(input_array),input_array.mean(),input_array.std()

#---------------------------------------------------------------------------

def mjd_to_greg(mjd):
   #This comes from http://www.usno.navy.mil/USNO/astronomical-applications/astronomical-information-center/julian-date-form
   JD = mjd + 2400000.5
   JD = int(JD)
   L= JD+68569
   N= 4*L/146097
   L= L-(146097*N+3)/4
   I= 4000*(L+1)/1461001
   L= L-1461*I/4+31
   J= 80*L/2447
   K= L-2447*J/80
   L= J/11
   J= J+2-12*L
   I= 100*(N-49)+I+L
   Year = I
   Month = J
   Day = K
   month_to_day = {'0': 0,'1':31, '2':59, '3':90, '4':120, '5':151, '6':181, '7':212, '8':243, '9':273, '10':304, '11':334, '12':365}
   tot_day = (month_to_day[str(int(Month)-1)] + Day)
   day_in_year = 365.0
   if (Month >= 2) & (Year%4 == 4): #for leap year
      tot_day = tot_day + 1.0 
      day_in_year = 365.0 + 1.0 
   frac_day = tot_day / day_in_year
   fractional_year = Year + frac_day
   return (Year, Month, Day, fractional_year)

#---------------------------------------------------------------------------


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
    
#---------------------------------------------------------------------------

def expand_direct(a,bins=(2,2)):
    '''
    Block expand input array
    '''
    import numpy
    import os
    y,x=a.shape
    assert (x%bins[1]==0),'X binning factor not factor of x.'
    assert (y%bins[0]==0),'Y binning factor not factor of y.'
    out_array=numpy.zeros((y*bins[0],x*bins[1]))

    print 'Rebinning array'
    for i in range(y):
        done=100*float(i)/(y*bins[0])
        os.write(1,'\b\b\b\b\b\b%3.2f%%'%(done))
        for j in range(x):
            for k in range(bins[0]):
                for l in range(bins[1]):
                    out_array[bins[0]*i+k,bins[1]*j+l]=a[i,j]
    os.write(1,'\b\b\b\b\b\b%3.2f%%'%(100))
    print
    return out_array

#---------------------------------------------------------------------------

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

#---------------------------------------------------------------------------

def parallelize(function,call_list,threads):
    import multiprocessing as mp
    pool = mp.Pool(processes=threads)
    pool.map(function,call_list)

#---------------------------------------------------------------------------

class Logger(object):
    def __init__(self,filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()

#---------------------------------------------------------------------------

def progress_bar(iteration,total):
    import sys
    
    bar_length = 20
    frac_done = (iteration + 1) / float(total)
    percent_done = int(100 * frac_done)
    bar_done = int(bar_length * frac_done)

    #Write to initial stdout, incase stdout was redirected in program
    #fixes writing each step of the bar to log files.
    sys.__stdout__.write('\r')
    sys.__stdout__.write("[%-20s] %d%%" % ('='* bar_done, percent_done))
    sys.__stdout__.flush()

#---------------------------------------------------------------------------

def send_email(subject=None,message=None,from_addr=None,to_addr=None):
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
