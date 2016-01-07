#!/usr/bin/env python

'''
Script to retrieve and analyze COS dark data for the NUV and FUV channels.
'''

from __future__ import division

__author__ = 'Justin Ely'
__maintainer__ = 'Justin Ely'
__email__ = 'ely@stsci.edu'
__status__ = 'Active'

# Solution for "_tkinter.TclError: couldn't connect to display
# "localhost:0"" when using cronjob

import matplotlib
matplotlib.use('Agg')

import glob
import os
import string
import shutil
import sys
sys.path.insert(0, '../'
                )
import scipy
from scipy.signal import medfilt
import numpy
import pylab
import pyfits
import matplotlib
import sqlite3
import datetime
from time import sleep, localtime, asctime
from matplotlib.ticker import *
import argparse
from support import decimal_year, init_plots, corrtag_image, send_email, rebin, mjd_to_greg
#from support import SybaseInterface
#from support import createXmlFile, submitXmlFile

from mast_interface.SybaseInterface import SybaseInterface
from mast_interface.DADSAll import createXmlFile, submitXmlFile

#----------------------------------------------------------------------
#------Constants

base_directory = '/grp/hst/cos/Monitors/Darks/'

web_directory = '/grp/webpages/COS/'
retrieve_dir = os.path.join(base_directory, 'requested/')

FUV_PROPOSALS = [12716, 13121, 13521]
NUV_PROPOSALS = [12720, 13126, 13528]

#-----------------------------------------------------------------------


class super_dark:

    def __init__(self, detector):
        if detector == 'FUVA':
            self.channel = 'FUV'
            self.segment = 'FUVA'
            self.shape = (1024, 16384)
            self.xlim = (1200, 15099)
            self.ylim = (380, 680)
            self.pha_min = 2
            self.pha_max = 23
            self.search_string = '*corrtag_a.fits'

        elif detector == 'FUVB':
            self.channel = 'FUV'
            self.segment = 'FUVB'
            self.shape = (1024, 16384)
            self.xlim = (950, 15049)
            self.ylim = (440, 720)
            self.pha_min = 2
            self.pha_max = 23
            self.search_string = '*corrtag_b.fits'

        elif detector == 'NUV':
            self.channel = 'NUV'
            self.segment = 'NUV'
            self.shape = (1024, 1024)
            self.xlim = (0, 1023)
            self.ylim = (0, 1023)
            self.pha_min = 0
            self.pha_max = 32
            self.search_string = '*corrtag.fits'

        self.path = ''
        self.exp_dates = []
        self.mean_date = 0
        self.dark_array_list = []
        self.pha_hist_list = []
        self.dark_rates_list = []
        self.dark_mean = 0
        self.dark_std = 0
        self.summed_dark = numpy.zeros(self.shape)
        self.exptime_list = []
        self.total_exptime = 0
        self.temperatures = []

        self.start_mjd = []
        self.start_decyear = []

        self.included_files = []
        self.all_longitude = []
        self.all_latitude = []

#-----------------------------------------------------------------------


def update_database():
    """Update database with dark information for Brian's monitors
    """
    db = sqlite3.connect("/grp/hst/cos/Monitors/DB/cos_dark_stats")
    for detector in ['FUVA', 'FUVB', 'NUV']:
        darktab = pyfits.getdata(
            base_directory + detector[:3] + '/dark_rates_' + detector + '.fits')

        c = db.cursor()
        table = '%s_stats' % (detector)
        try:
            c.execute(
                """CREATE TABLE %s (id integer PRIMARY KEY, mjd real, date real, dark real, dev real)""" %
                (table))
        except sqlite3.OperationalError:
            c.execute("""DROP TABLE %s""" % (table))
            c.execute(
                """CREATE TABLE %s (id integer PRIMARY KEY, mjd real, date real, dark real, dev real)""" %
                (table))
        for i, line in enumerate(darktab):
            if not len(line) == 4:
                continue
            mjd = float(line[0])
            date = float(line[1])
            dark = float(line[2])
            std = float(line[3])
            c.execute("""INSERT INTO %s VALUES (?,?,?,?,?)""" %
                      (table), (i + 1, mjd, date, dark, std))
        db.commit()  # none of the changes you made are final until this step
        c.execute("""SELECT * FROM %s """ % (table))
        for row in c:
            print row
            '''
            c.execute("""SELECT * FROM %s"""%(table))
            results = c.fetchall()
            if len(results) > 0:
                for row in results:
                    id = row[0]
                    name = row[1]
                    value = row[2]
                    print id,name,value*5
            '''
#-----------------------------------------------------------------------


def find_done(detector):
    if detector == 'FUV':
        ending = '_corrtag_a.fits'
    elif detector == 'NUV':
        ending = '_corrtag.fits'
    else:
        print 'ERROR: detector not recognized'

    done_obs = set()
    for root, dirs, files in os.walk(base_directory + detector):
        if not 'requested' in root:
            for item in files:
                if (ending in item) and (item[0] == 'l'):
                    done_obs.add(item[:9].upper())

    return done_obs

#-----------------------------------------------------------------------


def collect_new(
    done_obs, detector, ftp_dir=None, set=None, type="all", archive_user=None,
    archive_pwd=None, email=None, host='science3.stsci.edu',
        ftp_user=None, ftp_pwd=None, N_obs_req=2, file_type=None):
    '''
    Function to find and retrieve new datasets for given proposal.
    Uses modules created by B. York: DADSAll.py and SybaseInterface.py.
    '''
    print '#----------------------------#'
    print 'Searching for new observations'
    print '#----------------------------#'
    if detector == 'FUV':
        proposal_list = FUV_PROPOSALS
        N_obs_req = 5
    elif detector == 'NUV':
        proposal_list = NUV_PROPOSALS
        N_obs_req = 2

    query = SybaseInterface("HARPO", "dadsops_rep")
    OR_part = "".join(["science.sci_pep_id = %d OR " % (proposal)
                      for proposal in proposal_list])[:-3]
    query.doQuery(query="SELECT science.sci_data_set_name FROM science WHERE ( " +
                  OR_part + " ) AND science.sci_targname='DARK'")
    all_dict = query.resultAsDict()

    # remove non-obs entries in dictionary
    all_obs = all_dict[all_dict.keys()[0]][2:]
    new_obs = [obs for obs in all_obs if obs not in done_obs]

    if len(new_obs) == 0:
        print 'No new files to retrieve.'
        return [], False

    elif len(new_obs) < N_obs_req:
        print 'Not enough new to retrieve.'
        return [], False

    print 'New observations to retrieve: '
    print new_obs

    if not archive_user: archive_user = raw_input('I need your archive username: ')
    if not archive_pwd: archive_pwd = raw_input('I need your archive password: ')
    if not ftp_user: ftp_user = raw_input('I need the ftp username: ')
    if not ftp_pwd: ftp_pwd = raw_input('I need the ftp password: ')
    if not email: email = raw_input('Please enter the email address for the notification: ')

    xml = createXmlFile(
        ftp_dir=ftp_dir, set=new_obs, type='CTA', archive_user=archive_user,
        archive_pwd=archive_pwd, email=email, host=host,
        ftp_user=ftp_user, ftp_pwd=ftp_pwd)

    response = submitXmlFile(xml, 'dmsops1.stsci.edu')
    print response
    if ('SUCCESS' in response):
        success = True
    else:
        success = False
        print 'Request Failed'

    return new_obs, success

#-----------------------------------------------------------------------


def move_obs(new_obs, detector):
    if detector == 'FUV':
        search_string = os.path.join(retrieve_dir, '*corrtag_a.fits')
    elif detector == 'NUV':
        search_string = os.path.join(retrieve_dir, '*corrtag.fits')

    print 'Waiting for files to be delivered.'
    while len(glob.glob(search_string)) != len(new_obs):
        wait_minutes = 5
        sleep(wait_minutes * 60)

    list_to_move = glob.glob(os.path.join(retrieve_dir, '*corrtag*.fits'))
    list_to_move.sort()

    assert len(list_to_move) > 0, 'Empty list of new observations to move'

    DATE = pyfits.getval(list_to_move[0], 'DATE-OBS', 1)
    folder = string.replace(DATE, '-', '_')
    new_folder = base_directory + detector + '/%s' % (folder)
    if not os.path.exists(new_folder):
        os.mkdir(new_folder)

    for fits_file in list_to_move:
        path, file_name = os.path.split(fits_file)
        OBS_DATE = pyfits.getval(fits_file, 'DATE-OBS', 1)
        obs_day = int(OBS_DATE[-2:])
        ref_day = int(DATE[-2:])
        if ((obs_day > ref_day + 1) | (obs_day < ref_day - 1)):
            DATE = OBS_DATE
            folder = string.replace(DATE, '-', '_')
            new_folder = base_directory + detector + '/%s' % (folder)
            os.mkdir(new_folder)
        ROOTNAME = file_name[:9]
        spt_file = os.path.join(path, ROOTNAME + '_spt.fits')
        raw_file = fits_file.replace('corrtag', 'rawtag')

        shutil.move(fits_file, new_folder)
        if os.path.exists(spt_file):
            shutil.move(spt_file, new_folder)
        if os.path.exists(raw_file):
            shutil.move(raw_file, new_folder)

    for item in glob.glob(os.path.join(retrieve_dir, '*.fits')):
        os.remove(item)

#-----------------------------------------------------------------------


def get_directories(base, detector):
    '''
    Finds and returns a list of directories containing the dark exposures
    '''
    directories = []
    for year in range(1997, 2020):
        for month in ('01', '02', '03', '04', '05', '06',
                      '07', '08', '09', '10', '11', '12'):
            for day in ('01', '02', '03', '04', '05', '06',
                        '07', '08', '09', '10', '11', '12',
                        '13', '14', '15', '16', '17', '18',
                        '19', '20', '21', '22', '23', '24',
                        '25', '26', '27', '28', '29', '30', '31'):
                path = base + detector[:3] + '/' + \
                    str(year) + '_' + month + '_' + day + '/'
                if os.path.exists(path):
                    directories.append(path)

    # directories.append('/smov/cos/Data/12810/fasttrack/')
    return directories

#-----------------------------------------------------------------------


def make_vector(im):
    '''
    makes 1d vector used to scale the darks in y
    '''
    print 'Making dark scaling vector'
    coeff = sgcoeff(6, 1)
    rotated = numpy.rot90(im, k=3)
    summed = 0
    for line in rotated:
        summed += line

    summed[:100] = 0  # remove stims
    summed[-100:] = 0

    vector = scipy.convolve(summed, coeff, mode='same')

    # n=6
    # coeff=numpy.ones(n)/n

    # coeff=numpy.zeros(n)
    # coeff[0]=1
    # coeff[-1]=1
    # vector=scipy.convolve(summed,coeff,mode='same')

    return vector

#-----------------------------------------------------------------------


def saa_screen(events_hdu, xlimits=(0, 16384), ylimits=(0, 1024)):
    '''
    Computes dark rate for each second of the exposure,
    excludes events where max/min > 10
    '''
    assert events_hdu.header[
        'EXTNAME'] == 'EVENTS', 'Incorrect extension supplied'
    total = 1024 * 16384
    dark_rates = []
    valid = numpy.where((events_hdu.data['XCORR'] > xlimits[0]) &
                        (events_hdu.data['XCORR'] < xlimits[1]) &
                        (events_hdu.data['YCORR'] > ylimits[0]) &
                        (events_hdu.data['YCORR'] < ylimits[1]))

    valid_events = events_hdu.data[valid]
    for time in range(valid_events['TIME'][-1]):
        index = numpy.where(
            (valid_events['TIME'] > time) & (valid_events['TIME'] < time + 1))
        dark = valid_events['XCORR'][index].sum() / total
        dark_rates.append(dark)

    dark_rates = scipy.convolve(
        numpy.array(dark_rates), numpy.ones(20) / 20, mode='valid')
    ratio = dark_rates.max() / dark_rates.min()

    if ratio > 10:
        impacted = True
        print '  Warning, observation has been SAA impacted.'
        print '  This observation will not be included in any'
        print '  calculations or products'
    else:
        impacted = False
    return impacted

#-----------------------------------------------------------------------


def verify_hdu(fits):
    try:
        fits[1].data[1]
    except:
        print '  No data found: Skipping'
        return False

    try:
        fits[3].data
    except:
        print '  No timeline: Skipping'
        return False

    if len(fits[3].data) < 2:
        print 'Timeline extension only has 1 value.  Skipping'
        return False

    return True

#-----------------------------------------------------------------------


def run_monitor(detector):
    print '#------------------#'
    print 'Running dark monitor'
    print '#------------------#'
    directories = get_directories(base_directory, detector)
    for directory in directories:
        if not os.path.exists(os.path.join(directory, 'dark_stats_%s.fits' % (detector))):
            make_dark_file(directory, detector)
    find_trends(detector)

#-----------------------------------------------------------------------


def pha_hist_bonus(file_name, fits, directory, detector, xlim, ylim, expstart):

    index_all = numpy.where(((fits[1].data['XCORR'] > xlim[0]) &
                             (fits[1].data['XCORR'] < xlim[1])) &
                            ((fits[1].data['YCORR'] > ylim[0]) &
                             (fits[1].data['YCORR'] < ylim[1])))[0]
    pha_list_all = fits[1].data['PHA'][index_all]

    if detector == 'FUVA':
        other_xlim = xlim
        if expstart < 56172.96911:
            other_ylim = (467, 507)
        else:
            other_ylim = (506, 546)

    elif detector == 'FUVB':
        other_xlim = xlim
        if expstart < 56172.96911:
            other_ylim = (525, 565)
        else:
            other_ylim = (565, 605)

    index_other = numpy.where(((fits[1].data['XCORR'] > other_xlim[0]) &
                               (fits[1].data['XCORR'] < other_xlim[1])) &
                              ((fits[1].data['YCORR'] > other_ylim[0]) &
                               (fits[1].data['YCORR'] < other_ylim[1])))[0]
    pha_list_other = fits[1].data['PHA'][index_other]

    index_not_other = [item for item in index_all if item not in index_other]
    pha_list_not_other = fits[1].data['PHA'][index_not_other]

    pylab.figure(figsize=(8, 6))

    pylab.hist(pha_list_all, bins=32, range=(0, 31),
               color = 'k', alpha = .9, label='Global')
    pylab.hist(pha_list_not_other, bins=32, range=(0, 31),
               color = 'b', alpha = .7, label='Not Extraction')
    pylab.hist(pha_list_other, bins=32, range=(0, 31),
               color = 'r', alpha = .7, label='Extraction')
    pylab.ylim(0, 2000)
    pylab.xlabel('PHA')
    pylab.ylabel('Counts')
    pylab.title(fits[1].header['DATE-OBS'] + '   ' + file_name[:-5])

    pylab.legend(shadow=True, numpoints=1)
    pylab.savefig(directory + file_name[:-5] + '_hist.pdf', rasterized=True)
    shutil.copy(directory + file_name[:-5] + '_hist.pdf',
                base_directory + detector[:3].upper() + '/stills/')
    pylab.close()

#-----------------------------------------------------------------------


def make_dark_file(directory, detector):
    """Make superdark fits file for all dark observations in given directory
    """
    print directory
    superdark = super_dark(detector)
    input_list = glob.glob(os.path.join(directory, superdark.search_string))
    for item in input_list:
        SAA_IMPACTED = False
        path_to_file, file_name = os.path.split(item)
        rootname = file_name[:9]
        spt_file = os.path.join(path_to_file, rootname + '_spt.fits')
        print rootname
        fits = pyfits.open(item)

        if fits[0].header['EXPTYPE'] != 'DARK':
            continue

        if not verify_hdu(fits):
            print 'non-standard file'
            continue

        SAA_IMPACTED = saa_screen(fits[1], superdark.xlim, superdark.ylim)
        if detector == 'NUV' and rootname in ['lb8r1ta1q']:
            SAA_IMPACTED = True
        if SAA_IMPACTED:
            continue

        if detector == 'FUVA':
            temp_keyword = 'LDCAMPAT'
        elif detector == 'FUVB':
            temp_keyword = 'LDCAMPBT'
        elif detector == 'NUV':
            temp_keyword = 'LMMCETMP'

        expstart = fits[1].header['EXPSTART']
        dec_year = mjd_to_greg(expstart)[3]
        date = fits[1].header['DATE-OBS']
        temperature = pyfits.getval(spt_file, temp_keyword, ext=2)

        exptime = fits[1].header['EXPTIME']
        pha_min = superdark.pha_min
        pha_max = superdark.pha_max
        ylim = superdark.ylim
        xlim = superdark.xlim

        if detector != 'NUV':
            pha_hist_bonus(file_name, fits, directory, detector,
                           superdark.xlim, superdark.ylim, expstart)

        if superdark.channel == 'FUV':
            current_image = corrtag_image(fits[1].data, pha=(pha_min, pha_max))
        elif superdark.channel == 'NUV':
            current_image = corrtag_image(fits[1].data, NUV=True)
        else:
            print 'ERROR in superdark.channel'

        pylab.figure(figsize=(14, 8))
        if detector == 'NUV':
            image = current_image / exptime
            pylab.imshow(image, vmin=0, vmax=image.mean(),
                         cmap=pylab.get_cmap('gist_yarg'))
        else:
            pylab.imshow(rebin(current_image, bins=(4, 16), mode='weird'),
                         aspect='auto', vmin=0, vmax=1, cmap=pylab.get_cmap('gist_yarg'))
        pylab.colorbar()
        pylab.title('%s: %s' % (date, file_name[:-5]))
        pylab.xlabel('X Binned')
        pylab.ylabel('Y Binned')
        pylab.savefig(base_directory + detector[:3].upper() +
                      '/stills/' + file_name[:-5] + '_image.pdf', rasterized=True)
        pylab.close()

        dark_rate = current_image[ylim[0]:ylim[1],
                                  xlim[0]:xlim[1]].mean() / exptime
        superdark.included_files.append(rootname)
        obs_longitude, obs_latitude = get_orbit_info(fits['TIMELINE'].data)
        superdark.all_longitude.append(obs_longitude)
        superdark.all_latitude.append(obs_latitude)

        superdark.exp_dates.append(expstart)
        superdark.dark_rates_list.append(dark_rate)
        superdark.dark_array_list.append(current_image)
        superdark.summed_dark += current_image
        superdark.exptime_list.append(exptime)
        superdark.total_exptime += exptime
        superdark.start_mjd.append(expstart)
        superdark.start_decyear.append(dec_year)

        superdark.temperatures.append(temperature)

    if not len(superdark.included_files):
        return
    superdark.mean_date = numpy.mean(superdark.exp_dates)
    superdark.dark_mean = numpy.mean(superdark.dark_rates_list)
    superdark.dark_std = numpy.std(superdark.dark_rates_list)
    superdark.path = directory
    write_fits(superdark)
    plot_orbits(superdark)

#-----------------------------------------------------------------------


def region_trend(detector, region_list):
    [(5000, 9000, 545, 775), (10000, 14000, 550, 778)]
    dark_rate_all = [[] for row in region_list]
    dec_year_all = []
    directories = get_directories(base_directory, detector)
    print 'looping over directories'
    for directory in directories:
        print directory
        stats_file = os.path.join(directory, 'dark_stats_%s.fits' % (detector))
        if not os.path.exists(stats_file):
            continue

        fits = pyfits.open(stats_file)
        for ext in range(2, len(fits[2:]) + 1):
            # try:
            exptime = fits[ext].header['EXPTIME']
            mjd = fits[ext].header['EXPSTART']
            dec_year = mjd_to_greg(mjd)[3]
            dec_year_all.append(dec_year)
            for i, row in enumerate(region_list):
                print 'dark_rate'
                dark_rate = fits[ext].data[row[2]:row[3], row[0]:row[1]].mean()
                #dark_rate = numpy.mean(fits[ext].data)
                dark_rate_all[i].append(dark_rate / exptime)
            # except:
            #    pass
        del fits

    return dark_rate_all, dec_year_all

#-----------------------------------------------------------------------


def find_trends(detector):
    all_mean_dark = []
    all_mean_std = []
    all_mean_date = []
    all_dark = []
    all_date = []
    all_temp = []

    directories = get_directories(base_directory, detector)
    for directory in directories:
        stats_file = os.path.join(directory, 'dark_stats_%s.fits' % (detector))
        if not os.path.exists(stats_file):
            continue

        fits = pyfits.open(stats_file)
        # CHANGE these all to 1 at some point
        all_mean_dark.append(fits[0].header['MEANDARK'])
        all_mean_std.append(fits[0].header['DARK_STD'])
        #all_mean_date.append( fits[1].header['EXPSTART'] )
        for extension in fits[2:]:
            all_dark.append(extension.header['DARKRATE'])
            all_date.append(extension.header['EXPSTART'])
            all_temp.append(extension.header['TEMP'])

    plot_time(detector, all_dark, all_date, all_temp)
    plot_time_IHB(detector, all_dark, all_date, all_temp)

    make_dark_tab(detector, all_mean_dark, all_mean_std,
                  all_mean_date, all_mean_date, all_dark, all_date, all_temp)
    # FIXME Needs to have both MJD and DAte at some point

#-----------------------------------------------------------------------


def write_fits(superdark):

    out_name = os.path.join(superdark.path, 'dark_stats_%s.fits' %
                            (superdark.segment))

    # Ext 0
    hdu = pyfits.HDUList(pyfits.PrimaryHDU())
    hdu[0].header.update('TELESCOP', 'HST')
    hdu[0].header.update('INSTRUME', 'COS')
    hdu[0].header.update('DETECTOR', superdark.channel)
    hdu[0].header.update('SEGMENT', superdark.segment)
    hdu[0].header.update('OPT_ELEM', 'ANY')
    hdu[0].header.update('FILETYPE', 'DARKSTAT')
    hdu[0].header.update('PHA_MIN', superdark.pha_min)
    hdu[0].header.update('PHA_MAX', superdark.pha_max)
    hdu[0].header.update('MEANDARK', superdark.dark_mean)
    hdu[0].header.update('DARK_STD', superdark.dark_std)

    # Ext 1
    hdu.append(pyfits.core.ImageHDU(data=superdark.summed_dark))
    hdu[1].header.update('SEGMENT', superdark.segment)
    hdu[1].header.update('EXPTIME', superdark.total_exptime)
    hdu[1].header.update('EXTNAME', 'MEANDARK')

    '''
    ###Ext 2
 
    #vector_col=pyfits.Column('mjd','f4','MJD',array = vector) 
    date_col=pyfits.Column('date','f4','date',array=date)
    #date_col=pyfits.Column('date','10A','date',array=date)
    dark_col=pyfits.Column('dark','f4','cnts/sec/pixel',array=dark_rates)
    std_col=pyfits.Column('std','f4','pixel',array=dark_std)

    tab=pyfits.new_table([mjd_col,date_col,dark_col,std_col])
    hdu_out.append(tab)
    '''

    dark_arrays = superdark.dark_array_list
    dark_rates = superdark.dark_rates_list
    dark_exptimes = superdark.exptime_list
    dates = superdark.exp_dates
    temperatures = superdark.temperatures

    for array, rate, exptime, date, temp in zip(dark_arrays, dark_rates, dark_exptimes, dates, temperatures):
        head_to_add = pyfits.core.Header()
        head_to_add.update('EXTNAME', 'DARK')
        head_to_add.update('DARKRATE', rate)
        head_to_add.update('EXPTIME', exptime)
        head_to_add.update('EXPSTART', date)
        head_to_add.update('TEMP', temp)

        hdu.append(pyfits.core.ImageHDU(header=head_to_add, data=array))

    hdu.writeto(out_name, clobber=True)

#-----------------------------------------------------------------------


def get_orbit_info(timeline, frequency=20):
    '''Returns Latitute, longitude, and dark rate at a specific frequency for plotting
    '''
    latitude = []
    longitude = []
    frequency = int(len(timeline) / frequency)
    for i in range(0, len(timeline), frequency):
        latitude.append(timeline['LATITUDE'][i])
        longitude.append(timeline['LONGITUDE'][i])

    return longitude, latitude

#-----------------------------------------------------------------------


def get_annotate_locations():
    pass

#-----------------------------------------------------------------------


def plot_time(detector, dark_all, date_all, temp_all):
    """Plots detector dark rate vs time.
    """
    print 'Plotting Dark rate vs time'
    pylab.ioff()
    fig = pylab.figure()
    ax = fig.add_axes([.1, .3, .8, .6])

    date_to_predict = 2014.25

    dec_date_all = [mjd_to_greg(date)[3] for date in date_all]
    end_date = max(dec_date_all) + .5
    date_all = numpy.array(dec_date_all)
    dark_all = numpy.array(dark_all)

    ax.plot(dec_date_all, dark_all, color='k', marker='o',
            linestyle='', markersize=8, label='Dark Rate', zorder=1)
    ax.axvline(x=2012.326, ymin=0, ymax=1, color='b', linestyle='-',
               lw=2, label='COS Suspend', zorder=1, alpha=.4)
    ax.axvline(x=2012.980, ymin=0, ymax=1, color='b', linestyle='--',
               lw=2, label='Dec Safe', zorder=1, alpha=.4)
    ax.axvline(x=2013.126, ymin=0, ymax=1, color='b', linestyle=':',
               lw=2, label='Feb Safe', zorder=1, alpha=.4)

    if detector == 'NUV':
        parameters = scipy.polyfit(date_all, dark_all, 1)
        dates_to_predict = numpy.linspace(date_all[0], date_to_predict, 100)
        fit = scipy.polyval(parameters, dates_to_predict)
        pred = scipy.polyval(parameters, date_to_predict)

        ax.plot(dates_to_predict, fit, color='r', linestyle='-',
                linewidth=3, label='Fit', alpha=.6)
        ax.plot(date_to_predict, pred, color='r', marker='^',
                markersize=15, label='%1.4e' % pred)

    if detector != 'NUV':
        ax.axhline(y=1.5E-6, color='r', linestyle='--',
                   lw=3, label='1.5e-6', zorder=1, alpha=.6)

    pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
    pylab.gca().yaxis.set_major_formatter(FormatStrFormatter('%3.2e'))
    ax.set_xticklabels(['' for item in ax.get_xticklabels()])
    #ax.set_xlabel('Decimal Year')
    ax.set_ylabel('Mean Dark Rate cnts/sec/pix')
    pylab.title('Global Dark Rate: %s' % (detector.upper()))
    ax.set_xlim(2009.5, date_to_predict + .3)
    ax.legend(numpoints=1, shadow=True, loc='upper left')
    ax.grid(True)

    data = numpy.genfromtxt(base_directory + 'solar_flux.txt', dtype=None)
    date_atmo = numpy.array([line[0] for line in data])
    atmo = numpy.array([line[1] for line in data])

    atmo_smooth = scipy.convolve(atmo, numpy.ones(81) / 81.0, mode='same')
    #atmo_smooth = medfilt(atmo,81)

    #twin_ax = ax.twinx()
    '''
    twin_ax.plot(date_atmo,atmo,color='orange',marker='',linestyle='-',label='10.7cm',lw=1,alpha=.3, zorder = 1)
    twin_ax.plot(date_atmo[:-41],atmo_smooth[:-41],color='red',marker='',linestyle='-',label='10.7cm Smoothed',lw=3,alpha=.3, zorder=1)
    twin_ax.axvline(x=2012.326, ymin=0,ymax=1, color='b',linestyle='--', lw=2,label='COS Suspend', zorder=1, alpha=.4)
    twin_ax.axvline(x=2012.980, ymin=0,ymax=1, color='b',linestyle='--', lw=2,label='Dec Safe', zorder=1, alpha=.4)
    twin_ax.axvline(x=2013.126, ymin=0,ymax=1, color='b',linestyle='--', lw=2,label='Feb Safe', zorder=1, alpha=.4)
    twin_ax.set_ylabel('Radio Flux')
    '''
    # twin_ax.set_xlim(2009.5,end_date)
    # twin_ax.set_xlim(2009.5,date_to_predict+.5)
    # twin_ax.set_ylim(50,300)
    # twin_ax.set_ylim(0,1E5)
    #twin_ax.legend(numpoints=1,shadow=True,loc='upper left')
    # pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))

    temp_ax = fig.add_axes([.1, .09, .8, .19])
    if detector == 'NUV':
        temp_ax.plot(dec_date_all, temp_all, color='r',
                     linestyle='', markersize=8, marker='o')
        pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        temp_ax.set_xlabel('Decimal_year')
        temp_ax.set_ylabel('Temperature')
        temp_ax.set_xlim(2009.5, date_to_predict + .3)
        temp_ax.grid(True)
    else:
        temp_ax.plot(date_atmo, atmo, color='orange', marker='',
                     linestyle='-', label='10.7cm', lw=1, alpha=.9, zorder=1)
        temp_ax.plot(
            date_atmo[:-41], atmo_smooth[:-41], color='red', marker='',
            linestyle='-', label='10.7cm Smoothed', lw=3, alpha=1, zorder=1)
        pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        temp_ax.set_xlabel('Decimal_year')
        temp_ax.set_ylabel('Radio Flux')
        temp_ax.set_ylim(50, 200)
        temp_ax.set_xlim(2009.5, date_to_predict + .3)
        temp_ax.legend(numpoints=1, shadow=True, loc='best')
        temp_ax.grid(True)

    pylab.savefig(base_directory +
                  detector[:3] + '/dark_vs_time_' + detector + '.pdf')
    pylab.close()

#-----------------------------------------------------------------------


def plot_time_IHB(detector, dark_all, date_all, temp_all):
    """Plots detector dark rate vs time.
    """
    print 'Plotting Dark rate vs time'

    breakpoints = [1997, 2011, 2012.98]
    colors = ['g', 'r', 'b']
    date_to_predict = 2014.25

    pylab.ioff()
    fig = pylab.figure(figsize=(12, 10))
    ax = fig.add_axes([.1, .25, .8, .7])

    dec_date_all = [mjd_to_greg(date)[3] for date in date_all]

    ax.plot(dec_date_all, dark_all, color='k', marker='o',
            linestyle='', markersize=8, label='Dark Rate', zorder=10)
    pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
    pylab.gca().yaxis.set_major_formatter(FormatStrFormatter('%3.2e'))
    ax.set_xlabel('Decimal Year')
    ax.set_ylabel('Mean Dark Rate cnts/sec/pix')
    ax.set_xticklabels(['' for item in ax.get_xticklabels()])
    yticks = list(ax.get_yticks())
    yticks[0] = ''
    ax.set_yticklabels(yticks)
    pylab.title('Global Dark Rate: %s' % (detector.upper()))
    ax.grid(True)

    parameters = scipy.polyfit(dec_date_all, dark_all, 1)
    fit = scipy.polyval(parameters, dec_date_all)
    ax.plot(dec_date_all, fit, color='r', lw=5, linestyle='--', zorder=0)

    temp_ax = fig.add_axes([.1, .09, .8, .16])
    temp_ax.plot(dec_date_all, temp_all, color='r',
                 linestyle='', markersize=8, marker='o')
    pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
    yticks = list(temp_ax.get_yticks())
    yticks[0] = ''
    temp_ax.set_yticklabels(yticks)
    temp_ax.set_xlabel('Decimal year')
    temp_ax.set_ylabel('Temperature (C)')
    # temp_ax.set_xlim((2009.5,date_to_predict+.3))
    temp_ax.grid(True)

    pylab.savefig(base_directory + detector[:3]
                  + '/dark_vs_time_' + detector + '_IHB.pdf')
    pylab.close()

#-----------------------------------------------------------------------


def plot_orbits(superdark):
    """Makes a plot showing the orbital tracks of the dark exposures and the SAA contours.
    """
    print 'Plotting Orbit'

    detector = superdark.segment
    latitude = superdark.all_latitude
    longitude = superdark.all_longitude
    dark = superdark.dark_rates_list
    all_dark = []
    for i in range(len(latitude)):
        for j in range(len(latitude[i])):
            all_dark.append(dark[i])

    all_latitude = numpy.array([item for orbit in latitude for item in orbit])
    all_longitude = numpy.array(
        [item for orbit in longitude for item in orbit])
    index = numpy.where(all_longitude > 180)[0]
    all_longitude[index] -= 360

    latitude_annotate = [item[-1] for item in latitude]
    longitude_annotate = [item[-1] for item in longitude]

    pylab.ioff()
    fig = pylab.figure()
    ax = fig.add_subplot(1, 1, 1)
    im = pylab.imread(base_directory + 'map_world.jpg')
    ax.imshow(im, interpolation='bilinear',
              origin='lower', extent=[-180, 180, -90, 90])

    for name, x, y in zip(superdark.included_files, longitude_annotate, latitude_annotate):
        if x > 180:
            x -= 360
        ax.annotate(name, (x, y), color='y')

    cax = ax.scatter(all_longitude, all_latitude, c=all_dark, marker='o',
                     s=45, cmap=pylab.get_cmap('jet'), norm=None, linewidths=None)
    cbar = fig.colorbar(cax, shrink=0.5, aspect=5)

    saa_latitude = []
    saa_longitude = []
    saa_contours = get_saa(31)
    for line in saa_contours:
        saa_latitude.append(line[0])
        saa_longitude.append(line[1])
    saa_latitude.append(saa_contours[0][0])
    saa_longitude.append(saa_contours[0][1])
    for i, element in enumerate(saa_longitude):
        if element > 180:
            saa_longitude[i] = (element - 360)
    ax.plot(saa_longitude, saa_latitude, 'ro-')
    ax.set_xbound(-180, 180)
    ax.set_ybound(-90, 90)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    # pylab.draw()
    fig.savefig(os.path.join(superdark.path, 'Dark_vs_orbit_%s.pdf' %
                (superdark.segment)))
    fig.clear()

#-----------------------------------------------------------------------


def sgcoeff(w, k):
    '''
    Make up Savitsky-Golay coefficients for window size 2w+1 and polynomial order k
    Description of the method is Numerical Recipes 3rd Edition p766
    '''
    s = 2 * w + 1  # Center channel +/- w
    n = k + 1  # kth order polynomial has k+1 terms
    x = scipy.mgrid[-w:w + 1]
    a = scipy.zeros((n, n))
    for i in range(n):
        for j in range(n):
            a[i, j] = sum(x ** (i + j))
    b = numpy.linalg.inv(a)
    sg = scipy.zeros((s))
    for i in range(s):
        for j in range(n):
            sg[i] = sg[i] + b[0, j] * x[i] ** j
    return sg

#-----------------------------------------------------------------------


def make_dark_tab(detector, mean_dark, mean_std, mean_mjd, mean_date, all_dark, all_mjd, all_temp):
    """Create fits table with dark information
    """
    print 'Making new Darktab'
    out_fits = base_directory + \
        detector[:3] + '/dark_rates_' + detector + '.fits'

    hdu_out = pyfits.HDUList(pyfits.PrimaryHDU())
    hdu_out[0].header.update('TELESCOP', 'HST')
    hdu_out[0].header.update('INSTRUME', 'COS')
    hdu_out[0].header.update('DETECTOR', 'FUV')
    hdu_out[0].header.update('COSCOORD', 'USER')
    hdu_out[0].header.update('VCALCOS', 2.0)
    hdu_out[0].header.update('PEDIGREE', 'INFLIGHT 11/05/2009 21/03/2012 ')
    hdu_out[0].header.update('DESCRIP', '')
    hdu_out[0].header.update('COMMENT', "= 'This file was created by J. Ely'")

    # Ext=1

    mjd_col = pyfits.Column('mjd', 'f4', 'MJD', array=mean_mjd)
    date_col = pyfits.Column('date', 'f4', 'date', array=mean_date)
    # date_col=pyfits.Column('date','10A','date',array=date)
    dark_col = pyfits.Column('dark', 'f4', 'cnts/sec/pixel', array=mean_dark)
    std_col = pyfits.Column('std', 'f4', 'pixel', array=mean_std)

    head = pyfits.core.Header()
    head.update('EXTNAME', 'MEANDARK')
    tab = pyfits.new_table([mjd_col, date_col, dark_col, std_col], header=head)
    hdu_out.append(tab)

    # Ext=2

    mjd_col = pyfits.Column('mjd', 'f4', 'MJD', array=all_mjd)
    # date_col=pyfits.Column('date','f4','date',array=mean_date)
    dark_col = pyfits.Column('dark', 'f4', 'cnts/sec/pixel', array=all_dark)
    temp_col = pyfits.Column('temp', 'f4', 'degrees', array=all_temp)

    head = pyfits.core.Header()
    head.update('EXTNAME', 'ALLDARK')
    tab = pyfits.new_table([mjd_col, dark_col, temp_col], header=head)
    hdu_out.append(tab)

    hdu_out.writeto(out_fits, clobber=True)

#-----------------------------------------------------------------------


def make_movies():
    '''
    Make animations
    '''
    print '#--------------------------#'
    print 'Making animated gifs.'
    print
    print 'This will take some time,'
    print 'I recommend you go do'
    print 'something else'
    print '#--------------------------#'
    stills_dir = base_directory + 'FUV/stills/'
    write_dir = base_directory + 'FUV/'
    copy_dir = web_directory + 'fuv_darks/'
    print 'FUVA PHA histograms'
    os.system('convert -delay 10 -loop 1 %s*a_hist.pdf %smovie_hist_FUVA.gif' %
              (stills_dir, write_dir))
    print 'FUVB PHA histograms'
    os.system('convert -delay 10 -loop 1 %s*b_hist.pdf %smovie_hist_FUVB.gif' %
              (stills_dir, write_dir))
    print 'FUVA images'
    os.system(
        'convert -delay 10 -loop 1 %s*corrtag_a_image.pdf %smovie_image_FUVA.gif' %
        (stills_dir, write_dir))
    print 'FUVB images'
    os.system(
        'convert -delay 10 -loop 1 %s*corrtag_b_image.pdf %smovie_image_FUVB.gif' %
        (stills_dir, write_dir))

    for item in glob.glob(write_dir + '*FUV*.gif'):
        shutil.copy(item, copy_dir)

    stills_dir = base_directory + 'NUV/stills/'
    write_dir = base_directory + 'NUV/'
    copy_dir = web_directory + 'nuv_darks/'
    print 'NUV images'
    os.system(
        'convert -delay 10 -loop 1 %s*corrtag_image.pdf %smovie_image_NUV.gif' %
        (stills_dir, write_dir))
    for item in glob.glob(write_dir + '*NUV*.gif'):
        shutil.copy(item, copy_dir)

#-----------------------------------------------------------------------


def get_saa(model_num):
    saa_dict = {}
    # SAA MODEL 31 COS FUV (XDL)
    # INITIAL ENTRY: CLONE OF MODEL 25.  PR 45488,       03/22/02
    # UPDATE:  SHIFTED WEST 6DEG.  PR 65147, 5/13/10
    # flux density 125.0
    saa_dict['saa_31'] = [(-28.3,         14.0),
                          (-27.5,         15.0),
                          (-26.1,         13.0),
                          (-19.8,          1.5),
                          (-9.6,        341.0),
                          (-7.6,        330.4),
                          (-6.0,        318.8),
                          (-7.9,        297.2),
                          (-12.0,        286.1),
                          (-17.1,        279.9),
                          (-20.3,        277.5),
                          (-23.5,        276.5),
                          (-26.0,        276.4),
                          (-28.6,        276.7)]
    # SAA MODEL 32 COS NUV (MAMA)
    # INITIAL ENTRY: CLONE OF MODEL 25. PR 45488,        03/22/02
    # UPDATE: SHIFTED WEST 6DEG.  PR 65147, 5/13/10
    # flux density 125.0
    saa_dict['saa_32'] = [(-28.3,         14.0),
                          (-27.5,         15.0),
                          (-26.1,         13.0),
                          (-19.8,          1.5),
                          (-9.6,        341.0),
                          (-7.6,        330.4),
                          (-6.0,        318.8),
                          (-7.9,        297.2),
                          (-12.0,        286.1),
                          (-17.1,        279.9),
                          (-20.3,        277.5),
                          (-23.5,        276.5),
                          (-26.0,        276.4),
                          (-28.6,        276.7)]
    return saa_dict['saa_' + str(model_num)]

#-----------------------------------------------------------------------


def grab_solar_files():
    base_webpage = 'http://www.swpc.noaa.gov/ftpdir/indices/old_indices/'
    this_year = datetime.datetime.now().year

    for index in ['DPD', 'DSD']:
        for year in range(1997, this_year):
            file_name = '%d_%s.txt' % (year, index)
            get_command = "wget --quiet -O %s%s %s%s" % (base_directory,
                                                         file_name, base_webpage, file_name)
            print get_command
            os.system(get_command)

        for quarter in ('Q1', 'Q2', 'Q3', 'Q4'):
            file_name = '%d%s_%s.txt' % (this_year, quarter, index)
            get_command = "wget --quiet -O %s%s %s%s" % (base_directory,
                                                         file_name, base_webpage, file_name)
            print get_command
            os.system(get_command)


def compile_txt():
    date = []
    flux = []
    input_list = glob.glob(os.path.join(base_directory, '*DSD.txt'))
    input_list.sort()
    for item in input_list:
        try:
            data = numpy.genfromtxt(item, skiprows=13, dtype=None)
        except IOError:
            continue
        except StopIteration:
            continue
        for line in data:
            # FIXX INCORRECT
            line_date = decimal_year(line[1], line[2], line[0])
            line_flux = line[3]
            if line_flux > 0:
                date.append(line_date)
                flux.append(line_flux)

    return numpy.array(date), numpy.array(flux)


def get_solar_data():
    print 'Gettting Solar flux data'
    for txtfile in glob.glob(os.path.join(base_directory, '*_D?D.txt')):
        os.remove(txtfile)

    grab_solar_files()
    date, flux = compile_txt()

    outfile = open(os.path.join(base_directory, 'solar_flux.txt'), 'w')
    for d, f in zip(date, flux):
        outfile.write('%4.5f  %d\n' % (d, f))
    outfile.close()


#-----------------------------------------------------------------------

def move_products():
    '''
    Move created pdf files to webpage directory
    '''
    print '#-------------------#'
    print 'Moving products into'
    print 'webpage directory'
    print '#-------------------#'
    for detector in ['FUV', 'NUV']:
        directories = get_directories(base_directory, detector)
        write_dir = web_directory + detector.lower() + '_darks/'
        last_run = directories[-1]
        move_list = glob.glob(last_run + '*.pdf')
        assert len(move_list) > 0, 'ERROR: nothing to move.'
        for item in move_list:
            path, file_to_move = os.path.split(item)
            shutil.copy(item, write_dir + file_to_move)
            print 'Moving: %s' % (file_to_move)

        move_list = glob.glob(base_directory + detector + '/*.pdf') + \
            glob.glob(base_directory + detector + '/*.gif')
        assert len(move_list) > 0, 'ERROR: nothing to move.'
        for item in move_list:
            path, file_to_move = os.path.split(item)
            shutil.copy(item, write_dir + file_to_move)
            print 'Moving: %s' % (file_to_move)
        os.system('chmod 777 ' + write_dir + '*.pdf')
        os.system('chmod 777 ' + write_dir + '*.gif')

#-----------------------------------------------------------------------


def send_report(detector):
    print detector
    if detector == 'NUV':
        proposal = NUV_PROPOSALS[-1]
    elif detector == 'FUV':
        proposal = FUV_PROPOSALS[-1]
    else:
        detector = ''
        proposal = 'something'
    subject = 'COS %s dark monitor complete' % (detector)
    message = 'Observations for %s, %s dark monitor, have been retrieved and analyzed and new products have\n' % (
        proposal, detector)
    message += 'been updated to the website.\n'
    message += '\n'
    message += 'Data and additional products can be found in %s.' % (
        os.path.join(base_directory, detector))
    message += '\n'
    message += '\n'
    message += 'Regards,\n'
    message += 'COS Dark Monitor'
    send_email(subject=subject, message=message)

#-----------------------------------------------------------------------


def clean(detector):
    print 'Cleaning directories of products'
    directories = get_directories(base_directory, detector)
    for directory in directories:
        stats_file = os.path.join(directory, 'dark_stats_%s.fits' % detector)
        if os.path.exists(stats_file):
            os.remove(stats_file)
#-----------------------------------------------------------------------


def remove_all(directory):
    for ext in ('?????????_hist.pdf', '*corrtag*.fits', '*_counts*.fits', '*_flt*.fits', '*_x1d*.fits'):
        os.system('rm ' + directory + '/' + ext)

#-----------------------------------------------------------------------


def remove_products(directory):
    for ext in ('*_counts*.fits', '*_flt*.fits', '*_x1d*.fits'):
        os.system('rm ' + directory + '/' + ext)

#-----------------------------------------------------------------------


def recal_all(detector):
    import calcos
    calcos_vers = '2.19.5'
    os.environ['lref'] = '/grp/hst/cdbs/lref/'
    directories = get_directories(base_directory, detector)
    for root in directories:
        #test = glob.glob(os.path.join(root,'*corrtag*.fits'))
        # if len(test):
        #    print len(test)
        #    calver = pyfits.getval(test[0],'CAL_VER',0)
        #    print calver,calcos_vers
            # if calcos_vers in calver:
            #    print root,' up to date: skipping'
            #    continue
        print root
        remove_all(root)
        files = glob.glob(os.path.join(root, '*raw*.fits'))
        for item in files:
            print item
            pyfits.setval(item, 'GEOFILE',
                          value='/user/ely/lref/new_geo.fits', ext=0)
            pyfits.setval(item, 'BRFTAB',
                          value='/user/ely/lref/new_brf.fits', ext=0)
            pyfits.setval(item, 'PHATAB',
                          value='/user/ely/lref/new_pha.fits', ext=0)
        for item in files:
            if '_a.fits' in item:
                os.system('/Users/ely/CALCOS/2.18.5/calcos.py -o ' +
                          root + '/ ' + item)
                # calcos.calcos(item,outdir=root)
                remove_products(root)

#-----------------------------------------------------------------------


def translate_infostring(input_string):
    info_dict = {}
    if not input_string:
        info_dict['arch_user'] = None
        info_dict['arch_pwd'] = None
        info_dict['ftp_user'] = None
        info_dict['ftp_pwd'] = None
        info_dict['ftp_host'] = 'science3.stsci.edu'
        info_dict['ftp_dir'] = retrieve_dir
        info_dict['email'] = None
    else:
        information = input_string.split(',')
        info_dict['arch_user'] = information[0]
        info_dict['arch_pwd'] = information[1]
        info_dict['ftp_user'] = information[2]
        info_dict['ftp_pwd'] = information[3]
        info_dict['ftp_host'] = information[4]
        info_dict['ftp_dir'] = information[5]
        info_dict['email'] = information[6]

    return info_dict

#-----------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--detector", dest="detector", default='All',
                        help="FUPA,FUVB,NUV or ALL")
    parser.add_argument("-c", "--no_collect", dest="collect_new",
                        action='store_false', default=True,
                        help="Search for new obs.")
    parser.add_argument("-r", "--redo", dest="redo",
                        action='store_true', default=False,
                        help="Delete products and redo all months: True or False")
    parser.add_argument("-u", "--user_information",
                        action='store', dest="user_information", default=None,
                        help="info string needed to request data")
    return parser.parse_args()

#-----------------------------------------------------------------------


def monitor():
    '''
    Main driver for monitor.
    '''
    args = parse_args()

    print '#--------------#'
    print asctime()
    print __file__
    print
    for attr, value in args.__dict__.iteritems():
        if attr != 'user_information':
            print '%s:  %s' % (attr, value)
    print '#--------------#'
    print

    while pylab.isinteractive():
        pylab.ion()
        pylab.ioff()
    init_plots()

    get_solar_data()

    if args.detector == 'All':
        detector_list = ['FUVA', 'FUVB', 'NUV']
    elif args.detector == 'FUV':
        detector_list = ['FUVA', 'FUVB']
    elif args.detector == 'NUV':
        detector_list = ['NUV']
    elif args.detector in ['FUVA', 'FUVB']:
        detector_list = [args.detector]
    else:
        sys.exit('Detector not recognized')

    for detector in detector_list:
        success = False
        if args.redo:
            clean(detector)

        if ((args.collect_new) and (detector != 'FUVB')):
            info_dict = translate_infostring(args.user_information)
            done_obs = find_done(detector[:3])
            new_obs, success = collect_new(
                done_obs=done_obs, detector=detector[
                    :3], ftp_dir=info_dict['ftp_dir'],
                set=None, type="all", archive_user=info_dict['arch_user'],
                archive_pwd=info_dict[
                    'arch_pwd'], email=info_dict['email'],
                host=info_dict['ftp_host'],
                ftp_user=info_dict['ftp_user'], ftp_pwd=info_dict['ftp_pwd'])
            if success:
                move_obs(new_obs, detector[:3])
            else:
                pass

        run_monitor(detector)
        if detector != 'FUVA' and success:
            send_report(detector[:3])

    make_movies()
    move_products()
    # update_database()

#-----------------------------------------------------------------------

if __name__ == '__main__':
    monitor()
