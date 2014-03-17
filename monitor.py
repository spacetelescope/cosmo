#!/usr/bin/env python

"""Script to monitor the COS FUV STIM pulses in TIME-TAG observations.

1: move through /smov/cos/Data/ and populate an sql database of STIM locations.
2: Make plots of overall trends, move to webpage.
3: Check through individual datasets, send email of abberant datasets.

"""

__author__ = 'Justin Ely'
__status__ = 'Active'

import pyfits
import os
import sys
import sqlite3
import matplotlib as mpl
mpl.use('agg')
from matplotlib.ticker import FormatStrFormatter
import matplotlib.pyplot as plt
import argparse
import datetime
import shutil
import time
import re
from ..support import send_email, corrtag_image
import glob
import numpy as np
from astropy.time import Time

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

#------------------------------------------------------
#--------------------Constants-------------------------
DATA_DIR = '/smov/cos/Data/'
MONITOR_DIR = '/grp/hst/cos/Monitors/Stim/'
WEB_DIR = '/grp/webpages/COS/stim/'

brf_file = '/grp/hst/cdbs/lref/s7g1700el_brf.fits'
DB_NAME = os.path.join(MONITOR_DIR, 'Stim_Locations.db')

#-----------------------------------------------------


def gaussian(height, center_x, center_y, width_x, width_y):
    """
    Returns a gaussian function with the given parameters
    """
    width_x = float(width_x)
    width_y = float(width_y)
    return lambda x, y: height * exp(
        -(((center_x - x) / width_x) ** 2 + ((center_y - y) / width_y) ** 2) / 2)

#-----------------------------------------------------


def find_center(data):
    """
    Returns (height, x, y, width_x, width_y)
    the gaussian parameters of a 2D distribution by calculating its
    moments
    """
    total = data.sum()
    if not total:
        return None, None

    X, Y = np.indices(data.shape)
    x = (X * data).sum() / total
    y = (Y * data).sum() / total
    col = data[:, int(y)]
    width_x = np.sqrt(
        abs((np.arange(col.size) - y) ** 2 * col).sum() / col.sum())
    row = data[int(x), :]
    width_y = np.sqrt(
        abs((np.arange(row.size) - x) ** 2 * row).sum() / row.sum())
    height = data.max()

    return y, x

#-----------------------------------------------------


def get_files(search_string='_rawtag_'):
    '''
    Walk through directories containing all COS exposures
    and return *_flt* path+filename.
    '''
    for root, dirs, files in os.walk(DATA_DIR):
        if not re.search('\d\d-\d\d-\d\d\d\d', root):
            continue
        if root.endswith('gzip'):
            continue
        if root.endswith('podfiles'):
            continue
        if 'experimental' in root:
            continue
        if 'Quality' in root:
            continue
        if 'Fasttrack' in root:
            continue
        if 'targets' in root:
            continue
        if 'experimental' in root:
            continue
        if 'Anomalies' in root:
            continue
        print root
        for file_name in files:
            if (search_string in file_name):
                yield root + '/' + file_name

#-----------------------------------------------------


def brf_positions(brftab, segment, position):
    """ Gets the search ranges for a stimpulse from
    the given baseline reference table
    """
    brf = pyfits.getdata(brftab)

    if segment == 'FUVA':
        row = 0
    elif segment == 'FUVB':
        row = 1

    if position == 'ul':
        x_loc = 'SX1'
        y_loc = 'SY1'
    elif position == 'lr':
        x_loc = 'SX2'
        y_loc = 'SY2'

    xcenter = brf[row][x_loc]
    ycenter = brf[row][y_loc]
    xwidth = brf[row]['XWIDTH']
    ywidth = brf[row]['YWIDTH']

    xmin = max(xcenter - xwidth, 0)
    xmax = min(xcenter + xwidth, 16384)
    ymin = max(ycenter - ywidth, 0)
    ymax = min(ycenter + ywidth, 1024)

    return xmin, xmax, ymin, ymax

#-----------------------------------------------------


def find_stims(image, segment, stim, brf_file):
    x1, x2, y1, y2 = brf_positions(brf_file, segment, stim)
    found_x, found_y = find_center(image[y1:y2, x1:x2])
    if (not found_x) and (not found_y):
        return -999, -999

    return found_x + x1, found_y + y1

#-----------------------------------------------------


def check_present(filename, database_name):
    """Returns True if filename is already in sql database,
    otherwise returns False
    """

    db = sqlite3.connect(database_name)
    c = db.cursor()
    c.execute(
     """SELECT * FROM measurements WHERE obsname='%s'""" %
     (filename))

    entries = [item for item in c]
    if len(entries):
        return True
    else:
        return False

#-----------------------------------------------------

def locate_stims(fits_file):

    DAYS_PER_SECOND = 1. / 60. / 60. / 24.

    file_path, file_name = os.path.split(fits_file)

    EXPTIME = pyfits.getval(fits_file, 'EXPTIME', ext=1)
    EXPSTART = pyfits.getval(fits_file, 'EXPSTART', ext=1)
    SEGMENT = pyfits.getval(fits_file, 'SEGMENT')

    if EXPTIME < 10:
        INCREMENT = .03
    elif EXPTIME < 100:
        INCREMENT = 1
    else:
        INCREMENT = 30

    INCREMENT = 30
    START = 0
    STOP = START + INCREMENT

    stim_info = []

    while STOP < (EXPTIME):
        im = corrtag_image(
            fits_file,
            xtype='RAWX',
            ytype='RAWY',
            times=(
                START,
                STOP))

        ABS_TIME = EXPSTART + START * DAYS_PER_SECOND

        found_ul_x, found_ul_y = find_stims(im, SEGMENT, 'ul', brf_file)
        found_lr_x, found_lr_y = find_stims(im, SEGMENT, 'lr', brf_file)

        print '%s \t %15.7f \t %4d \t %5.3f \t %4.3f \t %5.3f \t %4.3f' % (file_name, ABS_TIME, START, found_ul_x, found_ul_y, found_lr_x, found_lr_y)
        stim_info.append( (ABS_TIME, START, found_ul_x, found_ul_y, found_lr_x, found_lr_y) )

        START += INCREMENT
        STOP = START + INCREMENT

    return stim_info

#-----------------------------------------------------

def populate_db():
    """Populate a database with STIM locations for each dataset on a sub-sampled timescale

    Database contains:
    ------------------
    obsname, absolute time, time since EXPSTART, X (upper left), Y (upper left), X (lower right), Y (lower right)

    """

    db = sqlite3.connect(DB_NAME)
    c = db.cursor()
    table = 'measurements'
    try:
        c.execute(
    """CREATE TABLE %s (obsname text, abs_time real, start real, ul_x real, ul_y real, lr_x real, lr_y real)""" %
            (table))
    except sqlite3.OperationalError:
        pass

    DAYS_PER_SECOND = 1. / 60. / 60. / 24.

    for fits_file in get_files():
        file_path, file_name = os.path.split(fits_file)
        file_name, file_ext = os.path.splitext(file_name)
        if check_present(file_name, DB_NAME):
            continue

        stim_info = locate_stims(fits_file)

        for ABS_TIME, START, found_ul_x, found_ul_y, found_lr_x, found_lr_y in stim_info:
            c.execute(
    """INSERT INTO %s VALUES (?,?,?,?,?,?,?)""" % (table),
                (file_name,
                 ABS_TIME,
                 START,
                 found_ul_x,
                 found_ul_y,
                 found_lr_x,
                 found_lr_y))

        # Don't want to commit the changes until an entire observation is done.
        # This way the procedure can be stopped and restarted without missing
        # Parts of an observations
        db.commit()

#-----------------------------------------------------


def find_missing():
    """Return list of datasets that had a missing stim in
    any time period.
    """

    db = sqlite3.connect(DB_NAME)
    c = db.cursor()
    c.execute(
    """SELECT obsname,abs_time FROM measurements where ul_x='-999' or ul_y='-999' or lr_x='-999' or lr_y='-999'""")

    missed_data = [(item[0].strip(), item[1]) for item in c]
    all_obs = [item[0] for item in missed_data]
    all_mjd = [item[1] for item in missed_data]

    obs_uniq = list(set(all_obs))

    date_uniq = []
    for obs in obs_uniq:
        min_mjd = all_mjd[all_obs.index(obs)]
        date_uniq.append(min_mjd)

    date_uniq = Time(date_uniq, format='mjd', scale='utc')
    return obs_uniq, date_uniq

#-----------------------------------------------------


def coord_deviates(position_array):
    """Simple test for the deviation of a list of coordinates.

    Returns True if the standard deviation of the array about the mean
    is greater than a threshold value.  Otherwise returns False.

    """

    position_array = np.array(position_array)

    deviation = (position_array - position_array.mean()).std()

    if deviation > 2:
        return True
    else:
        return False

#-----------------------------------------------------


def check_individual():
    """Run tests on each individual datasets.

    Currently checks if any coordinate deviates from the mean
    over the exposure and produces a plot if so.

    """
    print '#--------------------------#'
    print 'Checking individual datasets'
    print '#--------------------------#'
    db = sqlite3.connect(DB_NAME)
    c = db.cursor()
    c.execute("""SELECT obsname FROM measurements""")

    all_obsnames = sorted(set([item[0].strip() for item in c]))

    for obset_name in all_obsnames:
        plot_name = os.path.join(
            MONITOR_DIR,
            obset_name.replace(
                '.fits.gz',
                '.png').replace(
                '.fits',
                '.png'))
        if os.path.exists(plot_name):
            continue
        c.execute(
     """SELECT start,ul_x,ul_y,lr_x,lr_y FROM measurements WHERE obsname='%s'""" %
            (obset_name))

        data = [line for line in c]

        times = [line[0] for line in data]
        ul_x = [line[1] for line in data]
        ul_y = [line[2] for line in data]
        lr_x = [line[3] for line in data]
        lr_y = [line[4] for line in data]

        if np.any(map(coord_deviates, [ul_x, ul_y, lr_x, lr_y])):
            print obset_name

            fig = plt.figure(figsize=(12, 10))
            fig.suptitle(obset_name)
            all_xlabels = [
                'X, Upper Left',
                'Y, Upper Left',
                'X, Lower Right',
                'Y, Lower Right']
            for i, coords in enumerate([ul_x, ul_y, lr_x, lr_y]):
                ax = fig.add_subplot(2, 2, i + 1)
                ax.plot(times, coords, 'o')
                ax.set_xlabel('TIME since EXPSTART')
                ax.set_ylabel(all_xlabels[i])
                ax.xaxis.set_major_formatter(FormatStrFormatter('%d'))
                ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))
            fig.savefig(plot_name)

#-----------------------------------------------------


def stim_monitor():
    """Main function to monitor the stim pulses in COS observations

    1: populate the database
    2: find any datasets with missing stims [send email]
    3: make plots
    4: move plots to webpage
    5: check over individual observations

    """

    #populate_db()

    missing_obs, missing_dates = find_missing()
    send_email(missing_obs, missing_dates)

    make_plots()

    move_to_web()

    check_individual()

#-----------------------------------------------------

def send_email(missing_obs, missing_dates):
    """inform the parties that retrieval and calibration are done

    """

    sorted_index = np.argsort(missing_dates)
    missing_obs = np.array(missing_obs)[sorted_index]
    missing_dates = missing_dates[sorted_index]

    date_now = datetime.date.today()
    date_now = Time('%d-%d-%d' % (date_now.year, date_now.month, date_now.day), scale='utc', format='iso')
    date_diff = (date_now - missing_dates).sec / (60. * 60. * 24)

    index = np.where(date_diff < 7)[0]
    message = '--- WARNING ---\n'
    message += 'The following observations had missing stim pulses within the past week:\n'
    message += '-' * 40 + '\n'
    message += ''.join(['{} {}\n'.format(obs, date) for obs, date in zip(missing_obs[index], missing_dates.iso[index])])
    
    message += '\n\n\n'
    message += 'The following is a complete list of observations with missing stims.\n'
    message += '-' * 40 + '\n'
    message += ''.join(['{} {}\n'.format(obs, date) for obs, date in zip(missing_obs, missing_dates.iso)])

    svr_addr = 'smtp.stsci.edu'
    from_addr = 'ely@stsci.edu'
    recipients = ['ely@stsci.edu', 'sahnow@stsci.edu']
    to_addr = ', '.join(recipients)

    msg = MIMEMultipart()
    msg['Subject'] = 'Stim Monitor Report'
    msg['From'] = from_addr
    msg['To'] = to_addr

    msg.attach(MIMEText(message))
    s = smtplib.SMTP(svr_addr)
    s.sendmail(from_addr, recipients, msg.as_string())
    s.quit()

#--------------------------------------------------------------------

def make_plots():
    """Make the overall STIM monitor plots.
    They will all be output to MONITOR_DIR.
    """

    plt.ioff()

    print '#-------------------------#'
    print '#       Plotting          #'
    print '#-------------------------#'

    db = sqlite3.connect(DB_NAME)
    c = db.cursor()
    c.execute(
    """SELECT obsname,abs_time,start,ul_x,ul_y,lr_x,lr_y FROM measurements WHERE ul_x>'0' AND ul_y>'0' AND lr_x>'0' and lr_y>'0'""")
    data = [line for line in c]

    brf = pyfits.getdata(brf_file, 1)

    # -------------------------------#
    # Plots of STIM (X,Y) positions #
    # over all time                 #
    # -------------------------------#
    plt.ioff()
    plt.figure(1, figsize=(18, 12))
    plt.grid(True)
    plt.subplot(2, 2, 1)
    x = [line[3] for line in data if '_a.fits' in line[0]]
    y = [line[4] for line in data if '_a.fits' in line[0]]
    plt.plot(x, y, 'b.', alpha=.7)
    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Segment A: Stim A (Upper Left)')
    xcenter = brf[0]['SX1']
    ycenter = brf[0]['SY1']
    xwidth = brf[0]['XWIDTH']
    ywidth = brf[0]['YWIDTH']
    xs = [
        xcenter -
        xwidth,
        xcenter +
        xwidth,
        xcenter +
        xwidth,
        xcenter -
        xwidth,
        xcenter -
        xwidth]
    ys = [
        ycenter -
        ywidth,
        ycenter -
        ywidth,
        ycenter +
        ywidth,
        ycenter +
        ywidth,
        ycenter -
        ywidth]
    plt.plot(xs, ys, color='r', linestyle='--', label='Search Box')
    plt.legend(shadow=True, numpoints=1)
    plt.xlabel('RAWX')
    plt.ylabel('RAWY')

    plt.subplot(2, 2, 2)
    x = [line[5] for line in data if '_a.fits' in line[0]]
    y = [line[6] for line in data if '_a.fits' in line[0]]
    plt.plot(x, y, 'r.', alpha=.7)
    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Segment A: Stim B (Lower Right)')
    xcenter = brf[0]['SX2']
    ycenter = brf[0]['SY2']
    xwidth = brf[0]['XWIDTH']
    ywidth = brf[0]['YWIDTH']
    xs = [
        xcenter -
        xwidth,
        xcenter +
        xwidth,
        xcenter +
        xwidth,
        xcenter -
        xwidth,
        xcenter -
        xwidth]
    ys = [
        ycenter -
        ywidth,
        ycenter -
        ywidth,
        ycenter +
        ywidth,
        ycenter +
        ywidth,
        ycenter -
        ywidth]
    plt.plot(xs, ys, color='r', linestyle='--', label='Search Box')
    plt.legend(shadow=True, numpoints=1)
    plt.xlabel('RAWX')
    plt.ylabel('RAWY')

    plt.subplot(2, 2, 3)
    x = [line[3] for line in data if '_b.fits' in line[0]]
    y = [line[4] for line in data if '_b.fits' in line[0]]
    plt.plot(x, y, 'b.', alpha=.7)
    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Segment B: Stim A (Upper Left)')
    xcenter = brf[1]['SX1']
    ycenter = brf[1]['SY1']
    xwidth = brf[1]['XWIDTH']
    ywidth = brf[1]['YWIDTH']
    xs = [
        xcenter -
        xwidth,
        xcenter +
        xwidth,
        xcenter +
        xwidth,
        xcenter -
        xwidth,
        xcenter -
        xwidth]
    ys = [
        ycenter -
        ywidth,
        ycenter -
        ywidth,
        ycenter +
        ywidth,
        ycenter +
        ywidth,
        ycenter -
        ywidth]
    plt.plot(xs, ys, color='r', linestyle='--', label='Search Box')
    plt.legend(shadow=True, numpoints=1)
    plt.xlabel('RAWX')
    plt.ylabel('RAWY')

    plt.subplot(2, 2, 4)
    x = [line[5] for line in data if '_b.fits' in line[0]]
    y = [line[6] for line in data if '_b.fits' in line[0]]
    plt.plot(x, y, 'r.', alpha=.7)
    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Segment B: Stim B (Lower Right)')
    xcenter = brf[1]['SX2']
    ycenter = brf[1]['SY2']
    xwidth = brf[1]['XWIDTH']
    ywidth = brf[1]['YWIDTH']
    xs = [
        xcenter -
        xwidth,
        xcenter +
        xwidth,
        xcenter +
        xwidth,
        xcenter -
        xwidth,
        xcenter -
        xwidth]
    ys = [
        ycenter -
        ywidth,
        ycenter -
        ywidth,
        ycenter +
        ywidth,
        ycenter +
        ywidth,
        ycenter -
        ywidth]
    plt.plot(xs, ys, color='r', linestyle='--', label='Search Box')
    plt.legend(shadow=True, numpoints=1)
    plt.xlabel('RAWX')
    plt.ylabel('RAWY')

    plt.draw()
    plt.savefig(os.path.join(MONITOR_DIR, 'STIM_locations.png'))
    plt.close(1)

    # -----------------------#
    # stim location plots  #
    # versus time          #
    # -----------------------#
    for segment in ['FUVA', 'FUVB']:
        fig = plt.figure(2, figsize=(18, 12))
        fig.suptitle('%s coordinate locations with time' % (segment))
        if segment == 'FUVA':
            selection = '_a.fits'
        elif segment == 'FUVB':
            selection = '_b.fits'
        for i, title in zip([1, 2, 3, 4], ['Upper Left, X', 'Upper Left, Y', 'Lower Right, X', 'Lower Right, Y']):
            ax = fig.add_subplot(2, 2, i)
            ax.set_title(title)
            ax.set_xlabel('MJD')
            ax.set_ylabel('Coordinate')
            coords = [line[i + 2] for line in data if selection in line[0]]
            times = [line[1] for line in data if selection in line[0]]
            ax.plot(times, coords, 'o')
        fig.savefig(
            os.path.join(MONITOR_DIR, 'STIM_locations_vs_time_%s.png' %
                         (segment)))
        plt.close(fig)

    # ------------------------#
    # strech and midpoint   #
    # ------------------------#
    for segment in ['FUVA', 'FUVB']:
        fig = plt.figure(figsize=(18, 12))
        fig.suptitle("Strech and Midpoint vs time")
        if segment == 'FUVA':
            selection = '_a.fits'
        elif segment == 'FUVB':
            selection = '_b.fits'

        ax1 = fig.add_subplot(2, 2, 1)
        stretch = np.array([line[5 ] for line in data if selection in line[0] ]) - \
            np.array([line[3]
                      for line in data if selection in line[0]])
        times = [line[1] for line in data if selection in line[0]]
        ax1.plot(times, stretch, 'o')
        ax1.set_xlabel('MJD')
        ax1.set_ylabel('Stretch')

        ax2 = fig.add_subplot(2, 2, 2)
        midpoint = .5 * (np.array([line[5] for line in data if selection in line[0]])
                         + np.array([line[3] for line in data if selection in line[0]]))
        ax2.plot(times, midpoint, 'o')
        ax2.set_xlabel('MJD')
        ax2.set_ylabel('Midpoint')

        ax3 = fig.add_subplot(2, 2, 3)
        stretch = np.array([line[6 ] for line in data if selection in line[0] ]) - \
            np.array([line[4]
                      for line in data if selection in line[0]])
        ax3.plot(times, stretch, 'o')
        ax3.set_xlabel('MJD')
        ax3.set_ylabel('Stretch')

        ax4 = fig.add_subplot(2, 2, 4)
        midpoint = .5 * (np.array([line[6] for line in data if selection in line[0]])
                         + np.array([line[4] for line in data if selection in line[0]]))
        ax4.plot(times, midpoint, 'o')
        ax4.set_xlabel('MJD')
        ax4.set_ylabel('Midpoint')

        fig.savefig(
            os.path.join(MONITOR_DIR, 'STIM_stretch_vs_time_%s.png' %
                         (segment)))
        plt.close(fig)

#-----------------------------------------------------

def move_to_web():
    """Simple function to move created plots in the MONITOR_DIR
    to the WEB_DIR.  Will move all files that match the string
    STIM*.p* and then change permissions to 777.
    """

    print 'Moving plots to web'
    for item in glob.glob(os.path.join(MONITOR_DIR, 'STIM*.p*')):
        shutil.copy(item, WEB_DIR)
        orig_path, filename = os.path.split(item)
        os.chmod(os.path.join(WEB_DIR, filename), 0o777)

#-----------------------------------------------------

if __name__ == '__main__':
    os.system('clear')
    stim_monitor()
