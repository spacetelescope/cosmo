from __future__ import absolute_import, print_function
"""Routine to monitor the modal gain in each pixel as a
function of time.  Uses COS Cumulative Image (CCI) files
to produce a modal gain map for each time period.  Modal gain
maps for each period are collated to monitor the progress of
each pixel(superpixel) with time.  Pixels that drop below
a threshold value are flagged and collected into a
gain sag table reference file (gsagtab).

The PHA modal gain threshold is set by global variable MODAL_GAIN_LIMIT.
Allowing the modal gain of a distribution to come within 1 gain bin
of the threshold results in ~8% loss of flux.  Within
2 gain bins, ~4%
3 gain bins, ~2%
4 gain bins, ~1%

However, due to the column summing, a 4% loss in a region does not appear to be so in the extracted spectrum.
"""

__author__ = 'Justin Ely'
__maintainer__ = 'Justin Ely'
__email__ = 'ely@stsci.edu'
__status__ = 'Active'

import os
import glob
from sqlalchemy.engine import create_engine
from ..database.db_tables import open_settings, load_connection

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import multiprocessing as mp

from .constants import Y_BINNING, X_BINNING, MONITOR_DIR

#-------------------------------------------------------------------------------

def time_trends():
    print('#----------------------#')
    print('Finding trends with time')
    print('#----------------------#')

    print('Cleaning previous products')
    for item in glob.glob(os.path.join(MONITOR_DIR, 'cumulative_gainmap_*.png')):
        os.remove(item)

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    connection = engine.connect()

    try:
        connection.execute("""CREATE TABLE flagged (mjd real,
                                                    segment text,
                                                    dethv int,
                                                    x int,
                                                    y int);""")
        connection.execute("""CREATE INDEX position ON flagged (x,y);""")
    except:
        connection.execute("""DROP TABLE flagged;""")
        connection.execute("""CREATE TABLE flagged (mjd real,
                                                    segment text,
                                                    dethv int,
                                                    x int,
                                                    y int);""")
        connection.execute("""CREATE INDEX position ON flagged (x,y);""")

    result = connection.execute("""SELECT DISTINCT segment,dethv FROM gain WHERE segment!= 'None' and dethv!= 'None'""")

    connection.close()

    all_combos = [(row['segment'], row['dethv']) for row in result]
    print(all_combos)
    pool = mp.Pool(processes=10)
    pool.map(find_flagged, all_combos)

#-------------------------------------------------------------------------------

def find_flagged(args):
    segment, hvlevel = args

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    connection = engine.connect()

    print("{}, {}: Searching for pixels below 3.".format(segment, hvlevel))
    results = connection.execute("""SELECT DISTINCT x,y
                                           FROM gain
                                           WHERE segment='%s'
                                                 AND dethv='%s'
                                                 AND gain<=3
                                                 AND counts>=30;""" % (segment,
                                                                       hvlevel))

    #-- refine to active area
    all_coords = [(row['x'], row['y']) for row in results]
    print("{}, {}: found {} superpixels below 3.".format(segment,
                                                         hvlevel,
                                                         len(all_coords)))

    plotfile = os.path.join(MONITOR_DIR, 'flagged_{}_{}.pdf'.format(segment,
                                                                    hvlevel))
    print("Plotting to {}:".format(plotfile))
    #with PdfPages(plotfile) as pdf:
    with open('blank', 'w') as pdf:
        for x, y in all_coords:

            #--filter above and below possible spectral locations
            if (y > 600//Y_BINNING) or (y < 400//Y_BINNING):
                continue

            #-- Nothing bad before 2010,
            #-- and there are some weird gainmaps back there
            #-- filtering out for now.
            results = connection.execute("""SELECT gain,counts,std,expstart
                                                FROM gain
                                                    WHERE segment='%s'
                                                        AND dethv='%s'
                                                        AND x='%s'
                                                        AND y='%s'
                                                        AND expstart>55197;""" % (segment, hvlevel, x, y))

            all_gain = []
            all_counts = []
            all_std = []
            all_expstart = []
            for row in results:
                all_gain.append(row['gain'])
                all_counts.append(row['counts'])
                all_std.append(row['std'])
                all_expstart.append(row['expstart'])

            all_gain = np.array(all_gain)
            all_expstart = np.array(all_expstart)

            below_thresh = np.where(all_gain <= 3)[0]

            if len(below_thresh):
                MJD_bad = all_expstart[below_thresh].min()


                '''
            	fig = plt.figure(figsize=(16, 6))
            	ax = fig.add_subplot(2, 1, 1)
            	ax.set_title('{} {}'.format(x, y))

            	ax.plot(all_expstart, all_gain, marker='o', color='b', ls='')
            	ax.axhline(y=3, color='r', lw=2, ls='--', alpha=.5, zorder=0)
            	ax.axvline(x=MJD_bad, color='r', lw=2, alpha=.5, zorder=0)

            	ax2 = fig.add_subplot(2, 1, 2)
            	ax2.plot(all_expstart, all_counts, marker='o', ls='')

            	fig.set_rasterized(True)
            	#fig.savefig(os.path.join(MONITOR_DIR, 'flagged_{}_{}_{}_{}.pdf'.format(segment, hvlevel, x, y)),
                #	        bbox_inches='tight',
                #        	dpi=300)

            	pdf.savefig(fig, bbox_inches='tight')
                plt.close(fig)
                '''
                MJD_bad = round(MJD_bad, 5)
                print("inserting {} {} {} {} {}".format(segment,
                                                        hvlevel,
                                                        x*X_BINNING,
                                                        y*Y_BINNING,
                                                        MJD_bad))
                connection.execute("""INSERT INTO flagged VALUES ('{}','{}','{}','{}','{}');""".format(MJD_bad,
                                                                                                       segment,
                                                                                                       hvlevel,
                                                                                                       x,
                                                                                                       y))

#-------------------------------------------------------------------------------

def check_rapid_changes(x_values, y_values):
    """Check for rapid changes in the values.

    An email will be sent if any rapid dip or jump is seen.
    """
    gain_thresh = 5
    mjd_thresh = 28
    #Set initial values at ridiculous numbers
    previous_gain = y_values[0]
    previous_mjd = x_values[0]

    jumps = []
    for gain, mjd in zip(y_values, x_values):
        gain_diff = np.abs(previous_gain - gain)
        mjd_diff = np.abs(previous_mjd - mjd)
        if (gain_diff > gain_thresh) and (mjd_diff < mjd_thresh):
            jumps.append(mjd)

        previous_gain = gain
        previous_mjd = mjd

    return jumps

#-------------------------------------------------------------------------------
