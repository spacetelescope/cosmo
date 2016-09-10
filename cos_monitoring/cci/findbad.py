from __future__ import absolute_import, print_function

"""
"""

__author__ = 'Justin Ely'
__maintainer__ = 'Justin Ely'
__email__ = 'ely@stsci.edu'
__status__ = 'Active'

import os
import glob

from astropy.io import fits
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import multiprocessing as mp
from sqlalchemy.engine import create_engine
from sqlalchemy import and_, distinct
import scipy
from scipy.optimize import leastsq, newton, curve_fit

from .constants import Y_BINNING, X_BINNING, MONITOR_DIR
from ..database.db_tables import open_settings, load_connection
from ..database.db_tables import Flagged, GainTrends, Gain

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
    session = Session()

    #-- Clean out previous results from flagged table
    session.query(Flagged).delete()

    #-- Clean out previous results from gain trends table
    session.query(GainTrends).delete()

    #-- Force commit.
    session.commit()
    session.close()
    engine.dispose()


    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])
    session = Session()

    pool = mp.Pool(processes=10)
    print("Finding bad pixels for all HV/Segments.")
    print("This will take a while, so go make yourself some tea or something.")
    all_combos = [(row.segment, row.dethv) for row in session.query(Gain).filter(and_(Gain.segment!='None',
                                                                                      Gain.dethv!='None'))]


    #-- Distinct call above not working apparently.
    all_combos = list(set(all_combos))

    print(all_combos)
    pool.map(find_flagged, all_combos)

    print("Measuring gain degredation slopes.")
    print("this doesn't take quite so much time...i think.")
    pool.map(measure_slopes, all_combos)

    #-- write projection files
    for (segment, dethv) in all_combos:
        print("Outputing projection files for {} {}".format(segment, dethv))
        results = session.query(GainTrends).filter(and_(GainTrends.segment==segment,
                                                         GainTrends.dethv==dethv))

        slope_image = np.zeros((1024, 16384))
        intercept_image = np.zeros((1024, 16384))
        bad_image = np.zeros((1024, 16384))

        for row in results:
            print(row)
            y = row.y
            x = row.x
            slope_image[y:y+Y_BINNING, x:x+X_BINNING] = row.slope
            intercept_image[y:y+Y_BINNING, x:x+X_BINNING] = row.intercept

            bad_image[y:y+Y_BINNING, x:x+X_BINNING] = row.mjd

        if slope_image.any():
            write_projection(slope_image, intercept_image, bad_image, segment, dethv)

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def find_flagged(args):
    segment, hvlevel = args

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    session = Session()

    print("{}, {}: Searching for pixels below 3.".format(segment, hvlevel))
    all_coords = [(row.x, row.y) for row in session.query(Gain).distinct(Gain.x, Gain.y).filter(and_(Gain.segment==segment,
                                                                                                     Gain.dethv==hvlevel,
                                                                                                     Gain.gain<=3,
                                                                                                     Gain.counts>=30))]
    #-- again, Distinct not working or not being used correctly
    all_coords = list(set(all_coords))
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
            results = session.query(Gain).filter(and_(Gain.segment==segment,
                                                      Gain.dethv==hvlevel,
                                                      Gain.x==x,
                                                      Gain.y==y,
                                                      Gain.expstart>55197))

            all_gain = []
            all_counts = []
            all_std = []
            all_expstart = []
            for row in results:
                all_gain.append(row.gain)
                all_counts.append(row.counts)
                all_std.append(row.std)
                all_expstart.append(row.expstart)

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
                session.add(Flagged(mjd=MJD_bad,
                                    segment=segment,
                                    dethv=hvlevel,
                                    x=x,
                                    y=y))

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def measure_slopes(args):
    segment, hvlevel = args

    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    session = Session()


    print("{}, {}: Measuring gain degredation slopes.".format(segment, hvlevel))

    all_coords = [(row.x, row.y) for row in session.query(Gain).distinct(Gain.x, Gain.y).filter(and_(Gain.segment==segment,
                                                                                                     Gain.dethv==hvlevel))]

    #-- once more, distinct error
    all_coords = list(set(all_coords))
    for x, y in all_coords:

        #--filter above and below possible spectral locations
        if (y > 600//Y_BINNING) or (y < 400//Y_BINNING):
            continue

        #-- Nothing bad before 2010,
        #-- and there are some weird gainmaps back there
        #-- filtering out for now.
        results = session.query(Gain).filter(and_(Gain.segment==segment,
                                                  Gain.dethv==hvlevel,
                                                  Gain.x==x,
                                                  Gain.y==y,
                                                  Gain.gain>0,
                                                  Gain.expstart>55197))

        all_gain = []
        all_counts = []
        all_expstart = []
        for row in results:
            all_gain.append(row.gain)
            all_counts.append(row.counts)
            all_expstart.append(row.expstart)

        all_gain = np.array(all_gain)
        all_expstart = np.array(all_expstart)

        if not len(all_gain) > 5:
            continue

        sorted_index = all_gain.argsort()
        all_gain = all_gain[sorted_index]
        all_expstart = all_expstart[sorted_index]


        fit, parameters, success = time_fitting(all_expstart, all_gain)

        if success:
            intercept = parameters[1]
            slope = parameters[0]

            f = lambda x, a, b: a * x + b - 3
            fprime = lambda x, a, b: a

            try:
                date_bad = newton(f, all_gain[-1], fprime, args=tuple(parameters), tol=1e-5, maxiter=1000)
            except RuntimeError:
                date_bad = 0


            print(date_bad, segment, hvlevel, x, y, slope, intercept)
            session.add(GainTrends(mjd=round(date_bad, 5),
                                   segment=segment,
                                   dethv=hvlevel,
                                   x=x,
                                   y=y,
                                   slope=round(slope, 5),
                                   intercept=round(intercept, 5)))

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def check_rapid_changes(x_values, y_values):
    """Check for rapid changes in gain values.

    Jumps of 5 PHA values within 28 days (~1 month) are considered severe and
    their MJD values will be returned.

    Parameters
    ----------
    x_values : np.ndarray
        MJD values of gain measurements
    y_values : np.ndarray
        gain measurements

    Returns
    -------
    jumps : dates of significant jumps

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

def write_projection(slope_image, intercept_image, bad_image, segment, dethv):
    """Writs a fits file with information useful for post-monitoring analysis.

    Parameters
    ----------
    slope_image : np.ndarray
        2D image of linear gain degredation slopes
    intercept_image : np.ndarray
        2D image of intercepts for the linear gain degredations
    bad_image : np.ndarray
        2D image of the extrapolated date where the gain will drop below 3
    segment : str
        'FUVA' or 'FUVB', COS detector segment of the measurements
    dethv : int
        Detector high-voltage setting of the measurements

    Returns
    -------
        None

    Outputs
    -------
        FITS file with the saved array data.
    """

    print('Writing projection file')
    hdu_out = fits.HDUList(fits.PrimaryHDU())
    hdu_out[0].header.update('TELESCOP', 'HST')
    hdu_out[0].header.update('INSTRUME', 'COS')
    hdu_out[0].header.update('DETECTOR', 'FUV')
    hdu_out[0].header.update('OPT_ELEM', 'ANY')
    hdu_out[0].header.update('FILETYPE', 'PROJ_BAD')
    hdu_out[0].header.update('DETHV', dethv)

    hdu_out[0].header.update('SEGMENT', segment)

    #---Ext 1
    hdu_out.append(fits.ImageHDU(data=bad_image))
    hdu_out[1].header.update('EXTNAME', 'PROJBAD')

    #---Ext 2
    hdu_out.append(fits.ImageHDU(data=slope_image))
    hdu_out[2].header.update('EXTNAME', 'SLOPE')

    #---Ext 3
    hdu_out.append(fits.ImageHDU(data=intercept_image))
    hdu_out[3].header.update('EXTNAME', 'INTERCEPT')

    #---Writeout
    hdu_out.writeto(MONITOR_DIR+'proj_bad_{}_{}.fits'.format(segment, dethv),clobber=True)
    hdu_out.close()

#-------------------------------------------------------------------------------

def time_fitting(x_fit, y_fit):
    """Fit a linear relation to the x_fit and y_fit parameters

    Parameters
    ----------
    x_fit : np.ndarray
        x-values to fit
    y_fit : np.ndarray
        y-values to fit

    Returns
    -------
    fit, parameters, success : tuple
        fit to the values, fit parameters, boolean-success
    """

    x_fit = np.array(x_fit)
    y_fit = np.array(y_fit)

    ###First fit iteration and remove outliers
    POLY_FIT_ORDER = 1

    slope, intercept = scipy.polyfit(x_fit, y_fit, POLY_FIT_ORDER)
    fit = scipy.polyval((slope, intercept), x_fit)
    fit_sigma = fit.std()
    include_index = np.where(np.abs(fit-y_fit) < 1.5*fit_sigma)[0]

    if len(include_index) < 4:
        return None, None, False

    x_fit_clipped = x_fit[include_index]
    y_fit_clipped = y_fit[include_index]

    parameters = scipy.polyfit(x_fit_clipped, y_fit_clipped, POLY_FIT_ORDER)
    fit = scipy.polyval(parameters, x_fit)

    return fit, parameters, True

#-------------------------------------------------------------------------------
