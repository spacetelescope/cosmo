""" Make darkrate plots

"""

from __future__ import absolute_import, division

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
import scipy
from scipy.ndimage.filters import convolve
import numpy as np
import math
import os

from ..utils import remove_if_there

#-------------------------------------------------------------------------------

def magnitude(x):
    """Calculate the order of magnitude of the value x

    Parameters
    ----------
    x : int,float
        number from which to find the order

    Returns
    -------
    order : int
        order of magnitude of the input value

    """

    return int(math.floor(math.log10(x)))

#-------------------------------------------------------------------------------

def plot_histogram(dark, outname):
    """Plot a linear and logarithmic histogram of the dark rates.

    These plots are used in determining the dark-rate values to include
    in the Exposure Time Calculator and Instrument Handbooks for COS.

    Parameters
    ----------
    dark : np.ndarray
        array of dark rates in counts/s
    outname : str
        name of the output plot

    """
    remove_if_there(outname)
    fig = plt.figure(figsize=(12, 9))

    bin_size = 1e-7
    n_bins = int((dark.max()-dark.min())/bin_size)

    print(n_bins)
    ax = fig.add_subplot(2, 1, 1)
    ax.hist(dark, bins=n_bins, align='mid', histtype='stepfilled')
    counts, bins = np.histogram(dark, bins=100)
    cuml_dist = np.cumsum(counts)
    count_99 = abs(cuml_dist / float(cuml_dist.max()) - .99).argmin()
    count_95 = abs(cuml_dist / float(cuml_dist.max()) - .95).argmin()

    mean = dark.mean()
    med = np.median(dark)
    std = dark.std()
    mean_obj = ax.axvline(x=mean, lw=2, ls='--', color='r', label='Mean ')
    med_obj = ax.axvline(x=med, lw=2, ls='-', color='r', label='Median')
    two_sig = ax.axvline(x=med + (2*std), lw=2, ls='-', color='gold')
    three_sig = ax.axvline(x=med + (3*std), lw=2, ls='-', color='DarkOrange')
    dist_95 = ax.axvline(x=bins[count_95], lw=2, ls='-', color='LightGreen')
    dist_99 = ax.axvline(x=bins[count_99], lw=2, ls='-', color='DarkGreen')

    ax.grid(True, which='both')
    ax.set_title('Histogram of Dark Rates')
    ax.set_ylabel('Frequency')
    ax.set_xlabel('Counts/pix/sec')
    ax.set_xlim(dark.min(), dark.max())
    ax.xaxis.set_major_formatter(FormatStrFormatter('%3.2e'))

    #--- Logarithmic

    ax = fig.add_subplot(2, 1, 2)
    #log_bins = np.logspace(np.log10(dark.min()), np.log10(dark.max()), 100)
    ax.hist(dark, bins=n_bins, align='mid', log=True, histtype='stepfilled')
    ax.axvline(x=mean, lw=2, ls='--', color='r', label='Mean')
    ax.axvline(x=med, lw=2, ls='-', color='r', label='Median')
    ax.axvline(x=med+(2*std), lw=2, ls='-', color='gold')
    ax.axvline(x=med+(3*std), lw=2, ls='-', color='DarkOrange')
    ax.axvline(x=bins[count_95], lw=2, ls='-', color='LightGreen')
    ax.axvline(x=bins[count_99], lw=2, ls='-', color='DarkGreen')

    #ax.set_xscale('log')
    ax.grid(True, which='both')
    ax.set_ylabel('Log Frequency')
    ax.set_xlabel('Counts/pix/sec')
    ax.set_xlim(dark.min(), dark.max())
    ax.xaxis.set_major_formatter(FormatStrFormatter('%3.2e'))

    fig.legend([med_obj, mean_obj, two_sig, three_sig, dist_95, dist_99],
               ['Median',
                'Mean',
                r'2$\sigma$: {0:.2e}'.format(med+(2*std)),
                r'3$\sigma$: {0:.2e}'.format(med+(3*std)),
                r'95$\%$: {0:.2e}'.format(bins[count_95]),
                r'99$\%$: {0:.2e}'.format(bins[count_99])],
               shadow=True,
               numpoints=1,
               bbox_to_anchor=[0.8, 0.8])

    remove_if_there(outname)
    fig.savefig(outname, bbox_inches='tight')
    plt.close(fig)

#-------------------------------------------------------------------------------

def plot_time(detector, dark, date, temp, solar, solar_date, outname):
    """Plot the dar-rate vs time

    Parameters
    ----------
    detector : str
        FUV or NUV
    dark : np.ndarray
        array of measured dark rates in counts/s
    date : np.ndarray
        array of measured times
    temp : np.ndarray
        array of temperatures
    solar : np.ndarray
        array of solar flux values
    solar_date : np.ndarray
        array of solar dates
    outname : str
        name of output plot

    """
    remove_if_there(outname)
    fig = plt.figure(figsize=(20, 12))

    sorted_index = np.argsort(solar_date)
    solar = solar[sorted_index]
    solar_date = solar_date[sorted_index]

    if detector == 'FUV':
        dark_ax = fig.add_axes([.1, .3, .8, .6])
        sub_ax = fig.add_axes([.1, .09, .8, .19])
    else:
        dark_ax = fig.add_axes([.1, .5, .8, .4])
        sub_ax = fig.add_axes([.1, .1, .8, .2])
        sub_ax2 = fig.add_axes([.1, .3, .8, .2])


        temp_index = np.where(temp > 15)[0]
        dark = dark[temp_index]
        date = date[temp_index]
        temp = temp[temp_index]

    dark_ax.plot(date,
                 dark,
                 color='k',
                 marker='o',
                 linestyle='',
                 markersize=2,
                 label='Dark Count Rate',
                 zorder=1,
                 rasterized=True)

    #dark_ax.axvline(x=2012.326, ymin=0, ymax=1, color='b', linestyle='-',
    #                lw=2, label='COS Suspend', zorder=1, alpha=.4)
    #dark_ax.axvline(x=2012.980, ymin=0, ymax=1, color='b', linestyle='--',
    #                lw=2, label='Dec Safe', zorder=1, alpha=.4)
    #dark_ax.axvline(x=2013.126, ymin=0, ymax=1, color='b', linestyle=':',
    #                lw=2, label='Feb Safe', zorder=1, alpha=.4)

    if not detector == 'NUV':
        dark_ax.axhline(y=1.5E-6,
                        color='r',
                        linestyle='--',
                        lw=3,
                        label='1.5e-6',
                        zorder=1,
                        alpha=.6)

    dark_ax.xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
    dark_ax.yaxis.set_major_formatter(FormatStrFormatter('%3.2e'))

    dark_ax.yaxis.set_ticks(np.arange(0, dark.max(),
                                      2 * 10 ** magnitude(dark.mean())))

    dark_ax.set_xticklabels(['' for item in dark_ax.get_xticklabels()])
    dark_ax.set_ylabel('Mean Dark Rate cnts/sec/pix')

    if 'FUVA' in outname:
        segment = 'FUVA'
    elif 'FUVB' in outname:
        segment = 'FUVB'
    else:
        segment = 'NUV'
    dark_ax.set_title('Global Dark Rate: %s' % (segment.upper()))
    dark_ax.set_xlim(2009.5, date.max() + .1)
    dark_ax.legend(numpoints=1, shadow=True, loc='upper left')
    dark_ax.grid(True)

    if detector == 'NUV':
        sub_ax.plot(date,
                    temp,
                    color='r',
                    linestyle='',
                    markersize=8,
                    marker='o')

        sub_ax.set_xlabel('Decimal Year')
        sub_ax.set_ylabel('Temperature')
        sub_ax.xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        sub_ax.set_xlim(2009.5, date.max() + .1)
        #sub_ax.set_ylim(15, 27)
        sub_ax.grid(True)

        solar_smooth = scipy.convolve(solar, np.ones(81) / 81.0, mode='same')
        sub_ax2.plot(solar_date,
                     solar,
                     color='orange',
                     marker='',
                     linestyle='-',
                     label='10.7cm',
                     lw=1,
                     alpha=.9,
                     zorder=1)
        sub_ax2.plot(solar_date[:-41],
                     solar_smooth[:-41],
                     color='red',
                     marker='',
                     linestyle='-',
                     label='10.7cm Smoothed',
                     lw=3,
                     alpha=1,
                     zorder=1)

        sub_ax2.set_xticklabels(['' for item in dark_ax.get_xticklabels()])
        sub_ax2.set_ylabel('Radio Flux')
        sub_ax2.set_ylim(50, 210)
        sub_ax2.set_xlim(2009.5, date.max() + .1)
        sub_ax2.legend(numpoints=1, shadow=True, loc='best')
        sub_ax2.grid(True)

    else:
        solar_smooth = scipy.convolve(solar, np.ones(81) / 81.0, mode='same')
        sub_ax.plot(solar_date,
                    solar,
                    color='orange',
                    marker='',
                    linestyle='-',
                    label='10.7cm',
                    lw=1,
                    alpha=.9,
                    zorder=1)
        sub_ax.plot(solar_date[:-41],
                    solar_smooth[:-41],
                    color='red',
                    marker='',
                    linestyle='-',
                    label='10.7cm Smoothed',
                    lw=3,
                    alpha=1,
                    zorder=1)
        plt.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        sub_ax.set_xlabel('Decimal_year')
        sub_ax.set_ylabel('Radio Flux')
        sub_ax.set_ylim(50, 210)
        sub_ax.set_xlim(2009.5, date.max() + .1)
        sub_ax.legend(numpoints=1, shadow=True, loc='best')
        sub_ax.grid(True)

    remove_if_there(outname)
    fig.savefig(outname, bbox_inches='tight')
    plt.close(fig)

#-------------------------------------------------------------------------------

def plot_orbital_rate(longitude, latitude, darkrate, sun_lon, sun_lat, outname):
    """Plot the dark-rate of the detector vs orbital position

    Parameters
    ----------
    longitude : np.ndarray
        longitude of HST
    latitude : np.ndarray
        latitude of HST
    darkrate : np.ndarray
        measured dark-rate of the detector
    sun_long : np.ndarray
        longitude of the sub-solar point
    sun_lat : np.ndarray
        latitude of the sub-solar point
    outname : str
        name of the output plot

    """
    
    pretty_plot = True
    if pretty_plot:
        pl_opt = {"fontweight": "bold",
                  "titlesize": 22,
                  "labelsize": 18,
                  "legendsize": 10,
                  "tickwidth": 2,
                  "ticksize": 13,
                  "cbarticksize": 12,
                  "ticklength": 5,
                  "markersize": 12}
    else:
        pl_opt = {"fontweight": "semibold",
                  "titlesize": 15,
                  "labelsize": 15,
                  "legendsize": 8,
                  "tickwidth": 1.5,
                  "ticksize": 10,
                  "cbarticksize": 10,
                  "ticklength": 4,
                  "markersize": 10}

    color_min = darkrate.min()
    color_max = darkrate.min() + 3 * darkrate.std()

    remove_if_there(outname)
    fig, (ax,ax2,ax3) = plt.subplots(3, sharex=True, figsize=(20, 15))

    if 'FUVA' in outname:
        detector = 'FUVA'
    elif 'FUVB' in outname:
        detector = 'FUVB'
    elif 'NUV' in outname:
        detector = 'NUV'

    fig.suptitle('Orbital Variation in Darkrate for {}'.format(detector), 
                 size=pl_opt['titlesize'], fontweight=pl_opt['fontweight'], 
                 family='serif')

    colors = ax.scatter(longitude,
                        latitude,
                        c=darkrate,
                        marker='o',
                        alpha=.7,
                        edgecolors='none',
                        s=3,
                        lw=0,
                        vmin=color_min,
                        vmax=color_max,
                        rasterized=True)

    ax.set_xlim(0, 360)
    ax.set_ylim(-35, 35)
    ax.set_ylabel('Latitude', size=pl_opt['labelsize'],
                  fontweight=pl_opt['fontweight'], family='serif')
    ax.set_xlabel('Longitude', size=pl_opt['labelsize'],
                  fontweight=pl_opt['fontweight'], family='serif')

    '''
    plt.ion()
    from mpl_toolkits.mplot3d import Axes3D
    fig = plt.figure()
    ax = fig.add_subplot( 1,1,1, projection='3d')
    ax.scatter( longitude, latitude, zs=darkrate, c=darkrate,
                marker='o', alpha=.7, edgecolors='none',
                s=5, lw=0, vmin=color_min, vmax=color_max )
    raw_input()
    '''

    #-- Get rid of the SAA passages
    index_keep = np.where((longitude < 250) | (latitude > 10))[0]
    darkrate = darkrate[index_keep]
    latitude = latitude[index_keep]
    longitude = longitude[index_keep]
    sun_lat = sun_lat[index_keep]
    sun_lon = sun_lon[index_keep]

    lon_diff = longitude - sun_lon
    lat_diff = latitude - sun_lat

    index = np.where(lon_diff < 0)[0]
    lon_diff[index] += 360

    colors = ax2.scatter(lon_diff,
                         lat_diff,
                         c=darkrate,
                         marker='o',
                         alpha=.7,
                         edgecolors='none',
                         s=5,
                         lw=0,
                         vmin=color_min,
                         vmax=color_max,
                         rasterized=True)

    ax2.set_xlim(0, 360)
    ax2.set_ylabel('Lat. - Sub-Solar Pnt', size=pl_opt['labelsize'], 
                   fontweight=pl_opt['fontweight'], family='serif')
    ax2.set_xlabel('Long. - Sub-Solar Pnt', size=pl_opt['labelsize'], 
                   fontweight=pl_opt['fontweight'], family='serif')

    #-- Cut out the low-points
    dark_smooth = convolve(darkrate, np.ones(91)/91, mode='mirror')

    thresh = dark_smooth + 1.5*dark_smooth.std()
    index_keep = np.where((darkrate > thresh))[0]

    if len(index_keep) and detector in ['FUVA', 'FUVB']:
        darkrate = darkrate[index_keep]
        latitude = latitude[index_keep]
        longitude = longitude[index_keep]
        sun_lat = sun_lat[index_keep]
        sun_lon = sun_lon[index_keep]
    elif detector == 'NUV':
        pass
    else:
        raise ValueError("This needs to be NUV data at this point. Found: {}".format(detctor))

    lon_diff = longitude - sun_lon
    lat_diff = latitude - sun_lat

    index = np.where(lon_diff < 0)[0]
    lon_diff[index] += 360

    colors = ax3.scatter(lon_diff,
                         lat_diff,
                         c=darkrate,
                         marker='o',
                         alpha=.7,
                         edgecolors='none',
                         s=5,
                         lw=0,
                         vmin=color_min,
                         vmax=color_max,
                         rasterized=True)

    cax = plt.axes([0.92, 0.2, 0.02, 0.6])
    cbar = fig.colorbar(colors, cax=cax)
    for ytick in cbar.ax.yaxis.get_ticklabels():
        ytick.set_weight("bold")
    cbar.ax.tick_params(axis="y", labelsize=pl_opt["cbarticksize"])
    cbar.formatter.set_powerlimits((0,0))
    cbar.update_ticks()
#    cbar.ax.yaxis.set_major_formatter(FormatStrFormatter("%3.1e"))

    ax3.set_xlim(0, 360)
    ax3.set_ylabel('Lat. - Sub-Solar Pnt', size=pl_opt['labelsize'], 
                   fontweight=pl_opt['fontweight'], family='serif')
    ax3.set_xlabel('Long. - Sub-Solar Pnt', size=pl_opt['labelsize'], 
                   fontweight=pl_opt['fontweight'], family='serif')

    for cur_ax in [ax, ax2, ax3]:
        for xtick,ytick in zip(cur_ax.xaxis.get_ticklabels(),cur_ax.yaxis.get_ticklabels()):
            xtick.set_weight("bold")
            ytick.set_weight("bold")
        cur_ax.tick_params(axis="both", which="major", labelsize=pl_opt['ticksize'],
                           width=pl_opt['tickwidth'], length=pl_opt['ticklength'])

    remove_if_there(outname)
    fig.savefig(outname, bbox_inches='tight')
    plt.close(fig)

    '''
    gridx, gridy = np.mgrid[all_lon.min():all_lon.max():.1,
                            all_lat.min():all_lat.max():.1]
    thing = griddata(zip(all_lon, all_lat),
                     darkrate,
                     (gridx, gridy),
                     method='nearest' )
    image = medfilt(thing.T, (5,5))
    '''

#-------------------------------------------------------------------------------
