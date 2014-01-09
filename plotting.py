""" Make darkrate plots

"""

import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
import scipy
import numpy as np

#-------------------------------------------------------------------------------

def plot_time(detector, dark, date, temp, solar, solar_date, outname):
    """ Make main dark-rate plots
    """

    fig = plt.figure()

    dark_ax = fig.add_axes([.1, .3, .8, .6])
    sub_ax = fig.add_axes([.1, .09, .8, .19])

    dark_ax.plot( date, dark, color='k', marker='o',
                  linestyle='', markersize=8, label='Dark Count Rate', zorder=1)

    #dark_ax.axvline(x=2012.326, ymin=0, ymax=1, color='b', linestyle='-',
    #                lw=2, label='COS Suspend', zorder=1, alpha=.4)
    #dark_ax.axvline(x=2012.980, ymin=0, ymax=1, color='b', linestyle='--',
    #                lw=2, label='Dec Safe', zorder=1, alpha=.4)
    #dark_ax.axvline(x=2013.126, ymin=0, ymax=1, color='b', linestyle=':',
    #                lw=2, label='Feb Safe', zorder=1, alpha=.4)

    if detector != 'NUV':
        dark_ax.axhline(y=1.5E-6, color='r', linestyle='--',
                   lw=3, label='1.5e-6', zorder=1, alpha=.6)

    plt.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
    plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%3.2e'))

    dark_ax.set_xticklabels(['' for item in dark_ax.get_xticklabels()])
    dark_ax.set_ylabel('Mean Dark Rate cnts/sec/pix')
    dark_ax.set_title('Global Dark Rate: %s' % (detector.upper()))
    #dark_ax.set_xlim(2009.5, date_to_predict + .3)
    dark_ax.legend(numpoints=1, shadow=True, loc='upper left')
    dark_ax.grid(True)

    if detector == 'NUV':
        sub_ax.plot(date, temp, color='r',
                    linestyle='', markersize=8, marker='o')
        plt.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        sub_ax.set_xlabel('Decimal_year')
        sub_ax.set_ylabel('Temperature')
        #sub_ax.set_xlim(2009.5, date_to_predict + .3)
        sub_ax.grid(True)
    else:
        solar_smooth = scipy.convolve(solar, np.ones(81) / 81.0, mode='same')
        sub_ax.plot(solar_date, solar, color='orange', marker='',
                    linestyle='-', label='10.7cm', lw=1, alpha=.9, zorder=1)
        sub_ax.plot(solar_date[:-41], solar_smooth[:-41], color='red', marker='',
                    linestyle='-', label='10.7cm Smoothed', lw=3, alpha=1, zorder=1)
        plt.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        sub_ax.set_xlabel('Decimal_year')
        sub_ax.set_ylabel('Radio Flux')
        sub_ax.set_ylim(50, 200)
        #sub_ax.set_xlim(2009.5, date_to_predict + .3)
        sub_ax.legend(numpoints=1, shadow=True, loc='best')
        sub_ax.grid(True)

    fig.savefig(outname)

#-------------------------------------------------------------------------------

def plot_orbital_rate(longitude, latitude, darkrate, sun_lon, sun_lat, outname):
    fig = plt.figure( )
    ax = fig.add_subplot( 2,1,1 )
    colors = ax.scatter( longitude, latitude, c=darkrate, marker='o', alpha=.7, edgecolors='none', 
                         s=3, lw=0, vmin=darkrate.min(), vmax=darkrate.min() + 3*darkrate.std() )
    fig.colorbar( colors )

    ax2 = fig.add_subplot( 2,1,2 )
    lon_diff = longitude - sun_lon
    lat_diff = latitude - sun_lat
    colors = ax2.scatter( lon_diff, lat_diff, c=darkrate, 
                          marker='o', alpha=.7, edgecolors='none', 
                          s=5, lw=0, vmax=darkrate.min() + 3*darkrate.std() )

    fig.colorbar( colors )

    fig.savefig( outname )

    '''
    gridx, gridy = np.mgrid[all_lon.min():all_lon.max():.1, all_lat.min():all_lat.max():.1]
    thing = griddata( zip(all_lon, all_lat), darkrate, (gridx, gridy), method='nearest' )
    image = medfilt( thing.T, (5,5) )
    '''

#-------------------------------------------------------------------------------
