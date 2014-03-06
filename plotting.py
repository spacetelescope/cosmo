""" Make darkrate plots

"""

import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
import scipy
from scipy.ndimage.filters import convolve
import numpy as np

#-------------------------------------------------------------------------------

def plot_time(detector, dark, date, temp, solar, solar_date, outname):
    """ Make main dark-rate plots
    """

    fig = plt.figure( figsize=(20,12) )

    dark_ax = fig.add_axes([.1, .3, .8, .6])
    sub_ax = fig.add_axes([.1, .09, .8, .19])

    dark_ax.plot( date, dark, color='k', marker='o',
                  linestyle='', markersize=6, label='Dark Count Rate', zorder=1, rasterized=True)

    #dark_ax.axvline(x=2012.326, ymin=0, ymax=1, color='b', linestyle='-',
    #                lw=2, label='COS Suspend', zorder=1, alpha=.4)
    #dark_ax.axvline(x=2012.980, ymin=0, ymax=1, color='b', linestyle='--',
    #                lw=2, label='Dec Safe', zorder=1, alpha=.4)
    #dark_ax.axvline(x=2013.126, ymin=0, ymax=1, color='b', linestyle=':',
    #                lw=2, label='Feb Safe', zorder=1, alpha=.4)

    if detector != 'NUV':
        dark_ax.axhline(y=1.5E-6, color='r', linestyle='--',
                   lw=3, label='1.5e-6', zorder=1, alpha=.6)

    dark_ax.xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
    dark_ax.yaxis.set_major_formatter(FormatStrFormatter('%3.2e'))

    dark_ax.set_xticklabels(['' for item in dark_ax.get_xticklabels()])
    dark_ax.set_ylabel('Mean Dark Rate cnts/sec/pix')
    dark_ax.set_title('Global Dark Rate: %s' % (detector.upper()))
    dark_ax.set_xlim(2009.5, date.max() + .1)
    dark_ax.legend(numpoints=1, shadow=True, loc='upper left')
    dark_ax.grid(True)

    if detector == 'NUV':
        sub_ax.plot(date, temp, color='r',
                    linestyle='', markersize=8, marker='o')
        plt.gca().xaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        sub_ax.set_xlabel('Decimal_year')
        sub_ax.set_ylabel('Temperature')
        sub_ax.set_xlim(2009.5, date.max() + .1)
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
        sub_ax.set_xlim(2009.5, date.max() + .1)
        sub_ax.legend(numpoints=1, shadow=True, loc='best')
        sub_ax.grid(True)

    fig.savefig(outname, bbox_inches='tight')

#-------------------------------------------------------------------------------

def plot_orbital_rate(longitude, latitude, darkrate, sun_lon, sun_lat, outname):

    color_min = darkrate.min()
    color_max = darkrate.min() + 3*darkrate.std()
    
    fig = plt.figure( figsize=(20,15) )

    if 'FUVA' in outname:
        detector = 'FUVA'
    elif 'FUVB' in outname:
        detector = 'FUVB'
    elif 'NUV' in outname:
        detector = 'NUV'

    fig.suptitle('Orbital Variation in Darkrate for {}'.format(detector))

    ax = fig.add_subplot( 3,1,1 )
    colors = ax.scatter( longitude, latitude, c=darkrate, marker='o', alpha=.7, edgecolors='none', 
                         s=3, lw=0, vmin=color_min, vmax=color_max, rasterized=True )
    fig.colorbar( colors )
    ax.set_xlim(0, 360)
    ax.set_ylabel('Latitude')
    ax.set_xlabel('Longitude')

    
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
    ax2 = fig.add_subplot( 3,1,2 )

    #-- Get rid of the SAA passages
    index_keep = np.where( (longitude < 250) | (latitude > 10) )[0]
    darkrate = darkrate[index_keep]
    latitude = latitude[index_keep]
    longitude = longitude[index_keep]
    sun_lat = sun_lat[index_keep]
    sun_lon = sun_lon[index_keep]
    
    lon_diff = longitude - sun_lon
    lat_diff = latitude - sun_lat

    index = np.where(lon_diff < 0)[0]
    lon_diff[index] += 360

    colors = ax2.scatter( lon_diff, lat_diff, c=darkrate, 
                          marker='o', alpha=.7, edgecolors='none', 
                          s=5, lw=0, vmin=color_min, vmax=color_max, rasterized=True )
    ax2.set_xlim(0, 360)
    ax2.set_ylabel('Latitude - sub-solar point')
    ax2.set_xlabel('Longitude - sub-solar point')

    fig.colorbar( colors )



    ax3 = fig.add_subplot( 3,1,3 )
    #-- Cut out the low-points
    dark_smooth = convolve( darkrate, np.ones(91)/91, mode='mirror' )

    thresh = dark_smooth + 1.5*dark_smooth.std()
    index_keep = np.where( (darkrate > thresh) )[0]

    if len(index_keep):
        darkrate = darkrate[index_keep]
        latitude = latitude[index_keep]
        longitude = longitude[index_keep]
        sun_lat = sun_lat[index_keep]
        sun_lon = sun_lon[index_keep]
    else:
        print 'I sure hope this is NUV data'

    lon_diff = longitude - sun_lon
    lat_diff = latitude - sun_lat

    index = np.where(lon_diff < 0)[0]
    lon_diff[index] += 360

    colors = ax3.scatter( lon_diff, lat_diff, c=darkrate, 
                          marker='o', alpha=.7, edgecolors='none', 
                          s=5, lw=0, vmin=color_min, vmax=color_max, rasterized=True )
    ax3.set_xlim(0, 360)
    ax3.set_ylabel('Latitude - sub-solar point')
    ax3.set_xlabel('Longitude - sub-solar point')

    fig.colorbar( colors )

    fig.savefig( outname, bbox_inches='tight' )

    '''
    gridx, gridy = np.mgrid[all_lon.min():all_lon.max():.1, all_lat.min():all_lat.max():.1]
    thing = griddata( zip(all_lon, all_lat), darkrate, (gridx, gridy), method='nearest' )
    image = medfilt( thing.T, (5,5) )
    '''

#-------------------------------------------------------------------------------
