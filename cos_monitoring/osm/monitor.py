from __future__ import absolute_import

""" Script to compile the spectrum shift data for COS FUV and NUV data.

"""

import glob
import os
import shutil
import sys

import numpy as np
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt

import scipy
from scipy.stats import linregress
from datetime import datetime

from astropy.io import fits
from astropy.table import Table

from ..database.db_tables import open_settings, load_connection

MONITOR_DIR = '/grp/hst/cos/Monitors/Shifts/'
WEB_DIR = '/grp/webpages/COS/shifts/'
lref = '/grp/hst/cdbs/lref/'

#-------------------------------------------------------------------------------

def fppos_shift(lamptab_name, segment, opt_elem, cenwave, fpoffset):
    lamptab = fits.getdata(os.path.join(lref, lamptab_name))

    if 'FPOFFSET' not in lamptab.names:
        return 0

    index = np.where((lamptab['segment'] == segment) &
                     (lamptab['opt_elem'] == opt_elem) &
                     (lamptab['cenwave'] == cenwave) &
                     (lamptab['fpoffset'] == fpoffset))[0]

    offset = lamptab['FP_PIXEL_SHIFT'][index][0]

    return offset

#-------------------------------------------------------------------------------

def pull_flashes(filename):
    """Calculate lampflash values for given file

    Parameters
    ----------
    filename : str
        file to calculate lamp shifts from

    Returns
    -------
    out_info : dict
        dictionary of pertinent value

    """

    with fits.open(filename) as hdu:
        out_info = {'date': hdu[1].header['EXPSTART'],
                    'rootname': hdu[0].header['ROOTNAME'],
                    'proposid': hdu[0].header['PROPOSID'],
                    'detector': hdu[0].header['DETECTOR'],
                    'opt_elem': hdu[0].header['OPT_ELEM'],
                    'cenwave': hdu[0].header['CENWAVE'],
                    'fppos': hdu[0].header.get('FPPOS', None)}

        if '_lampflash.fits' in filename:
            out_info['lamptab'] = hdu[0].header['LAMPTAB'].split('$')[-1]

            fpoffset = out_info['fppos'] - 3
            
            if not len(hdu[1].data):
                yield out_info

            for i, line in enumerate(hdu[1].data):
                out_info['flash'] = (i // 2) + 1
                out_info['x_shift'] = line['SHIFT_DISP'] - fppos_shift(out_info['lamptab'],
                                                                       line['segment'],
                                                                       out_info['opt_elem'],
                                                                       out_info['cenwave'],
                                                                       fpoffset)

                out_info['y_shift'] = line['SHIFT_XDISP']
                out_info['found'] = line['SPEC_FOUND']
                out_info['segment'] = line['SEGMENT']

                #-- don't need too much precision here
                out_info['x_shift'] = round(out_info['x_shift'], 5)
                out_info['y_shift'] = round(out_info['y_shift'], 5)

                yield out_info


        elif '_rawacq.fits' in filename:
            #-- Technically it wasn't found.
            out_info['found'] = False
            out_info['fppos'] = -1
            out_info['flash'] = 1
            out_info['segment'] = 'N/A'

            spt = fits.open(filename.replace('rawacq', 'spt'))

            if not spt[1].header['LQTAYCOR'] > 0:
                out_info['x_shift'] = None
                out_info['y_shift'] = None
            else:
                # These are in COS RAW coordinates, so shifted 90 degrees from
                # user and backwards
                out_info['x_shift'] = 1023 - spt[1].header['LQTAYCOR']
                out_info['y_shift'] = 1023 - spt[1].header['LQTAXCOR']

            yield out_info

        else:
            yield out_info

#-------------------------------------------------------------------------------

def fit_data(xdata, ydata):
    stats = linregress(xdata, ydata)

    parameters = (stats[0], stats[1])
    err = 0
    fit = scipy.polyval(parameters, xdata)

    return fit, xdata, parameters, err

#-------------------------------------------------------------------------------

def make_shift_table():
    SETTINGS = open_settings()
    Session, engine = load_connection(SETTINGS['connection_string'])

    #-- this is a crude implementation, but it lets me use the rest of the
    #-- plotting code as-is
    data = []
    for i, row in enumerate(engine.execute("""SELECT * FROM lampflash
                                                       WHERE x_shift IS NOT NULL AND
                                                             y_shift IS NOT NULL;""")):
        if not i:
            keys = row.keys()
        data.append(row.values())

    data = Table(rows=data, names=keys)

    return data

#-------------------------------------------------------------------------------

def make_plots(data):
    print('Plotting')

    mpl.rcParams['figure.subplot.hspace'] = 0.05

    sorted_index = np.argsort(data['date'])
    data = data[sorted_index]

    G140L = np.where((data['opt_elem'] == 'G140L'))[0]
    G140L_A = np.where((data['opt_elem'] == 'G140L') &
                       (data['segment'] == 'FUVA'))[0]
    G140L_B = np.where((data['opt_elem'] == 'G140L') &
                       (data['segment'] == 'FUVB'))[0]

    G130M = np.where((data['opt_elem'] == 'G130M'))[0]
    G130M_A = np.where((data['opt_elem'] == 'G130M') &
                       (data['segment'] == 'FUVA'))[0]
    G130M_B = np.where((data['opt_elem'] == 'G130M') &
                       (data['segment'] == 'FUVB'))[0]

    G160M = np.where((data['opt_elem'] == 'G160M'))[0]
    G160M_A = np.where((data['opt_elem'] == 'G160M') &
                       (data['segment'] == 'FUVA'))[0]
    G160M_B = np.where((data['opt_elem'] == 'G160M') &
                       (data['segment'] == 'FUVB'))[0]

    G230L = np.where((data['opt_elem'] == 'G230L'))[0]
    G230L_A = np.where((data['opt_elem'] == 'G230L') &
                       (data['segment'] == 'NUVA'))[0]
    G230L_B = np.where((data['opt_elem'] == 'G230L') &
                       (data['segment'] == 'NUVB'))[0]
    G230L_C = np.where((data['opt_elem'] == 'G230L') &
                       (data['segment'] == 'NUVC'))[0]

    G225M = np.where((data['opt_elem'] == 'G225M'))[0]
    G225M_A = np.where((data['opt_elem'] == 'G225M') &
                       (data['segment'] == 'NUVA'))[0]
    G225M_B = np.where((data['opt_elem'] == 'G225M') &
                       (data['segment'] == 'NUVB'))[0]
    G225M_C = np.where((data['opt_elem'] == 'G225M') &
                       (data['segment'] == 'NUVC'))[0]

    G285M = np.where((data['opt_elem'] == 'G285M'))[0]
    G285M_A = np.where((data['opt_elem'] == 'G285M') &
                       (data['segment'] == 'NUVA'))[0]
    G285M_B = np.where((data['opt_elem'] == 'G285M') &
                       (data['segment'] == 'NUVB'))[0]
    G285M_C = np.where((data['opt_elem'] == 'G285M') &
                       (data['segment'] == 'NUVC'))[0]

    G185M = np.where((data['opt_elem'] == 'G185M'))[0]
    G185M_A = np.where((data['opt_elem'] == 'G185M') &
                       (data['segment'] == 'NUVA'))[0]
    G185M_B = np.where((data['opt_elem'] == 'G185M') &
                       (data['segment'] == 'NUVB'))[0]
    G185M_C = np.where((data['opt_elem'] == 'G185M') &
                       (data['segment'] == 'NUVC'))[0]

    NUV = np.where((data['opt_elem'] == 'G230L') |
                   (data['opt_elem'] == 'G185M') |
                   (data['opt_elem'] == 'G225M') |
                   (data['opt_elem'] == 'G285M'))[0]

    #############

    fig = plt.figure( figsize=(14,8) )
    ax = fig.add_subplot(3,1,1)

    ax.plot( data['date'][G130M_A], data['x_shift'][G130M_A],'b.',label='G130M')
    ax.plot( data['date'][G130M_B], data['x_shift'][G130M_B],'b.')
    ax.xaxis.set_ticklabels( ['' for item in ax.xaxis.get_ticklabels()] )

    ax2 = fig.add_subplot(3,1,2)
    ax2.plot( data['date'][G160M_A], data['x_shift'][G160M_A],'g.',label='G160M')
    ax2.plot( data['date'][G160M_B], data['x_shift'][G160M_B],'g.')
    ax2.xaxis.set_ticklabels( ['' for item in ax2.xaxis.get_ticklabels()] )

    ax3 = fig.add_subplot(3,1,3)
    ax3.plot( data['date'][G140L_A], data['x_shift'][G140L_A],'y.',label='G140L')
    ax3.plot( data['date'][G140L_B], data['x_shift'][G140L_B],'y.')

    ax.legend(shadow=True, numpoints=1)
    fig.suptitle('FUV SHIFT1[A/B]')
    ax.set_xlabel('MJD')
    ax.set_ylabel('SHIFT1[A/B] (pixels)')

    for axis,index in zip([ax,ax2,ax3],[G130M,G160M,G140L]):
        axis.set_ylim(-300,300)
        axis.set_xlim( data['date'].min(),data['date'].max()+50 )
        axis.set_ylabel('SHIFT1[A/B/C] (pixels)')
        axis.axhline(y=0,color='r')
        axis.axhline(y=285,color='k',lw=3,ls='--',zorder=1,label='Search Range')
        axis.axhline(y=-285,color='k',lw=3,ls='--',zorder=1)
        fit,ydata,parameters,err = fit_data( data['date'][index],data['x_shift'][index] )
        axis.plot( ydata,fit,'k-',lw=3,label='%3.5fx'%(parameters[0]) )
        axis.legend(numpoints=1,shadow=True,prop={'size':10})

    fig.savefig( os.path.join(MONITOR_DIR,'FUV_shifts.png') )
    plt.close(fig)

    ##########

    fig = plt.figure(figsize=(14, 18))
    ax = fig.add_subplot(7, 1, 1)
    ax.plot(data['date'][G185M_A].data, data['x_shift'][G185M_A].data, 'bo', label='G185M')
    ax.plot(data['date'][G185M_B].data, data['x_shift'][G185M_B].data, 'bo', markeredgecolor='k')
    ax.plot(data['date'][G185M_C].data, data['x_shift'][G185M_C].data, 'bo', markeredgecolor='k')
    ax.axhline(y=0, color='red')

    #--second timeframe
    transition_fraction = (56500.0 - data['date'].min()) / \
        (data['date'].max() - data['date'].min())

    ax.axhline(y=58, xmin=0, xmax=transition_fraction, color='k',
                lw=3, ls='--', zorder=1, label='Search Range')
    ax.axhline(y=-58, xmin=0, xmax=transition_fraction,
                color='k', lw=3, ls='--', zorder=1)

    ax.axhline(y=58 - 20, xmin=transition_fraction, xmax=1,
                color='k', lw=3, ls='--', zorder=1)
    ax.axhline(y=-58 - 20, xmin=transition_fraction,
                xmax=1, color='k', lw=3, ls='--', zorder=1)
    #--

    sigma = data['x_shift'][G185M_A].std()

    ax.xaxis.set_ticklabels(['' for item in ax.xaxis.get_ticklabels()])

    ax2 = fig.add_subplot(7, 1, 2)
    ax2.plot(data['date'][G225M_A], data['x_shift'][G225M_A], 'ro', label='G225M')
    ax2.plot(data['date'][G225M_B], data['x_shift'][G225M_B], 'ro', markeredgecolor='k')
    ax2.plot(data['date'][G225M_C], data['x_shift'][G225M_C], 'ro', markeredgecolor='k')
    ax2.axhline(y=0, color='red')

    #--second timeframe
    transition_fraction = (56500.0 - data['date'].min()) / \
        (data['date'].max() - data['date'].min())

    ax2.axhline(y=58, xmin=0, xmax=transition_fraction, color='k', lw=3, ls='--', zorder=1, label='Search Range')
    ax2.axhline(y=-58, xmin=0, xmax=transition_fraction, color='k', lw=3, ls='--', zorder=1)

    ax2.axhline(y=58 - 10, xmin=transition_fraction, xmax=1,
                color='k', lw=3, ls='--', zorder=1)
    ax2.axhline(y=-58 - 10, xmin=transition_fraction,
                xmax=1, color='k', lw=3, ls='--', zorder=1)
    #--

    sigma = data['x_shift'][G225M_A].std()

    ax2.xaxis.set_ticklabels(['' for item in ax2.xaxis.get_ticklabels()])

    ax3 = fig.add_subplot(7, 1, 3)
    ax3.plot(data['date'][G285M_A], data['x_shift'][G285M_A], 'yo', label='G285M')
    ax3.plot(data['date'][G285M_B], data['x_shift']
             [G285M_B], 'yo', markeredgecolor='k')
    ax3.plot(data['date'][G285M_C], data['x_shift']
             [G285M_C], 'yo', markeredgecolor='k')
    ax3.axhline(y=0, color='red')
    ax3.axhline(y=58, color='k', lw=3, ls='--', zorder=1, label='Search Range')
    ax3.axhline(y=-58, color='k', lw=3, ls='--', zorder=1)

    sigma = data['x_shift'][G285M_A].std()

    ax3.xaxis.set_ticklabels(['' for item in ax3.xaxis.get_ticklabels()])

    ax4 = fig.add_subplot(7, 1, 4)
    ax4.plot(data['date'][G230L_A], data['x_shift'][G230L_A], 'go', label='G230L')
    ax4.plot(data['date'][G230L_B], data['x_shift']
             [G230L_B], 'go', markeredgecolor='k')
    ax4.plot(data['date'][G230L_C], data['x_shift']
             [G230L_C], 'go', markeredgecolor='k')

    ax4.axhline(y=0, color='red')

    #--second timeframe
    transition_fraction = (55535.0 - data['date'].min()) / \
        (data['date'].max() - data['date'].min())

    ax4.axhline(y=58, xmin=0, xmax=transition_fraction, color='k',
                lw=3, ls='--', zorder=1, label='Search Range')
    ax4.axhline(y=-58, xmin=0, xmax=transition_fraction,
                color='k', lw=3, ls='--', zorder=1)

    ax4.axhline(y=58 - 40, xmin=transition_fraction, xmax=1,
                color='k', lw=3, ls='--', zorder=1)
    ax4.axhline(y=-58 - 40, xmin=transition_fraction,
                xmax=1, color='k', lw=3, ls='--', zorder=1)
    #--
    ax4.xaxis.set_ticklabels(['' for item in ax3.xaxis.get_ticklabels()])
    sigma = data['x_shift'][G230L_A].std()

    ax.set_title('NUV SHIFT1[A/B/C]')
    for axis, index in zip([ax, ax2, ax3, ax4], [G185M, G225M, G285M, G230L]):
        axis.set_ylim(-110, 110)
        axis.set_xlim(data['date'].min(), data['date'].max() + 50)
        axis.set_ylabel('SHIFT1[A/B/C] (pixels)')
        fit, ydata, parameters, err = fit_data(
            data['date'][index], data['x_shift'][index])
        axis.plot(ydata, fit, 'k-', lw=3, label='%3.5fx' % (parameters[0]))
        axis.legend(numpoints=1, shadow=True, fontsize=12, ncol=3)

    ax4.set_xlabel('date')

    ax = fig.add_subplot(7, 1, 5)
    ax.plot(data['date'][NUV], data['x_shift'][NUV], '.')
    fit, ydata, parameters, err = fit_data(
        data['date'][NUV], data['x_shift'][NUV])
    ax.plot(ydata, fit, 'k-', lw=3, label='%3.5fx' % (parameters[0]))
    ax.legend(numpoints=1, shadow=True)
    ax.set_ylabel('All NUV')
    ax.xaxis.set_ticklabels(['' for item in ax.xaxis.get_ticklabels()])
    ax.set_xlim(data['date'].min(), data['date'].max() + 50)
    ax.set_ylim(-110, 110)

    mirrora = np.where((data['opt_elem'] == 'MIRRORA')
                       & (data['x_shift'] > 0))[0]
    ax = fig.add_subplot(7, 1, 6)
    ax.plot(data['date'][mirrora], data['x_shift'][mirrora], '.')
    fit, ydata, parameters, err = fit_data(
        data['date'][mirrora], data['x_shift'][mirrora])
    ax.plot(ydata, fit, 'k-', lw=3, label='%3.5fx' % (parameters[0]))
    ax.legend(numpoints=1, shadow=True)
    ax.set_xlim(data['date'].min(), data['date'].max() + 50)
    ax.set_ylabel('MIRRORA')
    ax.set_xlabel('date')
    ax.set_ylim(460, 630)

    mirrorb = np.where((data['opt_elem'] == 'MIRRORB')
                       & (data['x_shift'] > 0))[0]
    ax = fig.add_subplot(7, 1, 7)
    ax.plot(data['date'][mirrorb], data['x_shift'][mirrorb], '.')
    fit, ydata, parameters, err = fit_data(
        data['date'][mirrorb], data['x_shift'][mirrorb])
    ax.plot(ydata, fit, 'k-', lw=3, label='%3.5fx' % (parameters[0]))
    ax.legend(numpoints=1, shadow=True)
    ax.set_xlim(data['date'].min(), data['date'].max() + 50)
    ax.set_ylabel('MIRRORB')
    ax.set_xlabel('date')
    ax.set_ylim(260, 400)


    fig.savefig(os.path.join(MONITOR_DIR, 'NUV_shifts.png'),
                bbox_inches='tight',
                pad_inches=.5)
    plt.close(fig)

    ##############

    for elem in ['MIRRORA', 'MIRRORB']:
        mirror = np.where((data['opt_elem'] == elem)
                          & (data['x_shift'] > 0))[0]
        fig = plt.figure(figsize=(8, 4))
        ax = fig.add_subplot(1, 1, 1)
        ax.plot(data['date'][mirror], data['x_shift'][mirror], '.')
        fit, ydata, parameters, err = fit_data(data['date'][mirror],
                                               data['x_shift'][mirror])
        ax.plot(ydata, fit, 'r-', lw=3, label='%3.5f +/- %3.5f' %
                (parameters[0], err))
        ax.legend(numpoints=1, shadow=True)
        ax.set_xlim(data['date'].min(), data['date'].max() + 50)
        #ax.set_ylim(460, 630)
        fig.savefig(os.path.join(MONITOR_DIR, '{}_shifts.png'.format(elem.upper())))
        plt.close(fig)

    print 'Plotting cenwaves'
    for grating in list(set(data['opt_elem'])):
        fig = plt.figure()
        ax = fig.add_axes([.1, .1, .75, .8])
        ax.set_title(grating)
        for cenwave in list(set(data['cenwave'])):
            index = np.where((data['opt_elem'] == grating) &
                             (data['cenwave'] == cenwave))[0]
            if not len(index):
                continue

            xdata = np.array(map(int, data['date'][index]))
            ydata = data['x_shift'][index]
            new_ydata = []
            new_xdata = []
            for day in range(xdata.min(), xdata.max() + 1):
                index = np.where(xdata == day)[0]
                #n_times = len(index)
                median = np.median(ydata[index])
                new_ydata.append(median)
                new_xdata.append(day)

            if cenwave < 1700:
                ms = 6
                ylim = (-140, 80)
            else:
                ms = 10
                ylim = (-80, 80)

            ax.plot(new_xdata, new_ydata, '.', ms=ms, alpha=.7, label='%d' %
                    (cenwave))

            plt.legend(numpoints=1, shadow=True, bbox_to_anchor=(1.05, 1),
                       loc=2, borderaxespad=0., prop={'size': 8})
            ax.set_xlim(data['date'].min(), data['date'].max() + 50)
            ax.set_ylim(ylim[0], ylim[1])
        fig.savefig(os.path.join(MONITOR_DIR, '%s_shifts_color.pdf' %
                    (grating)))
        plt.close(fig)

#----------------------------------------------------------

def make_plots_2(data):
    """ Making the plots for the shift2 value
    """

    sorted_index = np.argsort(data['date'])
    data = data[sorted_index]
    '''
    for cenwave in set(data['cenwave']):
        cw_index = np.where(data['cenwave'] == cenwave)
        all_segments = set(data[cw_index]['segment'])
        n_seg = len(all_segments)

        fig = plt.figure()
        fig.suptitle('Shift2/{}'.format(cenwave))

        for i, segment in enumerate(all_segments):
            print cenwave, segment
            index = np.where( (data['segment'] == segment) &
                              (data['cenwave'] == cenwave) )

            ax = fig.add_subplot(n_seg, 1, i+1)
            ax.plot(data[index]['date'], data[index]['y_shift'], 'o')
            ax.set_xlabel('date')
            ax.set_ylabel('SHIFT2 {}'.format(segment))

        fig.savefig(os.path.join(MONITOR_DIR, 'shift2_{}.png'.format(cenwave)))
        plt.close(fig)
    '''

    print "relations"
    for cenwave in set(data['cenwave']):
        cw_index = np.where(data['cenwave'] == cenwave)
        all_segments = set(data[cw_index]['segment'])
        n_seg = len(all_segments)

        fig = plt.figure()
        fig.suptitle('Shift2 vs Shift1 {}'.format(cenwave))

        for i, segment in enumerate(all_segments):
            index = np.where( (data['segment'] == segment) &
                              (data['cenwave'] == cenwave) )

            ax = fig.add_subplot(n_seg, 1, i+1)
            ax.plot(data[index]['x_shift'], data[index]['y_shift'], 'o')
            ax.set_xlabel('x_shift')
            ax.set_ylabel('y_shift')
            #ax.set_ylabel('SHIFT2 vs SHIFT1 {}'.format(segment))
            #ax.set_ylim(-20, 20)

        fig.savefig(os.path.join(MONITOR_DIR, 'shift_relation_{}.png'.format(cenwave)))
        plt.close(fig)


#----------------------------------------------------------

def fp_diff(data):
    print "Checking the SHIFT2 difference"

    index = np.where((data['detector'] == 'FUV'))[0]
    data = data[index]

    datasets = list(set(data['dataset']))
    datasets.sort()

    all_cenwaves = set(data['cenwave'])
    diff_dict = {}
    for cenwave in all_cenwaves:
        diff_dict[cenwave] = []

    ofile = open(os.path.join(MONITOR_DIR, 'shift_data.txt'), 'w')
    for name in datasets:
        a_shift = None
        b_shift = None
        try:
            a_shift = data['x_shift'][np.where((data['dataset'] == name) &
                                               (data['segment'] == 'FUVA'))[0]][0]
            b_shift = data['x_shift'][np.where((data['dataset'] == name) &
                                               (data['segment'] == 'FUVB'))[0]][0]
        except IndexError:
            continue

        cenwave = data['cenwave'][np.where((data['dataset'] == name) &
                                           (data['segment'] == 'FUVA'))[0]][0]
        opt_elem = data['opt_elem'][np.where((data['dataset'] == name) &
                                             (data['segment'] == 'FUVA'))[0]][0]
        fppos = data['fppos'][np.where((data['dataset'] == name) &
                                       (data['segment'] == 'FUVA'))[0]][0]
        mjd = data['date'][np.where((data['dataset'] == name) &
                                   (data['segment'] == 'FUVA'))[0]][0]
        diff = a_shift - b_shift

        diff_dict[cenwave].append((mjd, diff))
        print '%5.5f  %s  %d  %d   %3.2f  %3.2f  \n'%(mjd, opt_elem, cenwave, fppos, a_shift, b_shift)
        ofile.write('%5.5f  %s  %d  %d   %3.2f  %3.2f  \n' %
                    (mjd, opt_elem, cenwave, fppos, a_shift, b_shift))

    for cenwave in diff_dict:
        all_diff = [line[1] for line in diff_dict[cenwave]]
        all_mjd = [line[0] for line in diff_dict[cenwave]]

        if not len(all_diff):
            continue

        print 'plotting', cenwave
        plt.figure(figsize=(8, 5))
        plt.plot(all_mjd, all_diff, 'o', label='%s' % (cenwave))
        plt.xlabel('MJD')
        plt.ylabel('SHIFT1 difference (pixels)')
        plt.title(cenwave)
        plt.legend(shadow=True, numpoints=1, loc='best')
        plt.savefig(os.path.join(MONITOR_DIR, 'difference_%s.pdf' % (cenwave)))
        plt.close()


    # for cenwave in diff_dict:
    #    all_diff = diff_dict[cenwave]
    #    print all_diff
    #    if not len(all_diff): continue
    #    plt.plot(all_diff,bins=100)
    #    plt.ylabel('Frequency (counts)')
    #    plt.xlabel('SHIFT1A difference (pixels)')
    #    plt.title(cenwave)
    #    plt.savefig('plot_%s.pdf'%(cenwave) )

        # plt.clf()

#----------------------------------------------------------

def monitor():
    """Run the entire suite of monitoring
    """

    flash_data = make_shift_table()
    make_plots(flash_data)
    make_plots_2(flash_data)
    #fp_diff(flash_data)

    for item in glob.glob(os.path.join(MONITOR_DIR, '*.p??')):
        shutil.copy(item, WEB_DIR)

#----------------------------------------------------------
