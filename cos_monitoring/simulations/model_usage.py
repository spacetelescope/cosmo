import os
from astropy.io import fits as pyfits
import glob
import numpy as np
import matplotlib.pyplot as plt
from astropy.io import ascii
import pdb
import argparse

from scipy.signal import medfilt

#-------------------------------------------------------------------------------

def enlarge(a, x=2, y=None):

    """
    Enlarges 2D image array a using simple pixel repetition in both dimensions.
    Enlarges by factor x horizontally and factor y vertically.
    If y is left as None, uses factor x for both dimensions.
    """

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

def make_slope_file( hv_values, segment, outname ):

    """
    Generate arrays of mean slope in each pixel

    Parameters
    ----------
        hv_values: array like
            A list of HV values.
        segment: string
            Detector Segment
        outname: string
            name of file created by function

    Returns
    -------
        None
    """

    slopes_dir = '/grp/hst/cos/Monitors/CCI/'

    if os.path.exists( outname ):
        print 'Slope file already exists, skipping'
        return

    if segment == 'FUVA':
        segment = '00'
    elif segment == 'FUVB':
        segment = '01'

    slope_files = [ os.path.join( slopes_dir, 'proj_bad_{}_cci_{}.fits'.format(segment, dethv) )
                                  for dethv in hv_values ]

    out_slopes = []

    for item in slope_files:
        hdu = pyfits.open( item )

        out_slopes.append( hdu['slope'].data )

    out_slopes = enlarge( np.mean( out_slopes, axis=0 ), x=8, y=2 )

    hdu_out = pyfits.HDUList(pyfits.PrimaryHDU())
    hdu_out.append(pyfits.ImageHDU(data=out_slopes))
    hdu_out.writeto( outname )

#-------------------------------------------------------------------------------

def assemble_slopes():

    """
    uses make_slope files to calculate all of the slopes for each pixel for each
    segment and selected HV levels.

    Parameters
    ----------
        None

    Returns
    -------
        None
    """

    make_slope_file( [167], 'FUVA', 'slope_FUVA_LP2.fits' )
    make_slope_file( [163], 'FUVB', 'slope_FUVB_LP2.fits' )

    make_slope_file( [169, 178], 'FUVA', 'slope_FUVA_LP1.fits' )
    make_slope_file( [167, 175], 'FUVB', 'slope_FUVB_LP1.fits' )

#-------------------------------------------------------------------------------

def make_usage_file( file_list, outname ):
    """
    Creates (writes) the usage file that is then used to perform aging simulations.

    Parameters
    ----------
        file_list: array like
            A list of files that contain cumulative count and exposure time info.
        outname: string
            The name of the file being created.

    Returns
    -------
        None

    """

    if not len( file_list ): return None

    hdu_out = pyfits.open( file_list[0] )

    for item in file_list[1:]:
        print item
        new_hdu = pyfits.open( item )
        hdu_out[1].header['exptime'] += new_hdu[1].header['exptime']
        hdu_out[1].data += new_hdu[1].data
        new_hdu.close()

    hdu_out.writeto( outname, output_verify='ignore' )

#-------------------------------------------------------------------------------

def assemble_usage():
    """
    assemble_usage gathers files created by Dave Sanhow (sahnow@stsci.edu) which
    cycles through smov/cos/Data and creates a fits file that keeps track of the
    cumulative counts and exposure times for each cenwave setting and segment.

    A few intermediate files are created by this function...


    1.) 'summed_{}_{}.fits'.format( lp, segment )
        This file sums all of the counts for each segment and LP.

    2.) 'summed_{}_{}_{}.fits'.format( grating, lp, segment )
        This file sums all of the counts for each segment and LP for each grating

    3.) 'summed_{}_{}_{}.fits'.format( cenwave, lp, segment )
        This file sums all of the counts for each segment and LP for each cenwave.

    Parameters
    ----------
        None

    Returns
    -------
        None

    """
    data_dir = '/grp/hst/cos/coslife/mode_by_mode'

    for segment in ['A', 'B']:
        for lp in ['LP1', 'LP2']:
            print 'Cumulating {} {} datasets'.format( segment, lp )
            all_datasets = glob.glob( os.path.join( data_dir, 'FP*_{}-{}.fits*'.format( lp, segment) ) )
            outname = 'summed_{}_{}.fits'.format( lp, segment )
            if os.path.exists( outname ):
                print outname, 'already exists'
                continue
            make_usage_file( all_datasets, outname )

    # I need the sums of all data for each LP
    for grating in ['G130M', 'G160M', 'G140L']:
        for segment in ['A', 'B']:
            for lp in ['LP1', 'LP2']:
                print 'Cumulating {} {} {} datasets'.format( grating, segment, lp )
                all_datasets = glob.glob( os.path.join( data_dir, 'FP*{}*_{}-{}.fits*'.format(grating, lp, segment) ) )
                outname = 'summed_{}_{}_{}.fits'.format( grating, lp, segment )
                if os.path.exists( outname ):
                    print outname, 'already exists'
                    continue
                make_usage_file( all_datasets, outname )

    for cenwave in ['1055', '1096', '1105', '1222',
                    '1230', '1280', '1291', '1300',
                    '1309', '1318', '1327', '1577',
                    '1589', '1600', '1611', '1623']:
        for segment in ['A', 'B']:
            for lp in ['LP1', 'LP2']:
                print 'Cumulating {} {} {} datasets'.format( cenwave, segment, lp )
                all_datasets = glob.glob( os.path.join( data_dir, 'FP*{}*_{}-{}.fits*'.format(cenwave, lp, segment) ) )
                outname = 'summed_{}_{}_{}.fits'.format( cenwave, lp, segment )
                if os.path.exists( outname ):
                    print outname, 'already exists'
                    continue
                make_usage_file( all_datasets, outname )

#-------------------------------------------------------------------------------

def make_degrade_file( slopes, frac_usage, multiplier = 2.0, outname='degredation.fits' ):

    """
    Makes the file that show the degredation at each LP based on predictions and usage.

    Parameters
    ----------
    slopes:
        Slope of the degradation as a function of usage for each pixel (?)
    frac_usage:
        The fraction of the usage time compared to other configurations.
    multiplier: float
        The ratio of time that a configuration of settings is being used compared to
        previous cycles.
    outname:str
        The name of the file that is going to be output

    Returns
    -------
        None

    """

    gainloss_per_day = multiplier * slopes * frac_usage

    gainloss_per_day[ np.isnan( gainloss_per_day ) ] = 0

    smoothed = medfilt( gainloss_per_day, (1, 301) )

    gainloss_per_day[:,0:4500] = smoothed[:,0:4500]
    gainloss_per_day[:,14000:] = smoothed[:,14000:]

    hdu_out = pyfits.HDUList(pyfits.PrimaryHDU())
    hdu_out.append(pyfits.ImageHDU(data=gainloss_per_day))
    hdu_out.writeto( outname )

#-------------------------------------------------------------------------------

def make_fractional_files( lp, usage_file=None ):
    """
    make_fracitonal_files calculates the fractional usage
    for all cenwave settings for both segments of the COS FUV mode

    Parameters
    ----------
    lp : str
        The lifetime position of the detector (LP1, LP2, .... etc).
    usage_file : str
        A text file that contains 4 columns for aging simulations

        column 1: Item
            Cenwave setting
        column 2: Cycle 20
            The usage time in seconds (?) for each cenwave settings for that cycle
        column 3: Cycle 21
            The usage time in seconds (?) for each cenwave setting for that cycle
        column 4: change
            The ratio of usage time from cycle 20 and 22 to show how much the usage has increased
            or decreased to make predictions for future cycles.

    Returns
    -------
        None

    """
    if usage_file:
        usage = ascii.read( usage_file )
        multiplier = {}
        for line in usage:
            print line['Item']
            cenwave = str(line['Item'])
            multiplier[ cenwave ] = line['change']

    for cenwave in ['1055', '1096', '1105', '1222',
                    '1280', '1291', '1300',
                    '1309', '1318', '1327', '1577',
                    '1589', '1600', '1611', '1623']:
        for segment in ['FUVA', 'FUVB']:
            outname = '{}_degrade_{}_{}.fits'.format(cenwave, segment, lp)
            if usage_file:
                mode_multiplier = multiplier[cenwave]
            else:
                mode_multiplier = 1

            if os.path.exists( outname ):
                print 'Already exists'
                continue

            slopes = pyfits.getdata( 'slope_{}_{}.fits'.format( segment, lp ) )
            try:
                frac_usage = pyfits.getdata( 'summed_{}_{}_{}.fits'.format(cenwave, lp, segment[-1]) ) \
                    / pyfits.getdata( 'summed_{}_{}.fits'.format( lp, segment[-1]) )
            except IOError:
                continue
            make_degrade_file( slopes, frac_usage, multiplier=mode_multiplier, outname=outname )

#-------------------------------------------------------------------------------

def count_sagged( gain, yrange=(0,1024), xrange=(0,16384), thresh=3.0 ):
    """
    Return # of pixels and # of columns affected by sagged pixels

    Parameters
    ----------
    gain: array-like
        The gain information for each setting and segment

    yrange: tuple
        The range of coordinates in the y direction of the detector

    thresh: float
        PHA threshold

    Returns
    -------
    n_pixels: int
        the length of effected pixels.
    n_columns: int
        the length of effected columns.

    """

    index = np.where( gain[yrange[0]:yrange[1], xrange[0]:xrange[1]] <= thresh )

    n_pixels = len( index[0] )
    n_columns = len( set(index[1]) )

    return n_pixels, n_columns

#-------------------------------------------------------------------------------

def increase_gain( modal_gain, hv_step):

    """
    Return # of pixels and # of columns affected by sagged pixels

    Parameters
    ----------
    modal_gain: int
        modal gain of the detector segment
    hv_steps: array-like
        list of high voltage steps

    Returns
    -------
    modal_gain: int
        modal gain of the detector segment

    """

    index = np.where( modal_gain )
    modal_gain[index] += .393 * hv_step

    return modal_gain

#-------------------------------------------------------------------------------

def assemble_degrade( life_adj, segment, *args ):

    """
    generate the degredation array for all combinations of life_adj and segment.

    Parameters
    ----------
        life_adj: int
            life time adjustment
        segment: str
            COS segment FUVA or FUVB
        args: array-like like
            all cenwave settings
    Returns
    -------
        all_degrade_array: array like
            an array full of all of the degredation information.
    """

    print 'Creating degredation array for life_adj: {}, segment: {}'.format( life_adj, segment )
    print args

    all_degrade_array = np.zeros( (1024, 16384) )

    for cenwave in args:
        print cenwave
        try: all_degrade_array += pyfits.getdata( '{}_degrade_{}_LP{}.fits'.format( cenwave, segment, life_adj ) )
        except IOError:
            print 'Not using ', cenwave, segment, life_adj, 'due to IOError'

    return all_degrade_array

#-------------------------------------------------------------------------------

def simulate( gain_start=163, steps=[178], segment='FUVB', gain_thresh=3, affected_thresh = .05,
              yshift=0, xshift=0):
    """
    simulate gain degredation

    Parameters
    ----------
        gain_start: int
            A list of files that contain cumulative count and exposure time info.
        steps: array-like
            The name of the file being created.
        segment: str
            COS segment FUVA or FUVB
        gain_thresh: int
            PHA
        affected_thresh: float
            ?
        yshift: int
            y shift in detector space
        xshift: int
            x shift in detector space
    Returns
    -------
        None

    Part of Old docstring we should hold onto.
    ------------------------------------------
    1. degrade LP2 with all modes using predicted Cycle 21 usage untill Sept 01 2014
      - Leave G140L at LP2 and degrade, raising HV whenever gainsagged pixels appear
      - Leave G140L at Lp1 ''

    """

    print '#-------------------#'
    print 'Begginning simulation'
    print '#-------------------#'

    gain_end = max( steps )
    initial_steps = steps[:] # force copy on mutable datatype

    life_adj = 2
    if segment == 'FUVA':
        #Lp2, FUVA
        ylim = (508, 544)
        xlim = (1140, 15220)
        total_lp2 = 300
        total_lp3 = 700
    elif segment == 'FUVB':
        #Lp2, FUVB
        ylim = (567, 598)
        xlim = (900, 15000)
        total_lp2 = 220
        total_lp3 = 600

    all_cenwaves = [1055, 1096, 1105, 1222, 1280, 1291, 1300, 1309, 1318, 1327, 1577, 1589, 1600, 1611, 1623]

    seg_ext = {'FUVA':1, 'FUVB':2}
    gainmap_dir = '/grp/hst/cos/Monitors/CCI'

    print 'Creating usage arrays'
    gainmap = pyfits.getdata( os.path.join( gainmap_dir, 'total_gain_{}.fits'.format( gain_start ) ),
                              ext=seg_ext[segment] )

    all_degrade_array = assemble_degrade( life_adj, segment, *all_cenwaves )

    cenwave_sample = [1105, 1222, 1280, 1291, 1300, 1309, 1318, 1327, 1577, 1589, 1600, 1611, 1623]
    degrade_array = assemble_degrade( life_adj, segment, *cenwave_sample)

    ### to move the degredation to LP3
    degrade_array = np.roll( degrade_array, shift=yshift, axis=0)
    degrade_array = np.roll( degrade_array, shift=xshift, axis=1)

    ### Perhaps this was the problem?  BOA or something?
    #lp2_remain = [1055, 1096]
    #degrade_array += assemble_degrade( life_adj, segment, *lp2_remain)

    plt.ioff()
    fig =  plt.figure()
    ax = fig.add_subplot( 1,1,1 )

    frac_affected = 0
    current_gain = gain_start
    n_days = 0
    days_step = 10
    ylim_2 = (ylim[0]+yshift, ylim[1]+yshift)
    ylim_3 = (ylim_2[0]-30, ylim_2[1]-30)

    reset_lp2 = False
    reset_lp3 = False

    while frac_affected < affected_thresh:
        n_pixels, n_columns = count_sagged( gainmap, yrange=ylim, xrange=xlim )
        frac_affected = n_columns / float((xlim[1] - xlim[0]))

        if n_days < total_lp2:
            low_index = np.where( gainmap <= 3 )
            high_index = np.where( gainmap > 3 )

            gainmap[ high_index ] += all_degrade_array[high_index] * days_step
            gainmap[ low_index ] += (all_degrade_array[low_index] / 2.) * days_step
        elif n_days < total_lp3:
            if not reset_lp2:
                print 'RESET at Lp3'
                increase_gain( gainmap, gain_start -current_gain )
                current_gain = gain_start
                steps = initial_steps[:]
                reset_lp2 = True

            ylim = ylim_2
            ylim = (ylim[0] + 5, ylim[1] - 5)

            low_index = np.where( gainmap <= 3 )
            high_index = np.where( gainmap > 3 )
            gainmap[ high_index ] += degrade_array[high_index] * days_step
            gainmap[ low_index ] += (degrade_array[low_index] / 2.) * days_step

        else:
            if not reset_lp3:
                print 'RESET at lp4'
                increase_gain( gainmap, gain_start -current_gain )
                current_gain = gain_start
                steps = initial_steps[:]
                degrade_array = np.roll( degrade_array, shift=-30, axis=0)
                degrade_array = np.roll( degrade_array, shift=-115, axis=1)
                reset_lp3 = True

            ylim = ylim_3
            ylim = (ylim[0] + 5, ylim[1] - 5)

            low_index = np.where( gainmap <= 3 )
            high_index = np.where( gainmap > 3 )
            gainmap[ high_index ] += degrade_array[high_index] * days_step
            gainmap[ low_index ] += (degrade_array[low_index] / 2.) * days_step

        n_days += days_step

        #if n_days > total_lp2 and frac_affected > .001:
        #    hdu_out = pyfits.HDUList(pyfits.PrimaryHDU())
        #    hdu_out.append(pyfits.core.ImageHDU(data=gainmap))
        #    hdu_out.writeto( 'simulation_{}_{}_{}_{}_{}.fits'.format( segment, yshift, xshift, n_days, current_gain ), clobber=True )

        if (current_gain < gain_end) and frac_affected > .02:
            print 'Raising gain on day', n_days, 'gain end', gain_end

            #if n_days > total_lp2:
            #    hdu_out = pyfits.HDUList(pyfits.PrimaryHDU())
            #    hdu_out.append(pyfits.core.ImageHDU(data=gainmap))
            #    hdu_out.writeto( 'simulation_{}_{}_{}_{}_{}.fits'.format( segment, yshift, xshift, n_days, current_gain ), clobber=True )

            new_gain = steps.pop(0)
            increase_gain( gainmap, new_gain - current_gain )
            current_gain = new_gain

        print n_days, n_columns, frac_affected, current_gain , ylim
        ax.plot( np.min(gainmap[ylim[0]:ylim[1]], axis=0) )
        ax.axhline( y=3, color='r', lw=3 )
        ax.set_ylim(0, 15 )

        day_str = str(n_days)
        while len(day_str) < 4:
            day_str = '0' + day_str

        fig.savefig( 'worst_{}_{}_{}_{}.pdf'.format( day_str, segment, yshift, xshift  ) )
        plt.cla()

        if n_days > 1500: break

    print 'Total time spent:', n_days/365.25

    hdu_out = pyfits.HDUList(pyfits.PrimaryHDU())
    hdu_out.append(pyfits.ImageHDU(data=gainmap))
    hdu_out.writeto( 'simulation_{}_{}_{}.fits'.format( segment, yshift, xshift ), clobber=True )

    plt.ioff()
    np.where( gainmap <=0, np.nan, gainmap )
    plt.imshow( gainmap, aspect='auto', interpolation='nearest' )
    #plt.axhline( y=ylim[0], lw=3, color='w' )
    #plt.axhline( y=ylim[1], lw=3, color='w' )
    plt.clim(0, 15)
    plt.colorbar()

    plt.savefig( 'simulation_{}_{}_{}.pdf'.format( segment, yshift, xshift ), bbox_inches='tight'  )


#-------------------------------------------------------------------------------

def verify_degradation():
    """
    I have absolutely no idea what this does just yet.
    """

    all_cenwaves = [1105,
                    1096,
                    1105,
                    1222,
                    1280,
                    1230,
                    1291,
                    1300,
                    1309,
                    1318,
                    1327,
                    1577,
                    1589,
                    1600,
                    1611,
                    1623]

    degrade_lp1 = assemble_degrade( 1, 'FUVB', *all_cenwaves )

    hdu = pyfits.open( '/grp/hst/cos/Monitors/CCI/55060.0000-55067.0000-01_cci_gainmap.fits' )

    initial_gain = enlarge( hdu['mod_gain'].data, y=2, x=8 )
    dethv = hdu[0].header['DETHV']

    initial_gain = increase_gain( initial_gain, 8 )
    print dethv + 8
    n_days = 1070

    final_gain = initial_gain + degrade_lp1 * n_days
    test_gain = pyfits.getdata( '/grp/hst/cos/Monitors/CCI/56124.0000-56131.0000-01_cci_gainmap.fits',
                                ext=('mod_gain',1) )
    test_gain = enlarge( test_gain, y=2, x=8)

    diff = test_gain - final_gain

    plt.ion()
    plt.subplot( 3,1,1 )
    plt.title( 'Simulated Gain' )
    plt.imshow( final_gain, aspect='auto', interpolation='nearest' )
    plt.clim(0,20)
    plt.colorbar()
    plt.ylim( 375, 675 )

    plt.subplot( 3,1,2 )
    plt.title( 'Actual Gain' )
    plt.imshow( test_gain, aspect='auto', interpolation='nearest' )
    plt.clim(0,20)
    plt.colorbar()
    plt.ylim( 375, 675 )

    plt.subplot( 3,1,3 )
    plt.title( 'Difference (modal gain)' )
    plt.imshow( diff, aspect='auto', interpolation='nearest' )
    plt.clim(-1,1)
    plt.colorbar()
    plt.ylim( 375, 675 )

    pdb.set_trace()

#-------------------------------------------------------------------------------

def main():

    '''
    Main driver

    Argparse, maybe combine a couple of the arguments into a list to shrink
    the number of args.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-lp",'--lifepos', nargs='+',
                        help="life time position you wish to simulate. Accepts one or more life time positions")
    parser.add_argument("-seg", '--segment',
                        help="COS FUV segment of choice. Can accept FUVA or FUVB")
    parser.add_argument("-gs","--gain_start", type=int,
                        help='High Voltage level to start simulation at.')
    parser.add_argument("--steps", nargs='+', type=int,
                        help="High voltage levels you want to step through.")
    parser.add_argument("-xs", '--x_shift', type=int,
                        help="x position shifted on the detector")
    parser.add_argument("-ys", '--y_shift', type=int,
                        help="y position shifted on the detector")
    args = parser.parse_args()


    assemble_usage()
    assemble_slopes()

    for lifepos in args.lp:
        if lifepos == 'LP1':
            make_fractional_files(lp=lifepos)
        else:
            make_fractional_files(lp=lifepos, usage_file='usage.txt' )


    plt.ioff()

    #simulate( gain_start=167, steps=[167, 173, 178], segment='FUVA', yshift=-70, xshift=70 )
    simulate( gain_start=args.gain_start, steps=args.steps, segment=args.seg,  yshift=args.y_shift, xshift=args.x_shift )

    #verify_degradation()
    #for yshift in [ -71, -74, -82 ]:
    #    for xshift in [ 0, 70 ]:
    #        simulate( gain_start=167, steps=[167, 173, 178], segment='FUVA', yshift=yshift, xshift=xshift )
    #        simulate( gain_start=163, steps=[163, 167, 171, 175], segment='FUVB',  yshift=yshift, xshift=xshift )

#-------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
