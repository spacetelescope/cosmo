""" Interface to the noaa site and grab daily 10.7 cm solar flux measurements

"""

from __future__ import print_function, absolute_import, division

from astropy.io import ascii
import numpy as np
import os
import glob
from ftplib import FTP

from astropy.time import Time

from ..utils import remove_if_there

#-------------------------------------------------------------------------------

def grab_solar_files(file_dir):
    """Pull solar data files from NOAA website

    Solar data is FTPd from NOAO and written to text files for use in plotting
    and monitoring of COS dark-rates and TDS.

    Parameters
    ----------
    file_dir : str
        Directory to write the files to

    """

    ftp = FTP('ftp.swpc.noaa.gov')
    ftp.login()

    ftp.cwd('/pub/indices/old_indices/')

    for item in sorted(ftp.nlst()):
        if item.endswith('_DSD.txt'):
            year = int(item[:4])
            if year >= 2000:
                print('Retrieving: {}'.format(item))
                destination = os.path.join(file_dir, item)
                ftp.retrbinary('RETR {}'.format(item), open(destination, 'wb').write)

                os.chmod(destination, 0o777)

#-------------------------------------------------------------------------------

def compile_txt(file_dir):
    """ Pull desired columns from solar data text files

    Parameters
    ----------

    Returns
    -------
    date : np.ndarray
        mjd of each measurements
    flux : np.ndarray
        solar flux measurements

    """

    date = []
    flux = []
    input_list = glob.glob(os.path.join(file_dir, '*DSD.txt'))
    input_list.sort()

    for item in input_list:
        print('Reading {}'.format(item))

        #-- clean up Q4 files when year-long file exists
        if ('Q4_' in item) and os.path.exists(item.replace('Q4_', '_')):
            print("Removing duplicate observations: {}".format(item))
            os.remove(item)
            continue
        data = ascii.read(item, data_start=1, comment='[#,:]')

        for line in data:
            line_date = Time('{}-{}-{} 00:00:00'.format(line['col1'],
                                                        line['col2'],
                                                        line['col3']),
                             scale='utc', format='iso').mjd
            line_flux = line[3]

            if line_flux > 0:
                date.append(line_date)
                flux.append(line_flux)

    return np.array(date), np.array(flux)

#-------------------------------------------------------------------------------

def get_solar_data(file_dir):
    """ Compile the necessary solar data from NOAA

    Parameters
    ----------
    file_dir : str
        directory containing retrieved solar data txt files

    Outputs
    -------
    solar_flux.txt :
        txt file containing mjd,flux of solar measurements

    """

    print('Gettting Solar flux data')
    for txtfile in glob.glob(os.path.join(file_dir, '*_D?D.txt')):
        os.remove(txtfile)

    grab_solar_files(file_dir)
    date, flux = compile_txt(file_dir)

    out_solar_file = os.path.join(file_dir, 'solar_flux.txt')
    remove_if_there(out_solar_file)

    with open(out_solar_file, 'w') as outfile:
        for d, f in zip(date, flux):
            outfile.write('%4.5f  %d\n' % (d, f))
    os.chmod(out_solar_file, 0o777)

#-------------------------------------------------------------------------------
