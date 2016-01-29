from __future__ import absolute_import, division

""" Interface to the noaa site and grab daily 10.7 cm solar flux measurements

"""

from astropy.io import ascii
import datetime
import numpy as np
import os
import glob
from ftplib import FTP

from astropy.time import Time

#-------------------------------------------------------------------------------

def grab_solar_files(file_dir):
    ftp = FTP('ftp.swpc.noaa.gov')
    ftp.login()

    ftp.cwd('/pub/indices/old_indices/')

    for item in sorted(ftp.nlst()):
        if (item.endswith('_DSD.txt')):
            year = int(item[:4])
            if year >= 2000:
                print 'Retrieving: {}'.format(item)
                destination = os.path.join(file_dir, item)
                ftp.retrbinary('RETR {}'.format(item), open(destination, 'wb').write)

                os.chmod(destination, 0o777)

#-------------------------------------------------------------------------------

def compile_txt(file_dir):
    date = []
    flux = []
    input_list = glob.glob(os.path.join(file_dir, '*DSD.txt'))
    input_list.sort()
    for item in input_list:
        print 'Reading {}'.format(item)
        data = ascii.read(item, data_start=1, comment='[#,:]')
        for line in data:
            line_date = Time('{}-{}-{} 00:00:00'.format(line['col1'], line['col2'], line['col3']),
                             scale='utc', format='iso').mjd
            line_flux = line[3]
            if line_flux > 0:
                date.append(line_date)
                flux.append(line_flux)

    return np.array(date), np.array(flux)

#-------------------------------------------------------------------------------

def get_solar_data( file_dir ):
    print 'Gettting Solar flux data'
    for txtfile in glob.glob(os.path.join(file_dir, '*_D?D.txt')):
        os.remove(txtfile)

    grab_solar_files(file_dir)
    date, flux = compile_txt(file_dir)

    out_solar_file = os.path.join(file_dir, 'solar_flux.txt')
    with open(out_solar_file, 'w') as outfile:
        for d, f in zip(date, flux):
            outfile.write('%4.5f  %d\n' % (d, f))
    os.chmod(out_solar_file, 0o777)
