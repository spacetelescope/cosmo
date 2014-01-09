""" Interface to the noaa site and grab daily 10.7 cm solar flux measurements

"""

import datetime
import numpy as np
import os
import glob

from astropy.time import Time

#-------------------------------------------------------------------------------

def grab_solar_files( file_dir ):
    base_webpage = 'http://www.swpc.noaa.gov/ftpdir/indices/old_indices/'
    this_year = datetime.datetime.now().year

    for index in ['DPD', 'DSD']:
        for year in range(1997, this_year):
            file_name = '%d_%s.txt' % (year, index)
            get_command = "wget --quiet -O %s%s %s%s" % (file_dir,
                                                         file_name, base_webpage, file_name)
            print get_command
            os.system(get_command)

        for quarter in ('Q1', 'Q2', 'Q3', 'Q4'):
            file_name = '%d%s_%s.txt' % (this_year, quarter, index)
            get_command = "wget --quiet -O %s%s %s%s" % (file_dir,
                                                         file_name, base_webpage, file_name)
            print get_command
            os.system(get_command)

#-------------------------------------------------------------------------------

def compile_txt( file_dir ):
    date = []
    flux = []
    input_list = glob.glob(os.path.join(file_dir, '*DSD.txt'))
    input_list.sort()
    for item in input_list:
        try:
            data = np.genfromtxt(item, skiprows=13, dtype=None)
        except IOError:
            continue
        except StopIteration:
            continue
        for line in data:
            line_date = Time( '{}-{}-{} 00:00:00'.format(line[0], line[1], line[2]),
                              scale='utc' ).mjd
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

    outfile = open(os.path.join(file_dir, 'solar_flux.txt'), 'w')
    for d, f in zip(date, flux):
        outfile.write('%4.5f  %d\n' % (d, f))
    outfile.close()
