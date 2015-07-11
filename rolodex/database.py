from __future__ import print_function

from astropy.io import fits
import os
from sqlalchemy import and_
import yaml
import numpy as np
import multiprocessing as mp

from census import find_all_datasets
from db_tables import Base, session, engine, Files, Lampflash, Headers

#-------------------------------------------------------------------------------

def fppos_shift(lamptab_name, segment, opt_elem, cenwave, fpoffset):
    lamptab = fits.getdata(os.path.join(os.environ['lref'], lamptab_name))

    if 'FPOFFSET' not in lamptab.names:
        return 0

    index = np.where((lamptab['segment'] == segment) &
                     (lamptab['opt_elem'] == opt_elem) &
                     (lamptab['cenwave'] == cenwave) &
                     (lamptab['fpoffset'] == fpoffset))[0]

    offset = lamptab['FP_PIXEL_SHIFT'][index][0]

    return offset

#-------------------------------------------------------------------------------

def insert_files(**kwargs):
    """Create table of path, filename"""

    previous_files = {os.path.join(path, fname) for path, fname in session.query(Files.path, Files.name)}

    data_location = kwargs.get('data_location', './')
    for path, filename in find_all_datasets(data_location):
        if os.path.join(path, filename) in previous_files:
            continue

        print("Found {}".format(os.path.join(path, filename)))
        session.add(Files(path=path, name=filename))
        session.commit()

#-------------------------------------------------------------------------------

def populate_lampflash():

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(Files.name.like('%lampflash%')).\
                                outerjoin(Lampflash, Files.id == Lampflash.file_id).\
                                filter(Lampflash.file_id == None)]


    ### Multiprocessing for this
    for f_key, full_filename in files_to_add:
        print(full_filename)

        with fits.open(full_filename) as hdu:

            date = hdu[1].header['EXPSTART']
            proposid = hdu[0].header['PROPOSID']
            detector = hdu[0].header['DETECTOR']
            opt_elem = hdu[0].header['OPT_ELEM']
            cenwave = hdu[0].header['CENWAVE']
            fppos = hdu[0].header['FPPOS']
            fpoffset = fppos - 3
            lamptab_name = hdu[0].header['LAMPTAB'].split('$')[-1]

            for i,line in enumerate(hdu[1].data):
                segment = line['SEGMENT']
                x_shift = line['SHIFT_DISP']
                y_shift = line['SHIFT_XDISP']
                found = line['SPEC_FOUND']

                correction = fppos_shift(lamptab_name,
                                         segment,
                                         opt_elem,
                                         cenwave,
                                         fpoffset)
                x_shift -= correction

                x_shift = round(x_shift, 5)
                y_shift = round(y_shift, 5)
                print(date, proposid, detector, opt_elem, cenwave, fppos, lamptab_name, i, x_shift, y_shift, found)
                session.add(Lampflash(date=date,
                                      proposid=proposid,
                                      detector=detector,
                                      opt_elem=opt_elem,
                                      cenwave=cenwave,
                                      fppos=fppos,
                                      lamptab=lamptab_name,
                                      flash=i,
                                      x_shift=x_shift,
                                      y_shift=y_shift,
                                      found=found,
                                      file_id=f_key))
            session.commit()

#-------------------------------------------------------------------------------

def populate_primary_headers():

    files_to_add = [(result.id, os.path.join(result.path, result.name))
                        for result in session.query(Files).\
                                filter(Files.name.like('%_raw%')).\
                                outerjoin(Headers, Files.id == Headers.file_id).\
                                filter(Headers.file_id == None)]


    ### Multiprocessing for this
    for f_key, full_filename in files_to_add:
        print(full_filename)

        with fits.open(full_filename) as hdu:
            session.add(Headers(filetype=hdu[0].header['filetype'],
                               instrume=hdu[0].header['instrume'],
                               rootname=hdu[0].header['rootname'],
                               imagetyp=hdu[0].header['imagetyp'],
                               targname=hdu[0].header['targname'],
                               ra_targ=hdu[0].header['ra_targ'],
                               dec_targ=hdu[0].header['dec_targ'],
                               proposid=hdu[0].header['proposid'],
                               qualcom1=hdu[0].header['qualcom1'],
                               qualcom2=hdu[0].header['qualcom2'],
                               qualcom3=hdu[0].header['qualcom3'],
                               quality=hdu[0].header['quality'],
                               cal_ver=hdu[0].header['cal_ver'],
                               obstype=hdu[0].header['obstype'],
                               obsmode=hdu[0].header['obsmode'],
                               exptype=hdu[0].header['exptype'],
                               detector=hdu[0].header['detector'],
                               segment=hdu[0].header['segment'],
                               detecthv=hdu[0].header['detecthv'],
                               life_adj=hdu[0].header['life_adj'],
                               fppos=hdu[0].header['fppos'],
                               exp_num=hdu[0].header['exp_num']
                               cenwave=hdu[0].header['cenwave'],
                               aperture=hdu[0].header['aperture'],
                               opt_elem=hdu[0].header['opt_elem'],
                               shutter=hdu[0].header['shutter'],
                               extended=hdu[0].header['extended'],
                               obset_id=hdu[0].header['obset_id'],
                               asn_id=hdu[0].header['asn_id'],
                               asn_tab=hdu[0].header['asn_tab'],
                               file_id=f_key))
            session.commit()

#-------------------------------------------------------------------------------

if __name__ == "__main__":
    with open('../configure.yaml', 'r') as f:
        settings = yaml.load(f)

    Base.metadata.create_all(engine)

    print(settings)
    #insert_files(**settings)
    #populate_lampflash()
    populate_primary_headers()
