import os
import datetime
import glob
import re
import pandas as pd
import dask

from itertools import repeat
from peewee import chunked
from typing import Union

from .sms_db import SMSFileStats, SMSTable, DB
from .. import SETTINGS

SMS_FILE_LOC = SETTINGS['sms']['source']


class SMSFile:
    db = DB
    rootname_pattern = r'l[a-z0-9]{7}'  # String of 7 alpha-numeric characters after 'l'
    propid_pattern = r'(?<=l[a-z0-9]{7} )\d{5}'  # String of 5 digits occurring after a rootname.
    detector_pattern = r'(?<= )NUV|FUV(?= )'  # Either NUV or FUV
    opmode_pattern = r'ACQ\/\S{5,6}|TIME\-TAG|ACCUM'  # ACQ followed by 5 or 6 characters or TIME-TAG or ACCUM
    exptime_pattern = r'(?<= )\d+\.\d{1}(?= )'  # Float with one decimal place followed and preceded by a space
    expstart_pattern = r'\d{4}\.\d{3}:\d{2}:\d{2}:\d{2}'

    # After expstart, either 6 spaces, or HV+(3 or 4 characters) or 2 sets of 3 digits separated by '/' followed
    # by a single space
    fuvhvstate_pattern = r'(?<=\d{4}\.\d{3}:\d{2}:\d{2}:\d{2} ) {6}|HV[a-zA-Z]{3,4}|\d{3}\/\d{3}(?= )'

    # Tuple of (aperture, ap_pos or empty depending on format of the sms file)
    aperture_pattern = r'(PSA|BOA|WCA|FCA|RELATIVE|REL) (\w{1}|\s+)'
    osm_pattern = r'(NCM1|G130M|G140L|G160M|NCM1FLAT)\s+(-----|MIRRORA|MIRRORB|G\d{3}M|G\d{3}L)'  # tuple (osm1, osm2)

    # Tuple of (cenwave, fpoffset, tsince1, tsince2)
    cenwave_fp_t1_t2_pattern = r'(?<= )(0|\d{4}|\d{3}) ( 0|-1|-2|-3| 1)\s+(\d{1,6})\s+(\d{1,6})'

    # Keywords for the sms_db table
    table_keys = [
        'rootname',
        'proposid',
        'detector',
        'opmode',
        'exptime',
        'expstart',
        'fuvhvstate',
        'aperture',
        'osm1pos',
        'osm2pos',
        'cenwave',
        'fppos',
        'tsinceosm1',
        'tsinceosm2'
    ]

    dtypes = [
        str,
        int,
        str,
        str,
        float,
        str,
        str,
        str,
        str,
        str,
        int,
        int,
        float,
        float
    ]

    def __init__(self, smsfile: str):
        self.datetime_format = '%Y-%m-%d %H:%M:%S'
        self.filename = smsfile

        if not os.path.exists(self.filename):
            raise OSError(f'{self.filename} does not seem to exist.')

        self._data = self.ingest_smsfile()
        self.ingest_date = datetime.datetime.today()

    @property
    def single_value_patterns(self):
        return [
            self.rootname_pattern,
            self.propid_pattern,
            self.detector_pattern,
            self.opmode_pattern,
            self.exptime_pattern,
            self.expstart_pattern,
            self.fuvhvstate_pattern
        ]

    @property
    def single_value_keys(self):
        return ['rootname', 'proposid', 'detector', 'opmode', 'exptime', 'expstart']

    @property
    def grouped_value_keys(self):
        return ['fuvhvstate', 'aperture', 'osm1pos', 'osm2pos', 'cenwave', 'fppos', 'tsinceosm1', 'tsinceosm2']

    @property
    def data(self):
        return pd.DataFrame(self._data).astype({key: dtype for key, dtype in zip(self.table_keys, self.dtypes)})

    def ingest_smsfile(self):
        """Collect data from the SMS file."""
        data = {}

        # Initialize keys that require breakdown of grouped matches
        for key in self.grouped_value_keys:
            data[key] = []

        filtered_lines = []
        with open(self.filename) as sms:
            for i, line in enumerate(sms):
                # Skip the header of the file
                if i <= 5:
                    continue

                # Skip "special" types
                if 'MEMORY' in line or 'ALIGN/OSM' in line or 'ALIGN/APER' in line:
                    continue

                filtered_lines.append(line)

        sms_string = ''.join(filtered_lines)

        # Ingest single-valued matches
        data.update(
            {
                key: re.findall(pattern, sms_string)
                for key, pattern in zip(self.single_value_keys, self.single_value_patterns)
            }
        )

        # Ingest fuvhvstate since some values are empty
        for item in re.findall(self.fuvhvstate_pattern, sms_string):
            data['fuvhvstate'].append(item if item.strip() else 'N/A')

        # Ingest aperture matches
        for item in re.findall(self.aperture_pattern, sms_string):
            data['aperture'].append(' '.join(item).strip())

        # Ingest osm1 and osm2 positions
        for item in re.findall(self.osm_pattern, sms_string):
            osm1pos, osm2pos = item
            data['osm1pos'].append(osm1pos)
            data['osm2pos'].append(osm2pos if osm2pos.strip('-') else 'N/A')

        # Ingest cenwave, fppos, tsinceosm1, and tsinceosm2
        for item in re.findall(self.cenwave_fp_t1_t2_pattern, sms_string):
            cenwave, fpoffset, tsinceosm1, tsinceosm2 = item
            fppos = int(fpoffset) + 3  # fpoffset is relative to the third position
            data['cenwave'].append(cenwave)
            # noinspection PyTypeChecker
            data['fppos'].append(fppos)
            data['tsinceosm1'].append(tsinceosm1)
            data['tsinceosm2'].append(tsinceosm2)

        # Add the filename
        data.update({'filename': list(repeat(self.filename, len(data['rootname'])))})

        return data

    def insert_to_db(self):
        """Insert ingested SMS data into the database tables."""
        # Insert into the file stats table
        if not SMSFileStats.table_exists():
            SMSFileStats.create_table()

        with self.db.atomic():
            SMSFileStats.insert(
                {'filename': self.filename, 'ingest_date': self.ingest_date.strftime(self.datetime_format)}
            ).execute()

        # Insert data into sms data table
        row_oriented_data = self.data.to_dict(orient='row')

        if not SMSTable.table_exists():
            SMSTable.create_table()

        with self.db.atomic():
            for batch in chunked(row_oriented_data, 100):
                SMSTable.insert_many(batch).execute()


def find_all_smsfiles(source: str = SMS_FILE_LOC) -> list:
    """Given a starting directory, find all SMS files."""
    pattern = r'\d{6}[a-z]\d{1}'

    return [file for file in glob.glob(os.path.join(source, '*')) if re.match(pattern, os.path.basename(file))]


def cold_start(file_source: str = SMS_FILE_LOC):
    """Collect and store data from all SMS files. Can only be executed if the database doesn't exist or if the tables
    are empty.
    """
    # TODO: An error should be raised if a cold start is attempted on a populated database; Add an option to drop?
    all_sms_data = [SMSFile(file) for file in find_all_smsfiles(file_source)]

    for sms in all_sms_data:
        sms.insert_to_db()


@dask.delayed
def is_new(file: str, last_ingest: datetime.datetime, today: datetime.datetime) -> Union[str, None]:
    """Determine if an sms file is considered 'new'. New is defined as any file that was added between the last
    ingestion date and the current date.
    """
    filestats = os.stat(file)

    # st_mtime = time of most recent modification. This gives us the date that the file was added to the directory, but
    # also might be subject to error in the case of someone modifying these files somehow. They shouldn't be though..
    file_birthday = datetime.datetime.fromtimestamp(filestats.st_mtime)

    if last_ingest < file_birthday <= today:
        return file

    return


def find_new_sms(source: str = SMS_FILE_LOC) -> list:
    """Find new sms files."""
    # Find the most recent ingest date from the database; This is inefficient, but the db is small enough that it
    # doesn't matter.
    most_recent = SMSFileStats.select().order_by(SMSFileStats.ingest_date.desc())[0].ingest_date
    today = datetime.datetime.today()

    # From the list of all sms files, find the ones that are "new"; again, inefficient, but the number of files is
    # relatively small and none are being opened. Plus parallelizing with dask which might be overkill
    all_sms = find_all_smsfiles(source)
    new = [is_new(sms, most_recent, today) for sms in all_sms]

    return [item for item in dask.compute(*new) if item is not None]
