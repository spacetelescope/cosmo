import os
import datetime
import glob
import re
import pandas as pd
import numpy as np

from itertools import repeat
from peewee import chunked

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


class SMSFinder:
    sms_pattern = r'\d{6}[a-z]\d{1}'  # TODO: determine if we also need to resolve "specially" named SMS files.

    def __init__(self, source: str = SMS_FILE_LOC):
        self.filesource = source
        if not os.path.exists(self.filesource):
            raise OSError(f'source directory, {self.filesource} does not exist.')

        self.today = datetime.datetime.today()

        # Find the most recent ingest date from the database; This is inefficient, but the db is small enough that it
        # doesn't matter.
        self._currently_ingested = pd.DataFrame(list(SMSFileStats.select().dicts()))
        self.last_ingest_date = self._currently_ingested.ingest_date.max()

        self._all_sms_results = self.find_all()
        self._grouped_results = self._all_sms_results.groupby('is_new')

    @property
    def all_sms(self):
        return self._all_sms_results.smsfile

    @property
    def new_sms(self):
        return self._grouped_results.get_group(True).smsfile.values

    @property
    def old_sms(self) -> np.ndarray:
        try:
            return self._grouped_results.get_group(False).smsfile.values
        except KeyError:
            return np.array([])

    @property
    def ingested_sms(self):
        return self._currently_ingested.filename.values

    def find_all(self) -> pd.DataFrame:
        """Find all SMS files from the source directory. Determine if the file is 'new'."""
        results = {'smsfile': [], 'is_new': []}

        for file in glob.glob(os.path.join(self.filesource, '*')):
            # There may be other files or directories in the sms source directory
            if re.match(self.sms_pattern, os.path.basename(file)) and os.path.isfile(file):
                results['smsfile'].append(file)
                results['is_new'].append(self._is_new(file, self.last_ingest_date))

        data = pd.DataFrame(results)

        # Raise an error if no sms files were found. Otherwise this will break other stuff in an obscure way
        if data.empty:
            raise OSError('No SMS files were found in the source directory.')

        return data

    def _is_new(self, smsfile, last_ingest_date):
        """Determine if an sms file is considered 'new'. New is defined as any file that was added between the last
        ingestion date and the current date.
        """
        filestats = os.stat(smsfile)

        # st_mtime = time of most recent modification. This gives us the date that the file was added to the directory,
        # but also might be subject to error in the case of someone modifying these files somehow. They shouldn't be
        # though..
        file_birthday = datetime.datetime.fromtimestamp(filestats.st_mtime)

        if last_ingest_date < file_birthday <= self.today:
            return True

        return False


def ingest_sms_data(file_source: str = SMS_FILE_LOC, cold_start: bool = False):
    """Add new sms data to the database. Defaults to finding and adding new files to the database. Cold start attempts
    to add all sms data found to the database.
    """
    sms_files = SMSFinder(file_source)  # Find the sms files

    if cold_start:
        sms_to_add = sms_files.all_sms

    else:
        sms_to_add = sms_files.new_sms

    # TODO: An error should be raised if a cold start is attempted on a populated database; Add an option to drop?
    all_sms_data = [SMSFile(file) for file in sms_to_add]

    for sms in all_sms_data:
        sms.insert_to_db()
