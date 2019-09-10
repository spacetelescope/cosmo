import os
import datetime
import glob
import re
import pandas as pd

from typing import Union
from itertools import repeat
from peewee import chunked, OperationalError, EXCLUDED

from .sms_db import SMSFileStats, SMSTable, DB
from .. import SETTINGS

SMS_FILE_LOC = SETTINGS['sms']['source']


class SMSFile:
    """Class that encapsulates the SMS file data."""
    _db = DB
    _patterns = {
        'ROOTNAME': r'l[a-z0-9]{7}',  # String of 7 alpha-numeric characters after 'l'
        'PROPOSID': r'(?<=l[a-z0-9]{7} )\d{5}',  # String of 5 digits occurring after a rootname.
        # Exposure: Group of three sets of alpha(caps)-numeric characters
        'EXPOSURE': r'(?<= )[A-Z0-9]{3} [A-Z0-9]{2} [A-Z0-9]{2}(?= \d{2} )',
        'DETECTOR': r'(?<= )NUV|FUV(?= )',  # Either NUV or FUV
        'OPMODE': r'ACQ\/\S{5,6}|TIME\-TAG|ACCUM',  # ACQ followed by 5 or 6 characters or TIME-TAG or ACCUM
        'EXPTIME': r'(?<= )\d+\.\d{1}(?= )',  # Float with one decimal place followed and preceded by a space
        'EXPSTART': r'\d{4}\.\d{3}:\d{2}:\d{2}:\d{2}',
        # fuvhvstate: After expstart, either 6 spaces, or HV+(3 or 4 characters) or 2 sets of 3 digits separated by
        # '/' followed by a single space
        'FUVHVSTATE': r'(?<=\d{4}\.\d{3}:\d{2}:\d{2}:\d{2} ) {6}|HV[a-zA-Z]{3,4}|\d{3}\/\d{3}(?= )',
        # aperture: Tuple of (aperture, ap_pos or empty depending on format of the sms file)
        'APERTURE': r'(PSA|BOA|WCA|FCA|RELATIVE|REL) (\w{1}|\s+)',
        'osm': r'(NCM1|G130M|G140L|G160M|NCM1FLAT)\s+(-----|MIRRORA|MIRRORB|G\d{3}M|G\d{3}L)',  # tuple (osm1, osm2)
        # cenwave_fp_t1_t2: Tuple of (cenwave, fpoffset, tsince1, tsince2)
        'cenwave_fp_t1_t2': r'(?<= )(0|\d{4}|\d{3}) ( 0|-1|-2|-3| 1)\s+(\d{1,6})\s+(\d{1,6})'
    }

    # Keywords and dtypes for the sms_db table
    _table_keys = {
        'ROOTNAME': str,
        'PROPOSID': int,
        'EXPOSURE': str,
        'DETECTOR': str,
        'OPMODE': str,
        'EXPTIME': float,
        'EXPSTART': str,
        'FUVHVSTATE': str,
        'APERTURE': str,
        'OSM1POS': str,
        'OSM2POS': str,
        'CENWAVE': int,
        'FPPOS': int,
        'TSINCEOSM1': float,
        'TSINCEOSM2': float
    }

    # Keys that should return a single item
    _single_value_keys = ['ROOTNAME', 'PROPOSID', 'DETECTOR', 'OPMODE', 'EXPTIME', 'EXPSTART']

    # Keys that return a tuple of items
    _grouped_value_keys = [
        'FUVHVSTATE',
        'APERTURE',
        'OSM1POS',
        'OSM2POS',
        'CENWAVE',
        'FPPOS',
        'TSINCEOSM1',
        'TSINCEOSM2'
    ]

    def __init__(self, smsfile: str):
        """Initialize and ingest the sms file data."""
        self.datetime_format = '%Y-%m-%d %H:%M:%S'
        self.filename = smsfile
        self.file_id = os.path.basename(self.filename).split('.')[0]
        self.sms_id = self.file_id[:6]   # The SMS id is the first 6 digits of the filename
        self.version = self.file_id[6:]  # The SMS version is the last 2 (or sometimes 3) digits of the filename
        self.ingest_date = datetime.datetime.today()

        self._data = self.ingest_smsfile()
        self.data: pd.DataFrame = pd.DataFrame(self._data).astype(self._table_keys)

    @property
    def _single_value_patterns(self) -> dict:
        """Return a dictionary subset of the patterns dictionary for patterns that return a single value."""
        return {key: self._patterns[key] for key in self._single_value_keys}

    def ingest_smsfile(self) -> dict:
        """Collect data from the SMS file."""
        data = {}

        # Initialize keys that require breakdown of grouped matches
        for key in self._grouped_value_keys:
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
                for key, pattern in self._single_value_patterns.items()
            }
        )

        # Ingest EXPOSURE
        data['EXPOSURE'] = [''.join(item.split()) for item in re.findall(self._patterns['EXPOSURE'], sms_string)]

        # Ingest fuvhvstate since some values are empty
        for item in re.findall(self._patterns['FUVHVSTATE'], sms_string):
            data['FUVHVSTATE'].append(item if item.strip() else 'N/A')

        # Ingest aperture matches
        for item in re.findall(self._patterns['APERTURE'], sms_string):
            data['APERTURE'].append(' '.join(item).strip())

        # Ingest osm1 and osm2 positions
        for item in re.findall(self._patterns['osm'], sms_string):
            osm1pos, osm2pos = item
            data['OSM1POS'].append(osm1pos)
            data['OSM2POS'].append(osm2pos if osm2pos.strip('-') else 'N/A')  # if osm2 isn't used, the value is -----

        # Ingest cenwave, fppos, tsinceosm1, and tsinceosm2
        for item in re.findall(self._patterns['cenwave_fp_t1_t2'], sms_string):
            cenwave, fpoffset, tsinceosm1, tsinceosm2 = item
            fppos = int(fpoffset) + 3  # fpoffset is relative to the third position

            data['CENWAVE'].append(cenwave)
            # noinspection PyTypeChecker
            data['FPPOS'].append(fppos)
            data['TSINCEOSM1'].append(tsinceosm1)
            data['TSINCEOSM2'].append(tsinceosm2)

        # Add the "sms id"
        data.update({'FILEID': list(repeat(self.file_id, len(data['ROOTNAME'])))})

        return data

    def insert_to_db(self):
        """Insert ingested SMS data into the database tables."""
        new_record = {
            'SMSID': self.sms_id,
            'VERSION': self.version,
            'FILEID': self.file_id,
            'FILENAME': self.filename,
            'INGEST_DATE': self.ingest_date.strftime(self.datetime_format)
        }

        # Insert into the file stats table
        with self._db.atomic():
            if not SMSFileStats.table_exists():
                SMSFileStats.create_table()
                SMSFileStats.insert(new_record).execute()

            else:
                SMSFileStats.insert(new_record).on_conflict(
                    action='update',
                    update=new_record,
                    conflict_target=[SMSFileStats.SMSID],
                    where=(EXCLUDED.VERSION > SMSFileStats.VERSION)
                ).execute()

        # Insert data into sms data table
        row_oriented_data = self.data.to_dict(orient='row')

        with self._db.atomic():
            if not SMSTable.table_exists():
                SMSTable.create_table()

                for batch in chunked(row_oriented_data, 100):
                    SMSTable.insert_many(batch).execute()

            else:
                # For new data with an existing table, need to check whether or not new records are replacing old ones.
                for row in row_oriented_data:
                    SMSTable.insert(**row).on_conflict(
                        action='update',
                        update=row,
                        conflict_target=[SMSTable.EXPOSURE],
                        # Update if the FILEID is greater. This is only possible if exposures appear in a later SMS
                        # or a new SMS version is available.
                        where=(EXCLUDED.FILEID_id > SMSTable.FILEID_id)
                    ).execute()


class SMSFinder:
    """Class for finding sms files in the specified filesystem and the database."""
    _sms_pattern = r'\A\d{6}[a-z]{1}[a-z0-9]{1}'

    def __init__(self, source: str = SMS_FILE_LOC):
        self.currently_ingested = None

        self.filesource = source
        if not os.path.exists(self.filesource):
            raise OSError(f'source directory, {self.filesource} does not exist.')

        # Find the most recent ingest date from the database; This is inefficient, but the db is small enough that it
        # doesn't matter.

        try:  # If the table doesn't exist, don't set _currently_ingested
            self.currently_ingested = pd.DataFrame(list(SMSFileStats.select().dicts()))
        except OperationalError:
            pass

        self.all_sms = self.find_all()
        self._grouped_results = self.all_sms.groupby('is_new')

    @property
    def new_sms(self) -> Union[pd.Series, None]:
        """Return only the sms files that were determined as new."""
        try:
            return self._grouped_results.get_group(True)

        except KeyError:
            return

    @property
    def old_sms(self) -> Union[pd.Series, None]:
        """Return only the sms files there were not determined as new."""
        try:
            return self._grouped_results.get_group(False)

        except KeyError:
            return

    @staticmethod
    def _filter_l_exp_files(files_list):
        """Filter l-exp files in the file list to eliminate those that also have a txt file copy."""
        txt_files = [file for file in files_list if file.endswith('.txt')]
        l_exp_files = [file for file in files_list if file.endswith('.l-exp')]

        filtered_lexp = [file for file in l_exp_files if file.strip('.l-exp') + '.txt' not in txt_files]

        return txt_files + filtered_lexp

    def find_all(self) -> pd.DataFrame:
        """Find all SMS files from the source directory. Determine if the file is 'new'."""
        results = {'smsfile': [], 'is_new': [], 'sms_id': [], 'version': []}
        all_files = glob.glob(os.path.join(self.filesource, '*'))

        # Filter out l-exp files that have txt counterparts
        reduced_files = self._filter_l_exp_files(all_files)

        for file in reduced_files:
            # There may be other files or directories in the sms source directory
            match = re.findall(self._sms_pattern, os.path.basename(file))
            if match and os.path.isfile(file):
                name = match[0]  # findall returns a list. There should only be one match per file
                sms_id = name[:6]
                version = name[6:]

                results['version'].append(version)
                results['sms_id'].append(sms_id)
                results['smsfile'].append(file)
                results['is_new'].append(self._is_new(file))

        data = pd.DataFrame(results)

        # Filter the dataframe for the most recent version of the files per file id
        data = data.iloc[
            [
                index for _, group in data.groupby('sms_id')
                for index in group[group.version == group.version.max()].index.values
            ]
        ].reset_index(drop=True)

        # Raise an error if no sms files were found. Otherwise this will break other stuff in an obscure way
        if data.empty:
            raise OSError('No SMS files were found in the source directory.')

        return data

    def _is_new(self, smsfile):
        """Determine if an sms file is considered 'new'. New is defined as any file that is not in the database
        """
        if self.currently_ingested is None or self.currently_ingested.empty:
            return True  # If there are no ingested files, they're all new

        return smsfile not in self.currently_ingested.FILENAME.values

    def ingest_files(self):
        """Add new sms data to the database."""
        if self.new_sms is not None:
            for sms in self.new_sms.smsfile:
                smsfile = SMSFile(sms)
                smsfile.insert_to_db()
