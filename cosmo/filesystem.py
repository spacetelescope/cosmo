import os
import dask
import re
import crds
import numpy as np
import warnings

from glob import glob
from astropy.io import fits
from astropy.table import Table
from typing import Sequence, Union, List, Dict, Any

from . import SETTINGS

FILES_SOURCE = SETTINGS['filesystem']['source']


def find_files(file_pattern: str, data_dir: str = FILES_SOURCE, cosmo_layout: bool = True) -> list:
    """Find COS data files from a source directory. The default is the cosmo data directory. If another source is
    used, it's assumed that that directory only contains the data files.
    """
    if not os.path.exists(data_dir):
        raise OSError(f'data_dir, {data_dir} does not exist.')

    if not cosmo_layout:
        return glob(os.path.join(data_dir, file_pattern))

    pattern = r'\d{5}'  # Match subdirectories named with program ids.
    programs = os.listdir(data_dir)

    # Glob files from all directories in parallel
    result = [
        dask.delayed(glob)(os.path.join(data_dir, program, file_pattern))
        for program in programs if re.match(pattern, program)
    ]

    results = dask.compute(result)[0]
    results_as_list = [file for file_list in results for file in file_list]   # Unpack list of lists into one list

    return results_as_list


class FileData(dict):
    """Class that acts as a dictionary, but with a constructor that grabs FITS file info"""

    # noinspection PyMissingConstructor
    def __init__(self, filename: str, header_keywords: Sequence, header_extensions: Sequence,
                 spt_suffix: str = 'spt.fits.gz', spt_keywords: Sequence = None, spt_extensions: Sequence = None,
                 data_keywords: Sequence = None, data_extensions: Sequence = None,
                 header_defaults: Dict[str, Any] = None, reference_request: Dict[str, Dict[str, list]] = None):
        """Initialize and create the possible corresponding spt file name."""
        super().__init__(self)

        spt_file = self._create_spt_filename(filename, spt_suffix)

        self.update({'FILENAME': filename})

        # Check that all keywords/extensions have corresponding extensions/keywords and that they're the same length
        if len(header_keywords) != len(header_extensions):
            raise ValueError('header_keywords and header_extensions must be the same length.')

        if bool(spt_keywords or spt_extensions):
            if not (spt_keywords and spt_extensions):
                raise ValueError('spt_keywords and spt_extensions must be used together.')

            if len(spt_keywords) != len(spt_extensions):
                raise ValueError('spt_keywords and spt_extensions must be the same length.')

        if bool(data_keywords or data_extensions):
            if not (data_keywords and data_extensions):
                raise ValueError('data_keywords and data_extensions must be used together.')

            if len(data_keywords) != len(data_extensions):
                raise ValueError('data_keywords and data_extensions must be the same length.')

        if reference_request:
            for reference in reference_request.keys():
                if not ('match' in reference_request[reference] and 'columns' in reference_request[reference]):
                    raise ValueError('reference_requests require "columns", and "match" keys.')

                if not isinstance(reference_request[reference]['columns'], list):
                    raise TypeError('"columns" value in reference_request must be a list')

                if not isinstance(reference_request[reference]['match'], list):
                    raise TypeError('"match" value in reference_request must be a list')

        with fits.open(filename) as hdu:
            self.get_header_data(hdu, header_keywords, header_extensions, header_defaults)

            if data_keywords:
                self.get_table_data(hdu, data_keywords, data_extensions)

            if reference_request:
                self.get_reference_data(hdu, reference_request)

        if spt_keywords:
            self.get_spt_header_data(spt_file, spt_keywords, spt_extensions)

    @staticmethod
    def _create_spt_filename(filename: str, spt_suffix: str) -> Union[str, None]:
        """Create an spt filename based on the input filename."""
        path, name = os.path.split(filename)
        spt_name = '_'.join([name.split('_')[0], spt_suffix])
        spt_file = os.path.join(path, spt_name)

        if os.path.exists(spt_file):
            return spt_file

        return

    def get_header_data(self, hdu: fits.HDUList, header_keywords: Sequence, header_extensions: Sequence,
                        header_defaults: dict = None):
        """Get header data."""
        for key, ext in zip(header_keywords, header_extensions):
            if header_defaults is not None and key in header_defaults:
                self[key] = hdu[ext].header.get(key, default=header_defaults[key])

            else:
                self[key] = hdu[ext].header[key]

    def get_spt_header_data(self, spt_file: str, spt_keywords: Sequence, spt_extensions: Sequence):
        """Open the spt file and collect requested data."""
        with fits.open(spt_file) as spt:
            for key, ext in zip(spt_keywords, spt_extensions):
                self[key] = spt[ext].header[key]

    def get_table_data(self, hdu: fits.HDUList, data_keywords: Sequence, data_extensions: Sequence):
        """Get table data."""
        for key, ext in zip(data_keywords, data_extensions):
            if key in self:
                self[f'{key}_{ext}'] = hdu[ext].data[key]

            else:
                self[key] = hdu[ext].data[key]

    @staticmethod
    def _get_match_values(hdu: fits.HDUList, match_list: list):
        return {key: hdu[0].header[key] for key in match_list}

    @staticmethod
    def _get_reference_table(hdu: fits.HDUList, reference_name) -> Union[Table, None]:
        # noinspection PyUnresolvedReferences
        reference_path = crds.locate_file(hdu[0].header[reference_name].split('$')[-1], 'hst')

        try:  # Some older reference files actually have bad formats for some columns and are unreadable.
            return Table.read(reference_path)

        except ValueError:
            return

    def _get_matching_values(self, match_values, reference_table, request, reference_name):
        for key, value in match_values.items():
            try:
                if isinstance(value, str):  # Different "generations" of ref files stored strings in different ways...
                    reference_table = reference_table[
                        (reference_table[key] == value) |
                        (reference_table[key] == value + '   ') |
                        (reference_table[key] == value.encode())
                    ]

                else:
                    reference_table = reference_table[reference_table[key] == value]

            except KeyError:
                continue

        if not len(reference_table):
            raise ValueError(
                f'A matching row could not be determined with the given parameters: {request["match"]}'
                f'\nAvailable columns: {reference_table.keys()}'
            )

        for column in request['columns']:
            if column in self:
                try:
                    self[f'{column}_{reference_name}'] = reference_table[column].data

                except KeyError:
                    self[f'{column}_{reference_name}'] = np.array([])

            else:
                try:
                    self[column] = reference_table[column].data

                except KeyError:
                    self[column] = np.array([])

    def get_reference_data(self, hdu: fits.HDUList, reference_request: Dict[str, Dict[str, list]]):
        for reference in reference_request.keys():
            request = reference_request[reference]

            ref_data = self._get_reference_table(hdu, reference)

            if ref_data is not None:  # Unreadable reference files are set to empty numpy arrays
                match_values = self._get_match_values(hdu, request['match'])

                self._get_matching_values(match_values, ref_data, request, reference)

            else:
                for column in request['columns']:
                    if column in self:
                        self[f'{column}_{reference}'] = np.array([])

                    else:
                        self[column] = np.array([])


def get_file_data(fitsfiles: List[str], keywords: Sequence, extensions: Sequence, spt_keywords: Sequence = None,
                  spt_extensions: Sequence = None, data_keywords: Sequence = None,
                  data_extensions: Sequence = None, header_defaults: Dict[str, Any] = None,
                  reference_request: dict = None) -> List[dict]:
    @dask.delayed
    def _get_file_data(fitsfile: str, *args, **kwargs) -> Union[FileData, None]:
        """Get specified data from a fitsfile and optionally its corresponding spt file."""
        try:
            return FileData(fitsfile, *args, **kwargs)

        # Occasionally there are empty or corrupt files that will throw an OSError; This shouldn't break the process,
        # but users should be warned.
        except OSError as e:
            warnings.warn(f'Bad file found: {fitsfile}\n{str(e)}', Warning)

            return

    delayed_results = [
        _get_file_data(
            fitsfile,
            keywords,
            extensions,
            spt_keywords=spt_keywords,
            spt_extensions=spt_extensions,
            data_keywords=data_keywords,
            data_extensions=data_extensions,
            header_defaults=header_defaults,
            reference_request=reference_request
        ) for fitsfile in fitsfiles
    ]

    return [item for item in dask.compute(*delayed_results, scheduler='multiprocessing') if item is not None]
