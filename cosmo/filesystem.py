import os
import dask
import re
import crds

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
                 header_defaults: Dict[str, Any] = None, reference_request: dict = None):
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
            condition = (
                    'reference' in reference_request and 'columns' in reference_request and 'match' in reference_request
            )

            if not condition:
                raise ValueError('reference_request requires "reference", "columns", and "match" keys.')

            if not isinstance(reference_request['columns'], list):
                raise TypeError('"columns" value in reference_request must be a list')

            if not isinstance(reference_request['match'], list):
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
    def _get_reference_table(hdu: fits.HDUList, reference_name) -> Table:
        # noinspection PyUnresolvedReferences
        cos_mapping = crds.rmap.get_cached_mapping('hst_cos.imap')

        reference_path = cos_mapping.locate_file(hdu[0].header[reference_name].split('$')[-1])
        return Table.read(reference_path)

    def get_reference_data(self, hdu: fits.HDUList, reference_request: dict):
        ref_data = self._get_reference_table(hdu, reference_request['reference'])
        match_values = self._get_match_values(hdu, reference_request['match'])

        for key, value in match_values.items():
            ref_data = ref_data[ref_data[key] == value]

        if not len(ref_data):
            raise ValueError(
                f'A matching row could not be determined with the given parameters: {reference_request["match"]}'
                f'\nAvailable columns: {ref_data.keys()}'
            )

        for column in reference_request['columns']:
            if column in self:
                self[f'{column}_ref'] = ref_data[column].data

            else:
                self[column] = ref_data[column].data


def get_file_data(fitsfiles: List[str], keywords: Sequence, extensions: Sequence, spt_keywords: Sequence = None,
                  spt_extensions: Sequence = None, data_keywords: Sequence = None,
                  data_extensions: Sequence = None, header_defaults: Dict[str, Any] = None) -> List[dict]:
    @dask.delayed
    def _get_file_data(fitsfile: str, *args, **kwargs) -> Union[FileData, None]:
        """Get specified data from a fitsfile and optionally its corresponding spt file."""
        try:
            return FileData(fitsfile, *args, **kwargs)

        except (ValueError, OSError):
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
            header_defaults=header_defaults
        ) for fitsfile in fitsfiles
    ]

    return [item for item in dask.compute(*delayed_results, scheduler='multiprocessing') if item is not None]
