import os
import dask
import re
import crds
import numpy as np
import warnings
import abc

from glob import glob
from astropy.io import fits
from typing import Sequence, Union, List, Dict, Any

from . import SETTINGS

FILES_SOURCE = SETTINGS['filesystem']['source']


class FileDataInterface(dict):
    """Partial implementation for classes used to get data from COS FITS files that subclasses the python dictionary."""
    def __init__(self):
        super().__init__(self)

    @abc.abstractmethod
    def get_header_data(self, *args, **kwargs):
        """Get header data."""
        pass

    @abc.abstractmethod
    def get_table_data(self, *args, **kwargs):
        """Get table data."""
        pass


class FileData(FileDataInterface):
    """Class that acts as a dictionary, but with a constructor that grabs FITS file info from typical COS data
    products.
    """
    def __init__(self, filename: str, header_keywords: Sequence, header_extensions: Sequence,
                 spt_suffix: str = 'spt.fits.gz', spt_keywords: Sequence = None, spt_extensions: Sequence = None,
                 data_keywords: Sequence = None, data_extensions: Sequence = None,
                 header_defaults: Dict[str, Any] = None, reference_request: Dict[str, Dict[str, list]] = None):
        """Initialize and create the possible corresponding spt file name."""
        super().__init__()

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

        self._convert_bytes_to_strings()

    def _convert_bytes_to_strings(self):
        """Convert byte-string arrays to strings. This affects reference files in particular, but can also be an issue
        for older COS datatypes.
        """
        for key, value in self.items():
            if isinstance(value, np.ndarray):
                if value.dtype in ['S3', 'S4']:
                    self[key] = value.astype(np.unicode_)

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
        """Get table data from the TableHDU."""
        for key, ext in zip(data_keywords, data_extensions):
            if key in self:
                self[f'{key}_{ext}'] = hdu[ext].data[key]

            else:
                self[key] = hdu[ext].data[key]

    @staticmethod
    def _get_match_values(hdu: fits.HDUList, match_list: list):
        """Get match key values from the input data."""
        return {key: hdu[0].header[key] for key in match_list}

    @staticmethod
    def _get_reference_table(hdu: fits.HDUList, reference_name: str) -> Union[fits.fitsrec.FITS_rec, None]:
        """Locate and read the requested reference file."""
        # noinspection PyUnresolvedReferences
        reference_path = crds.locate_file(hdu[0].header[reference_name].split('$')[-1], 'hst')

        # Check for gzipped files
        if not os.path.exists(reference_path):
            reference_path += '.gz'

        if not os.path.exists(reference_path):
            return

        try:  # Some older reference files actually have bad formats for some columns and are unreadable.
            return fits.getdata(reference_path)

        except ValueError:
            return

    def _get_matching_values(self, match_values: dict, reference_table: fits.fitsrec.FITS_rec, request: dict,
                             reference_name: str):
        """Find the row in the reference file data that corresponds to the values provided in match_values."""
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
                f'\nAvailable columns: {reference_table.names}'
            )

        for column in request['columns']:
            if column in self:
                try:
                    self[f'{column}_{reference_name}'] = np.array(reference_table[column])  # No masked arrays

                except KeyError:
                    self[f'{column}_{reference_name}'] = np.zeros(1)

            else:
                try:
                    self[column] = np.array(reference_table[column])

                except KeyError:
                    self[column] = np.zeros(1)

    def get_reference_data(self, hdu: fits.HDUList, reference_request: Dict[str, Dict[str, list]]):
        """Get data from requested reference files."""
        for reference in reference_request.keys():
            request = reference_request[reference]

            ref_data = self._get_reference_table(hdu, reference)

            if ref_data is not None:  # Unreadable reference files are set to empty numpy arrays
                match_values = self._get_match_values(hdu, request['match'])

                self._get_matching_values(match_values, ref_data, request, reference)

            else:
                for column in request['columns']:
                    if column in self:
                        self[f'{column}_{reference}'] = np.zeros(1)

                    else:
                        self[column] = np.zeros(1)


class JitterFileData(FileDataInterface):
    """Class that acts as a dictionary, but gets data from COS Jitter Files."""
    def __init__(self, filename: str, hdu: fits.HDUList, hdu_index: int, primary_hdr_keywords: Sequence[str],
                 extension_hdr_keywords: Sequence[str], data_keywords: Sequence[str], get_expstart: bool = True):
        super().__init__()

        self.setdefault('EXPSTART', 0)
        self.setdefault('EXPTYPE', 'N/A')

        if hdu_index == 0:
            raise ValueError('The hdu_index must be greater than 0.')

        self.update({'FILENAME': filename})
        self.get_header_data(hdu, hdu_index, primary_hdr_keywords, extension_hdr_keywords)

        if get_expstart:
            self.get_expstart()

        self.get_table_data(hdu, hdu_index, data_keywords)

    def get_expstart(self):
        """Get the EXPSTART from a corresponding 'raw' file."""
        possible_files = ('rawacq.fits.gz', 'rawtag.fits.gz', 'rawtag_a.fits.gz', 'rawtag_b.fits.gz')

        # jitter file rootnames are identical to typical rootnames apart from the last character
        exposure = self['EXPNAME'].strip('j') + 'q'

        for possible_file in possible_files:
            co_file = os.path.join(os.path.dirname(self['FILENAME']), f'{exposure}_{possible_file}')

            try:
                with fits.open(co_file) as co:
                    self['EXPSTART'] = co[1].header['EXPSTART']
                    self['EXPTYPE'] = co[0].header['EXPTYPE']

            except FileNotFoundError:
                continue

    def get_header_data(self, hdu: fits.HDUList, hdu_index: int, primary_hdr_keywords: Sequence[str],
                        extension_hdr_keywords: Sequence[str]):
        """Get data from the header(s)."""
        for key in primary_hdr_keywords:
            self[key] = hdu[0].header[key]

        for key in extension_hdr_keywords:
            self[key] = hdu[hdu_index].header[key]

    @staticmethod
    def _remove_bad_values(input_array):
        """Remove any placeholder entries from the jitter data arrays."""
        return input_array[input_array < 1e30]  # If data recording is interrupted, the software adds a placeholder

    def get_table_data(self, hdu: fits.HDUList, hdu_index: int, data_keywords: Sequence[str]):
        for column in data_keywords:
            self[column] = self._remove_bad_values(hdu[hdu_index].data[column])

    def reduce_to_stat(self, description: dict):
        supported = ('mean', 'std', 'max')

        for key, stats in description.items():
            for stat in stats:
                if stat not in supported:
                    raise ValueError(f'{stat} not one of {supported}. Please select a statistic from {supported}.')

                if stat == 'mean':
                    self[f'{key}_{stat}'] = self[key].mean()

                if stat == 'std':
                    self[f'{key}_{stat}'] = self[key].std()

                if stat == 'max':
                    self[f'{key}_{stat}'] = self[key].max()

            del self[key]


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
    results_as_list = [file for file_list in results for file in
                       file_list]  # Unpack list of lists into one list

    return results_as_list


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


# TODO: This takes forever.. may need to optimize
def get_jitter_data(jitter_files: List[str], primary_hdr_keywords: Sequence[str], extension_hdr_keywords: Sequence[str],
                    data_keywords: Sequence[str], get_expstart: bool = True, reduce_to_stats: dict = None):
    """Get data from COS Jitter Files in parallel. Optionally get a corresponding EXPSTART and reduce specified data
    keys to a representative statistic instead of returning the entire array.
    """

    @dask.delayed
    def _get_jitter_data(jitter_file, *args, **kwargs):
        """Get specified data from jitter_file."""
        description = kwargs.pop('reduce_to_stats')
        get_expstart_ = kwargs.get('get_expstart')

        try:
            jitter_results = []

            with fits.open(jitter_file) as jit:
                # Loop through data extensions, skipping 0; association jitters represent multiple exposures
                for i in range(1, len(jit)):
                    jitter_data = JitterFileData(jitter_file, jit, i, *args, **kwargs)

                    # If retrieving a corresponding EXPSTART, skip jitter files where a corresponding raw file was
                    # not found.
                    if get_expstart_ and jitter_data['EXPSTART'] == 0:
                        continue

                    if description:
                        try:
                            jitter_data.reduce_to_stat(description)

                        except ValueError:   # Sometimes, the jitter file is empty
                            continue

                    jitter_results.append(jitter_data)

            return jitter_results

        except OSError as e:
            warnings.warn(f'Bad file found: {jitter_file}\n{str(e)}', Warning)

            return

    delayed_results = [
        _get_jitter_data(
            jitter_file,
            primary_hdr_keywords,
            extension_hdr_keywords,
            data_keywords,
            get_expstart=get_expstart,
            reduce_to_stats=reduce_to_stats
        ) for jitter_file in jitter_files
    ]

    # Each jitter file will result in a list; need to unpack that list
    return [
        item for sublist in dask.compute(*delayed_results, scheduler='multiprocessing') if sublist is not None
        for item in sublist
    ]
