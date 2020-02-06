import os
import dask
import crds
import numpy as np
import warnings
import abc

from glob import glob
from astropy.io import fits
from typing import Sequence, Union, List, Dict, Any

from . import SETTINGS

FILES_SOURCE = SETTINGS['filesystem']['source']
REQUEST = Dict[int, Sequence[str]]


class FileDataInterface(abc.ABC, dict):
    """Partial implementation for classes used to get data from COS FITS files that subclasses the python dictionary."""
    def __init__(self):
        super().__init__(self)

    @abc.abstractmethod
    def get_header_data(self, hdu: fits.HDUList, header_request: REQUEST, defaults: Dict[str, Any]):
        """Get header data."""
        pass

    @abc.abstractmethod
    def get_table_data(self, hdu: fits.HDUList, table_request: REQUEST):
        """Get table data."""
        pass


class FileData(FileDataInterface):
    """Class that acts as a dictionary, but with a constructor that grabs FITS file info from typical COS data
    products.
    """
    def __init__(self, hdu: fits.HDUList, header_request: REQUEST = None, table_request: REQUEST = None,
                 header_defaults: Dict[str, Any] = None, bytes_to_str: bool = True):
        """Initialize and create the possible corresponding spt file name."""
        super().__init__()

        if header_request:
            self.get_header_data(hdu, header_request, header_defaults)

        if table_request:
            self.get_table_data(hdu, table_request)

        if bytes_to_str:
            self._convert_bytes_to_strings()

    def _convert_bytes_to_strings(self):
        """Convert byte-string arrays to strings."""
        for key, value in self.items():
            if isinstance(value, np.ndarray):
                if value.dtype.char == 'S':
                    self[key] = value.astype(str)

    @classmethod
    def from_file(cls, filename, *args, **kwargs):
        with fits.open(filename) as hdu:
            return cls(hdu, *args, **kwargs)

    def get_header_data(self, hdu: fits.HDUList, header_request: REQUEST, header_defaults: dict = None):
        """Get header data."""
        for ext, keys in header_request.items():
            for key in keys:
                if header_defaults is not None and key in header_defaults:
                    self[key] = hdu[ext].header.get(key, default=header_defaults[key])

                else:
                    self[key] = hdu[ext].header[key]

    def get_table_data(self, hdu: fits.HDUList, table_request: REQUEST):
        """Get table data from the TableHDU."""
        for ext, keys in table_request.items():
            for key in keys:
                if key in self:
                    self[f'{key}_{ext}'] = hdu[ext].data[key]

                else:
                    self[key] = hdu[ext].data[key]

    def combine(self, other, right_name):
        for key, value in other.items():
            if key in self:
                self[f'{right_name}_{key}'] = value

            else:
                self[key] = value


class ReferenceData(FileData):
    def __init__(self, input_hdu: Union[str, fits.HDUList], reference_name: str, match_keys: Sequence[str],
                 header_request: REQUEST = None, table_request: REQUEST = None, header_defaults: Dict[str, Any] = None):
        self.reference = input_hdu[0].header[reference_name].split('$')[-1]
        self.match_keys = match_keys
        self.match_values = self._get_input_match_values(input_hdu)

        path = crds.locate_file(self.reference, 'hst')

        # Check for gzipped files
        if not os.path.exists(path):
            path += '.gz'

        with fits.open(path) as ref:
            super().__init__(ref, header_request, table_request, header_defaults)

    def _get_input_match_values(self, input_hdu: fits.HDUList):
        """Get match key values from the input data."""
        return {key: input_hdu[0].header[key] for key in self.match_keys}

    def _get_matched_table_values(self, reference_table: fits.fitsrec.FITS_rec,
                                  column_names: Sequence[str], extension: int):
        """Find the row in the reference file data that corresponds to the values provided in match_values."""
        for key, value in self.match_values.items():
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
                f'A matching row could not be determined with the given parameters: {self.match_keys}'
                f'\nAvailable columns: {reference_table.names}'
            )

        for column in column_names:
            if column in self:
                column = f'{column}_{extension}'

            try:
                self[column] = np.array(reference_table[column])  # No masked arrays

            except KeyError:
                self[column] = np.zeros(1)

    def get_table_data(self, hdu: fits.HDUList, table_request: REQUEST):
        """Get data from requested reference files."""
        for ext, keys in table_request.items():
            self._get_matched_table_values(hdu[ext].data, keys, ext)


class SPTData(FileData):
    def __init__(self, input_filename: str, header_request: REQUEST = None, table_request: REQUEST = None,
                 header_defaults: Dict[str, Any] = None):
        self.sptfile = self._create_spt_filename(input_filename)

        with fits.open(self.sptfile) as spt:
            super().__init__(spt, header_request, table_request, header_defaults)

    @staticmethod
    def _create_spt_filename(filename: str) -> Union[str, None]:
        """Create an spt filename based on the input filename."""
        exts = filename.split(os.path.extsep)
        file = exts.pop(0)  # first item is the path + file name (sans extensions)

        path, name = os.path.split(file)
        spt_name = '.'.join(['_'.join([name.split('_')[0], 'spt'])] + exts)
        spt_file = os.path.join(path, spt_name)

        if os.path.exists(spt_file):
            return spt_file

        # Try checking for an unzipped version if filename is gzipped
        if 'gz' in exts:
            unzipped = spt_file.strip('.gz')

            if os.path.exists(unzipped):
                return unzipped

        # Try checking for a gzipped version if the filename is not gzipped
        if len(exts) == 1:
            zipped = spt_file + '.gz'

            if os.path.exists(zipped):
                return zipped

        return


class JitterFileData(list):
    """Class that acts as a dictionary, but gets data from COS Jitter Files."""
    def __init__(self, filename: str, primary_header_keys: Sequence[str] = None,
                 ext_header_keys: Sequence[str] = None, table_keys: Sequence[str] = None,
                 get_expstart: bool = True):
        super().__init__(self)
        header_request = None
        table_request = None

        if primary_header_keys is not None:
            header_request = {0: primary_header_keys}

        with fits.open(filename) as hdu:
            for i in range(1, len(hdu)):
                if ext_header_keys is not None:
                    if header_request is not None:
                        header_request.update({i: ext_header_keys})

                    else:
                        header_request = {i: ext_header_keys}

                if table_keys is not None:
                    table_request = {i: table_keys}

                self.append({**FileData(hdu, header_request, table_request), **{'FILENAME': filename}})

        if table_keys is not None:
            self._remove_bad_values(table_keys)

        if get_expstart:
            self.get_expstart()

    def get_expstart(self):
        """Get the EXPSTART from a corresponding 'raw' file."""
        possible_files = ('rawacq.fits.gz', 'rawtag.fits.gz', 'rawtag_a.fits.gz', 'rawtag_b.fits.gz')

        for filedata in self:
            filedata.setdefault('EXPSTART', 0)
            filedata.setdefault('EXPTYPE', 'N/A')

            # jitter file rootnames are identical to typical rootnames apart from the last character
            exposure = filedata['EXPNAME'][:-1] + 'q'

            for possible_file in possible_files:
                co_file = os.path.join(os.path.dirname(filedata['FILENAME']), f'{exposure}_{possible_file}')

                if not os.path.exists(co_file):
                    continue

                with fits.open(co_file) as co:
                    filedata['EXPSTART'] = co[1].header['EXPSTART']
                    filedata['EXPTYPE'] = co[0].header['EXPTYPE']

    def _remove_bad_values(self, table_keys):
        """Remove any placeholder entries from the jitter data arrays."""
        for item in self:
            for key in table_keys:
                item[key] = item[key][item[key] < 1e30]

    def reduce_to_stat(self, description: dict):
        supported = ('mean', 'std', 'max')

        for filedata in self:
            for key, stats in description.items():
                for stat in stats:
                    if stat not in supported:
                        raise ValueError(f'{stat} not one of {supported}. Please select a statistic from {supported}.')

                    if stat == 'mean':
                        filedata[f'{key}_{stat}'] = filedata[key].mean()

                    if stat == 'std':
                        filedata[f'{key}_{stat}'] = filedata[key].std()

                    if stat == 'max':
                        try:
                            filedata[f'{key}_{stat}'] = filedata[key].max()

                        except ValueError:
                            filedata[f'{key}_{stat}'] = np.nan

                del filedata[key]


def find_files(file_pattern: str, data_dir: str = FILES_SOURCE, subdir_pattern: Union[str, None] = '?????') -> list:
    """Find COS data files from a source directory. The default is the cosmo data directory subdirectories layout
    pattern. A different subdirectory pattern can be used or
    """
    if subdir_pattern:
        return glob(os.path.join(data_dir, subdir_pattern, file_pattern))

    return glob(os.path.join(data_dir, file_pattern))


def get_exposure_data(filename: str, header_request: REQUEST = None, table_request: REQUEST = None,
                      header_defaults: Dict[str, Any] = None, spt_header_request: REQUEST = None,
                      spt_table_request: REQUEST = None, spt_header_defaults: Dict[str, Any] = None,
                      reference_request: Dict[str, Dict[str, Union[Sequence[str], REQUEST]]] = None):

    try:
        with fits.open(filename) as hdu:
            if header_request or table_request:
                data = FileData(hdu, header_request, table_request, header_defaults)
                data['FILENAME'] = filename

            if reference_request:
                for reference, request in reference_request.items():
                    data.combine(
                        ReferenceData(
                            hdu,
                            reference,
                            request['match_keys'],
                            request.get('header_request', None),
                            request.get('table_request', None),
                            request.get('header_defaults', None)
                        ),
                        reference
                    )

    except OSError as e:
        warnings.warn(f'Bad file found: {filename}\n{str(e)}', Warning)

        return

    if spt_header_request or spt_table_request:
        data.combine(
            SPTData(
                data['FILENAME'],
                spt_header_request,
                spt_table_request,
                spt_header_defaults
            ),
            'spt'
        )

    return data


def get_jitter_data(jitter_file: str, primary_header_keys: Sequence[str] = None,
                    ext_header_keys: Sequence[str] = None, table_keys: Sequence[str] = None,
                    get_expstart: bool = True, reduce_to_stats: Dict[str, Sequence[str]] = None):
    try:
        jit = JitterFileData(jitter_file, primary_header_keys, ext_header_keys, table_keys, get_expstart)

    except OSError as e:
        warnings.warn(f'Bad file found: {jitter_file}\n{str(e)}', Warning)

        return

    if reduce_to_stats is not None:
        jit.reduce_to_stat(reduce_to_stats)

    return jit


def data_from_exposures(fitsfiles: List[str], header_request: REQUEST = None, table_request: REQUEST = None,
                        header_defaults: Dict[str, Any] = None, spt_header_request: REQUEST = None,
                        spt_table_request: REQUEST = None, spt_header_defaults: Dict[str, Any] = None,
                        reference_request: Dict[str, Dict[str, Union[Sequence[str], REQUEST]]] = None):
    delayed_results = [
        dask.delayed(get_exposure_data)(
            file,
            header_request,
            table_request,
            header_defaults,
            spt_header_request,
            spt_table_request,
            spt_header_defaults,
            reference_request
        ) for file in fitsfiles
    ]

    return [item for item in dask.compute(*delayed_results, scheduler='multiprocessing') if item is not None]


def data_from_jitters(jitter_files: List[str], primary_header_keys: Sequence[str] = None,
                      ext_header_keys: Sequence[str] = None, table_keys: Sequence[str] = None,
                      get_expstart: bool = True, reduce_to_stats: Dict[str, Sequence[str]] = None):
    """Get data from COS Jitter Files in parallel. Optionally get a corresponding EXPSTART and reduce specified data
    keys to a representative statistic instead of returning the entire array.
    """
    delayed_results = [
        dask.delayed(get_jitter_data)(
            jitter_file,
            primary_header_keys,
            ext_header_keys,
            table_keys,
            get_expstart,
            reduce_to_stats
        ) for jitter_file in jitter_files
    ]

    # Each jitter file will result in a list; need to unpack that list
    return [
        item for sublist in dask.compute(*delayed_results, scheduler='multiprocessing') if sublist is not None
        for item in sublist
    ]
