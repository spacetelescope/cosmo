import os
import dask
import re

from glob import glob
from astropy.io import fits
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


class FileData:
    """Class that represents a single file's data."""
    def __init__(self, filename: str, hdu: fits.HDUList, header_keywords: Sequence, header_extensions: Sequence,
                 spt_suffix: str = 'spt.fits.gz', spt_keywords: Sequence = None, spt_extensions: Sequence = None,
                 data_keywords: Sequence = None, data_extensions: Sequence = None,
                 header_defaults: Dict[str, Any] = None):
        """Initialize and create the possible corresponding spt file name."""
        self.filename = filename
        self.hdu = hdu
        self.header_keywords = header_keywords
        self.header_extensions = header_extensions
        self.header_defaults = header_defaults
        self.spt_keywords = spt_keywords
        self.spt_extensions = spt_extensions
        self.data_keywords = data_keywords
        self.data_extensions = data_extensions
        self.spt_suffix = spt_suffix
        self.spt_file = self._create_spt_filename()
        self.data = {'FILENAME': self.filename}

        # Check that all keywords/extensions have corresponding extensions/keywords and that they're the same length
        if len(self.header_keywords) != len(self.header_extensions):
            raise ValueError('header_keywords and header_extensions must be the same length.')

        if bool(self.spt_keywords or self.spt_extensions):
            if not (self.spt_keywords and self.spt_extensions):
                raise ValueError('spt_keywords and spt_extensions must be used together.')

            if len(self.spt_keywords) != len(self.spt_extensions):
                raise ValueError('spt_keywords and spt_extensions must be the same length.')

        if bool(self.data_keywords or self.data_extensions):
            if not (self.data_keywords and self.data_extensions):
                raise ValueError('data_keywords and data_extensions must be used together.')

            if len(self.data_keywords) != len(self.data_extensions):
                raise ValueError('data_keywords and data_extensions must be the same length.')

        self.get_file_data()

    def _create_spt_filename(self) -> Union[str, None]:
        """Create an spt filename based on the input filename."""
        path, name = os.path.split(self.filename)
        spt_name = '_'.join([name.split('_')[0], self.spt_suffix])
        spt_file = os.path.join(path, spt_name)

        if os.path.exists(spt_file):
            return spt_file

        return

    def get_file_data(self):
        self.get_header_data()

        if self.spt_keywords:
            self.get_spt_header_data()

        if self.data_keywords:
            self.get_table_data()

    def get_header_data(self):
        """Get header data."""
        header_values = {}
        for key, ext in zip(self.header_keywords, self.header_extensions):
            if self.header_defaults is not None and key in self.header_defaults:
                header_values[key] = self.hdu[ext].header.get(key, default=self.header_defaults[key])

            else:
                header_values[key] = self.hdu[ext].header[key]

        self.data.update(header_values)

    def get_spt_header_data(self):
        """Open the spt file and collect requested data."""
        with fits.open(self.spt_file) as spt:
            self.data.update({key: spt[ext].header[key] for key, ext in zip(self.spt_keywords, self.spt_extensions)})

    def get_table_data(self):
        """Get table data."""
        self.data.update({key: self.hdu[ext].data[key] for key, ext in zip(self.data_keywords, self.data_extensions)})


def get_file_data(fitsfiles: List[str], keys: Sequence, exts: Sequence, spt_keys: Sequence = None,
                  spt_exts: Sequence = None, data_exts: Sequence = None,
                  data_keys: Sequence = None, header_defaults: Dict[str, Any] = None) -> List[dict]:
    @dask.delayed
    def _get_file_data(fitsfile: str, *args, **kwargs) -> Union[dict, None]:
        """Get specified data from a fitsfile and optionally its corresponding spt file."""
        with fits.open(fitsfile, memmap=False) as file:
            file_data = FileData(fitsfile, file, *args, **kwargs)

        return file_data.data

    delayed_results = [
        _get_file_data(
            fitsfile,
            keys,
            exts,
            spt_keywords=spt_keys,
            spt_extensions=spt_exts,
            data_keywords=data_keys,
            data_extensions=data_exts,
            header_defaults=header_defaults
        ) for fitsfile in fitsfiles
    ]

    return [item for item in dask.compute(*delayed_results, scheduler='multiprocessing') if item is not None]
