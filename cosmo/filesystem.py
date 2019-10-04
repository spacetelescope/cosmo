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


class FileData(dict):
    """Class that acts as a dictionary, but with a constructor that grabs FITS file info"""

    # noinspection PyMissingConstructor
    def __init__(self, filename: str, header_keywords: Sequence, header_extensions: Sequence,
                 spt_suffix: str = 'spt.fits.gz', spt_keywords: Sequence = None, spt_extensions: Sequence = None,
                 data_keywords: Sequence = None, data_extensions: Sequence = None,
                 header_defaults: Dict[str, Any] = None):
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

        with fits.open(filename) as hdu:
            self.get_header_data(hdu, header_keywords, header_extensions, header_defaults)

            if data_keywords:
                self.get_table_data(hdu, data_keywords, data_extensions)

        if spt_keywords:
            self.get_spt_header_data(spt_file, spt_keywords, spt_extensions)

    @staticmethod
    def _create_spt_filename(filename, spt_suffix) -> Union[str, None]:
        """Create an spt filename based on the input filename."""
        path, name = os.path.split(filename)
        spt_name = '_'.join([name.split('_')[0], spt_suffix])
        spt_file = os.path.join(path, spt_name)

        if os.path.exists(spt_file):
            return spt_file

        return

    def get_header_data(self, hdu, header_keywords, header_extensions, header_defaults=None):
        """Get header data."""
        for key, ext in zip(header_keywords, header_extensions):
            if header_defaults is not None and key in header_defaults:
                self.update({key: hdu[ext].header.get(key, default=header_defaults[key])})

            else:
                self.update({key: hdu[ext].header[key]})

    def get_spt_header_data(self, spt_file, spt_keywords, spt_extensions):
        """Open the spt file and collect requested data."""
        with fits.open(spt_file) as spt:
            self.update({key: spt[ext].header[key] for key, ext in zip(spt_keywords, spt_extensions)})

    def get_table_data(self, hdu, data_keywords, data_extensions):
        """Get table data."""
        self.update({key: hdu[ext].data[key] for key, ext in zip(data_keywords, data_extensions)})


def get_file_data(fitsfiles: List[str], keywords: Sequence, extensions: Sequence, spt_keywords: Sequence = None,
                  spt_extensions: Sequence = None, data_keywords: Sequence = None,
                  data_extensions: Sequence = None, header_defaults: Dict[str, Any] = None) -> List[dict]:
    @dask.delayed
    def _get_file_data(fitsfile: str, *args, **kwargs) -> Union[FileData, None]:
        """Get specified data from a fitsfile and optionally its corresponding spt file."""
        try:
            return FileData(fitsfile, *args, **kwargs)

        except ValueError:
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
