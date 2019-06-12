import os
import dask
import re

from glob import glob
from astropy.io import fits
from typing import Sequence, Union

from cosmo import FILES_SOURCE
# TODO: Add the ability to select files based on file creation date:
# filestats = os.stat(<path>)
# t = datetime.datetime.fromtimestamp(filestats.st_birthtime)


class FileDataFinder:
    def __init__(self, source_dir: str, file_pattern: str, header_keywords: Sequence, header_extensions: Sequence,
                 spt_keywords: Sequence = None, spt_extensions: Sequence = None, data_keywords: Sequence = None,
                 data_extensions: Sequence = None, cosmo_layout: bool = True, exptype: str = None):
        self.source = source_dir
        self.search_pattern = file_pattern
        self.header_keywords = header_keywords
        self.header_extensions = header_extensions
        self.spt_keywords = spt_keywords
        self.spt_extensions = spt_extensions
        self.data_keywords = data_keywords
        self.data_extensions = data_extensions
        self.exptype = exptype

        cosmo_layout = cosmo_layout

        # Find data files
        self.files = self.find_files(self.search_pattern, self.source, cosmo_layout=cosmo_layout)

        # Check that all keywords/extensions have corresponding extensions/keywords and that they're the same length
        if len(self.header_keywords) != len(self.header_extensions):
            raise ValueError('header_keywords and header_extensions must be the same lenght.')

        if bool(self.spt_keywords or self.spt_extensions):
            if not(self.spt_keywords and self.spt_extensions):
                raise TypeError('spt_keywords and spt_extensions must be used together.')

            if len(self.spt_keywords) != len(self.spt_extensions):
                raise ValueError('spt_keywords and spt_extensions must be the same length.')

        if bool(self.data_keywords or self.data_extensions):
            if not (self.data_keywords and self.data_extensions):
                raise TypeError('data_keywords and data_extensions must be used together.')

            if len(self.data_keywords) != len(self.data_extensions):
                raise ValueError('data_keywords and data_extensions must be the same length.')

    def get_data_from_files(self):
        """Retrieve specified data from all files that matched the file search pattern."""
        delayed_results = []

        for file in self.files:
            delayed_results.append(
                get_file_data(
                    file,
                    self.header_keywords,
                    self.header_extensions,
                    self.exptype,
                    self.spt_keywords,
                    self.spt_extensions,
                    self.data_extensions,
                    self.data_keywords
                )
            )

        return [item for item in dask.compute(*delayed_results, scheduler='multiprocessing') if item is not None]

    @staticmethod
    def find_files(file_pattern: str, data_dir: str = FILES_SOURCE, cosmo_layout: bool = True) -> list:
        """Find COS data files from a source directory. The default is the cosmo data directory. If another source is
        used, it's assumed that that directory only contains the data files.
        """
        if not os.path.exists(data_dir):
            raise OSError(f'data_dir, {data_dir} does not exist.')

        if not cosmo_layout:
            return glob(os.path.join(data_dir, file_pattern))

        pattern = r'\d{5}'
        programs = os.listdir(data_dir)

        result = [
            dask.delayed(glob)(os.path.join(data_dir, program, file_pattern))
            for program in programs if re.match(pattern, program)
        ]

        results = dask.compute(result)[0]
        results_as_list = [file for file_list in results for file in file_list]

        return results_as_list


class FileData:
    def __init__(self, filename: str, hdu: fits.HDUList, keys: Sequence, exts: Sequence,
                 spt_suffix: str = 'spt.fits.gz', spt_keys: Sequence = None, spt_exts: Sequence = None,
                 data_keys: Sequence = None, data_exts: Sequence = None):
        self.filename = filename
        self.hdu = hdu
        self.header_keys = keys
        self.header_exts = exts
        self.spt_keys = spt_keys
        self.spt_exts = spt_exts
        self.data_keys = data_keys
        self.data_exts = data_exts
        self.spt_suffix = spt_suffix
        self.spt_file = self._create_spt_filename()
        self.data = {}

    def _create_spt_filename(self):
        path, name = os.path.split(self.filename)
        spt_name = '_'.join([name.split('_')[0], self.spt_suffix])
        spt_file = os.path.join(path, spt_name)

        if os.path.exists(spt_file):
            return spt_file

        return

    def get_spt_header_data(self):
        with fits.open(self.spt_file) as spt:
            self.data.update({key: spt[ext].header[key] for key, ext in zip(self.spt_keys, self.spt_exts)})

    def get_header_data(self):
        self.data.update({key: self.hdu[ext].header[key] for key, ext in zip(self.header_keys, self.header_exts)})

    def get_table_data(self):
        self.data.update({key: self.hdu[ext].data[key] for key, ext in zip(self.data_keys, self.data_exts)})


@dask.delayed
def get_file_data(fitsfile: str, keys: Sequence, exts: Sequence, exp_type: Sequence = None, spt_keys: Sequence = None,
                  spt_exts: Sequence = None, data_exts: Sequence = None,
                  data_keys: Sequence = None) -> Union[dict, None]:
    """Get specified data from a fitsfile and optionally its corresponding spt file."""
    with fits.open(fitsfile, memmap=False) as file:
        if exp_type and file[0].header['EXPTYPE'] != exp_type:
            return

        filedata = FileData(
            fitsfile,
            file,
            keys,
            exts,
            spt_keys=spt_keys,
            spt_exts=spt_exts,
            data_keys=data_keys,
            data_exts=data_exts
        )

        filedata.get_header_data()

        if data_keys:
            filedata.get_table_data()

    if spt_keys:
        filedata.get_spt_header_data()

    return filedata.data
