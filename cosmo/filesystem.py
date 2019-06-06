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
    def __init__(self, source_dir: str, file_pattern: str, keywords: Sequence, extensions: Sequence, **kwargs):
        self.source = source_dir
        self.search_pattern = file_pattern
        self.header_keywords = keywords
        self.header_extensions = extensions

        self.spt_keywords = kwargs.get('spt_keywords', None)
        self.spt_extensions = kwargs.get('spt_extensions', None)
        self.exptype = kwargs.get('exptype', None)
        self.data_extensions = kwargs.get('data_extensions', None)
        self.data_keywords = kwargs.get('data_keys', None)

        cosmo_layout = kwargs.get('cosmo_layout', True)

        # Find data files
        self.files = find_files(self.search_pattern, self.source, cosmo_layout=cosmo_layout)

        # Check that all keywords/extensions have corresponding extensions/keywords and that they're the same length
        for keys, exts in zip(
                [self.header_keywords, self.spt_keywords, self.data_keywords],
                [self.header_extensions, self.spt_extensions, self.data_extensions]
        ):
            if keys is not None and exts is None or exts is not None and keys is None:
                raise ValueError('All keyword lists must be accompanied by extension lists')

            if keys is not None and exts is not None:
                if len(keys) != len(exts):
                    raise ValueError('All keywords lists must be the same length as the accompanying extension lists')

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


class FileData:
    def __init__(
            self, filename, hdu, keys, exts, spt_keys=None, spt_exts=None, data_ext=None, data_keys=None
    ):
        self.filename = filename
        self.spt_file = self._define_spt_filename()
        self.hdu = hdu
        self.header_keys = keys
        self.header_exts = exts
        self.spt_keys = spt_keys
        self.spt_exts = spt_exts
        self.data_keys = data_keys
        self.data_exts = data_ext
        self.data = {}

    def _define_spt_filename(self):
        path, name = os.path.split(self.filename)
        spt_name = '_'.join([name.split('_')[0], 'spt.fits.gz'])
        spt_file = os.path.join(path, spt_name)

        if not os.path.exists(spt_file):
            return

        return spt_file

    def get_spt_header_data(self):
        with fits.open(self.spt_file) as spt:
            for key, ext in zip(self.spt_keys, self.spt_exts):
                self.data.update({key if key not in self.data else f'{key}_{ext}': spt[ext].header[key]})

    def get_header_data(self):
        for key, ext in zip(self.header_keys, self.header_exts):
            self.data.update({key if key not in self.data else f'{key}_{ext}': self.hdu[ext].header[key]})

    def get_table_data(self):
        for key, ext in zip(self.data_keys, self.data_exts):
            self.data.update({key if key not in self.data else f'{key}_{ext}': self.hdu[ext].data[key]})



@dask.delayed
def get_file_data(fitsfile: str, keys: Sequence, exts: Sequence,
                  exp_type: Sequence = None,
                  spt_keys: Sequence = None,
                  spt_exts: Sequence = None,
                  data_ext: Sequence = None,
                  data_keys: Sequence = None) -> Union[dict, None]:
    """Get specified data from a fitsfile and optionally its corresponding spt file."""
    with fits.open(fitsfile, memmap=False) as file:
        if exp_type and file[0].header['EXPTYPE'] != exp_type:
            return

        filedata = FileData(fitsfile, file, keys, exts, spt_keys, spt_exts, data_ext, data_keys)
        filedata.get_header_data()

        if data_keys:
            filedata.get_table_data()

    if spt_keys:
        filedata.get_spt_header_data()

    return filedata.data


def find_files(file_pattern: str, data_dir: str = FILES_SOURCE, cosmo_layout: bool = True) -> list:
    """Find COS data files from a source directory. The default is the cosmo data directory. If another source is used,
    it's assumed that that directory only contains the data files.
    """
    if cosmo_layout:
        pattern = r'\d{5}'
        programs = os.listdir(data_dir)

        result = [
            dask.delayed(glob)(os.path.join(data_dir, program, file_pattern))
            for program in programs if re.match(pattern, program)
        ]

        results = dask.compute(result)[0]
        results_as_list = [file for file_list in results for file in file_list]

        return results_as_list

    return glob(os.path.join(data_dir, file_pattern))

