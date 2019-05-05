import os
import dask
import re

from glob import glob
from astropy.io import fits


class FileDataFinder:
    def __init__(
            self, source_dr, file_pattern, keywords, extensions,
            spt_keywords=None, spt_extensions=None, exptype=None, data_extensions=None, data_keys=None
    ):

        self.source = source_dr
        self.files = find_files(file_pattern, data_dir=source_dr)
        self.header_keywords = keywords
        self.header_extensions = extensions
        self.spt_keywords = spt_keywords
        self.spt_extensions = spt_extensions
        self.exptype = exptype
        self.data_extensions = data_extensions
        self.data_keys = data_keys

        self._check_lengths(self.header_keywords, self.header_extensions)

        if self.spt_keywords is not None or self.spt_extensions is not None:
            self._check_lengths(self.spt_keywords, self.spt_extensions)

        if self.data_keys is not None or self.data_extensions is not None:
            self._check_lengths(self.data_keys, self.data_extensions)

    @staticmethod
    def _check_lengths(keywords, extensions):
        try:
            assert len(keywords) == len(extensions), 'Input keywords and extensions must be the same length'

        except TypeError:
            raise TypeError('Input keywords must have accompanying extensions and must be the same length')

    def data_from_files(self):
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
                    self.data_keys
                )
            )

        return [item for item in dask.compute(*delayed_results, scheduler='multiprocessing') if item is not None]


class FileData:
    def __init__(
            self, filename, hdu, keys, exts, spt_keys=None, spt_exts=None, data_ext=None, data_keys=None
    ):
        self.filename = filename
        self.hdu = hdu
        self.header_keys = keys
        self.header_exts = exts
        self.spt_keys = spt_keys
        self.spt_exts = spt_exts
        self.data_keys = data_keys
        self.data_exts = data_ext
        self.data = dict()

    def get_spt_header_data(self):
        path, name = os.path.split(self.filename)
        spt_name = '_'.join([name.split('_')[0], 'spt.fits.gz'])
        spt_file = os.path.join(path, spt_name)

        if not os.path.exists(spt_file):
            return

        with fits.open(spt_file) as spt:
            self.data.update({key: spt[ext].header[key] for key, ext in zip(self.spt_keys, self.spt_exts)})

    def get_header_data(self):
        self.data.update({key: self.hdu[ext].header[key] for key, ext in zip(self.header_keys, self.header_exts)})

    def get_file_data(self):
        self.data.update({key: self.hdu[ext].data[key] for key, ext in zip(self.data_keys, self.data_exts)})


@dask.delayed
def get_file_data(fitsfile, keys, exts, exp_type=None, spt_keys=None, spt_exts=None, data_ext=None, data_keys=None):
    with fits.open(fitsfile) as file:
        if exp_type and file[0].header['EXPTYPE'] != exp_type:
            return

        filedata = FileData(fitsfile, file, keys, exts, spt_keys, spt_exts, data_ext, data_keys)
        filedata.get_header_data()

        if data_keys:
            filedata.get_file_data()

    if spt_keys:
        filedata.get_spt_header_data()

    return filedata.data


def find_files(file_pattern, data_dir):
    pattern = r'\d{5}'
    programs = os.listdir(data_dir)

    result = [
        dask.delayed(glob)(os.path.join(data_dir, program, file_pattern))
        for program in programs if re.match(pattern, program)
    ]

    results = dask.compute(result)[0]
    results_as_list = [file for file_list in results for file in file_list]

    return results_as_list
