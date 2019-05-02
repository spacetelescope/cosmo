import os
import dask
import re

from glob import glob
from astropy.io import fits


class FileFinder:
    def __init__(self, source_dr, file_pattern, keywords, extensions,
                 spt_keywords=None,
                 spt_extensions=None,
                 exptype=None):

        self.source = source_dr
        self.files = find_files(file_pattern, data_dir=source_dr)
        self.keywords = keywords
        self.extensions = extensions
        self.spt_keywords = spt_keywords
        self.spt_extensions = spt_extensions
        self.exptype = exptype

        self._check_lengths(self.keywords, self.extensions)

        if self.spt_keywords is not None:
            self._check_lengths(spt_keywords, spt_extensions)

    @staticmethod
    def _check_lengths(keywords, extensions):
        try:
            assert len(keywords) == len(extensions), 'Input keywords and extensions must be the same length'

        except TypeError:
            raise TypeError('Input keywords must have accompanying extensions and must be the same length')

    def data_from_files(self):
        delayed_results = []

        for file in self.files:
            if self.spt_keywords is not None:
                path, name = os.path.split(file)
                spt_name = '_'.join([name.split('_')[0], 'spt.fits.gz'])
                spt_file = os.path.join(path, spt_name)

                if not os.path.exists(spt_file):
                    continue

            else:
                spt_file = None

            delayed_results.append(
                get_keyword_values(
                    file,
                    self.keywords,
                    self.extensions,
                    self.exptype,
                    spt_file,
                    self.spt_keywords,
                    self.spt_extensions
                )
            )

        return [item for item in dask.compute(*delayed_results, scheduler='multiprocessing') if item is not None]


@dask.delayed
def get_keyword_values(fitsfile, keys, exts, exp_type=None, spt_file=None, spt_keys=None, spt_exts=None):
    results = dict()
    with fits.open(fitsfile) as file:
        if exp_type and file[0].header['EXPTYPE'] != exp_type:
            return

        results.update({key: file[ext].header[key] for key, ext in zip(keys, exts)})

    if spt_file:
        with fits.open(spt_file) as spt:
            results.update({key: spt[ext].header[key] for key, ext in zip(spt_keys, spt_exts)})

    return results


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
