import numpy as np

from typing import List
from monitorframe.datamodel import BaseDataModel

from ..filesystem import FileDataFinder
from .. import SETTINGS

FILES_SOURCE = SETTINGS['filesystem']['source']


def dgestar_to_fgs(results: List[dict]) -> None:
    """Add a FGS key to each row dictionary."""
    for item in results:
        item.update({'FGS': item['DGESTAR'][-2:]})  # The dominant guide star key is the last 2 values in the string


def get_acq_data(data_dir: str, acq_keys: tuple, acq_extensions: tuple, spt_keys: tuple, spt_extensions: tuple,
                 exptype: str, cosmo_layout: bool) -> List[dict]:
    """Get data from all rawacq files and their corresponding spts."""
    finder = FileDataFinder(
        data_dir,
        '*rawacq*',
        acq_keys,
        acq_extensions,
        spt_keywords=spt_keys,
        spt_extensions=spt_extensions,
        exptype=exptype,
        cosmo_layout=cosmo_layout
    )

    data_results = finder.get_data_from_files()

    if 'DGESTAR' in spt_keys:
        dgestar_to_fgs(data_results)

    return data_results


class AcqPeakdModel(BaseDataModel):
    """Datamodel for the Acq Peakd Monitor."""
    files_source = FILES_SOURCE
    cosmo_layout = True

    def get_new_data(self):
        # ACQ file header keys, extensions
        acq_keywords = ('ACQSLEWX', 'EXPSTART', 'LIFE_ADJ', 'ROOTNAME', 'PROPOSID', 'OPT_ELEM', 'CENWAVE')
        acq_extensions = (0, 1, 0, 0, 0, 0, 0)

        # SPT file header keys, extensions
        spt_keywords, spt_extensions = ('DGESTAR',), (0,)

        # Data collected as a list of dictionaries (row-oriented)
        data_results = get_acq_data(
            self.files_source,
            acq_keywords,
            acq_extensions,
            spt_keywords,
            spt_extensions,
            'ACQ/PEAKD',
            self.cosmo_layout
        )

        return data_results


class AcqPeakxdModel(BaseDataModel):
    """Datamodel for the Acq Peakxd Monitor."""
    files_source = FILES_SOURCE
    cosmo_layout = True

    def get_new_data(self):
        # ACQ file header keys, extensions
        acq_keywords = ('ACQSLEWY', 'EXPSTART', 'LIFE_ADJ', 'ROOTNAME', 'PROPOSID', 'OPT_ELEM', 'CENWAVE')
        acq_extensions = (0, 1, 0, 0, 0, 0, 0)

        # SPT file header keys, extensions
        spt_keywords, spt_extensions = ('DGESTAR',), (0,)

        # Data collected as a list of dictionaries (row-oriented)
        data_results = get_acq_data(
            self.files_source,
            acq_keywords,
            acq_extensions,
            spt_keywords,
            spt_extensions,
            'ACQ/PEAKXD',
            self.cosmo_layout
        )

        return data_results


class AcqImageModel(BaseDataModel):
    """Datamodel for the ACQIMAGE monitors and V2V3 monitor."""
    files_source = FILES_SOURCE
    cosmo_layout = True

    def get_new_data(self):

        def detector_to_v2v3(slew_x, slew_y):
            """Detector coordinates to V2/V3 coordinates."""
            rotation_angle = np.radians(45.0)  # rotation angle in degrees converted to radians
            x_conversion = slew_x * np.cos(rotation_angle)
            y_conversion = slew_y * np.sin(rotation_angle)

            v2 = x_conversion + y_conversion
            v3 = x_conversion - y_conversion

            return v2, v3

        # ACQ keys, extensions
        acq_keywords = (
            'ACQSLEWX', 'ACQSLEWY', 'EXPSTART', 'ROOTNAME', 'PROPOSID', 'OBSTYPE', 'NEVENTS', 'SHUTTER', 'LAMPEVNT',
            'ACQSTAT', 'EXTENDED', 'LINENUM'
        )
        acq_extensions = (0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0)

        # SPT keys, extensions
        spt_keywords, spt_extensions = ('DGESTAR',), (0,)

        # Data collected as a list of dictionaries (row-oriented)
        data_results = get_acq_data(
            self.files_source,
            acq_keywords,
            acq_extensions,
            spt_keywords,
            spt_extensions,
            'ACQ/IMAGE',
            self.cosmo_layout
        )

        # Add v2 and v3 coordinate columns
        for item in data_results:
            v2_values, v3_values = detector_to_v2v3(item['ACQSLEWX'], item['ACQSLEWY'])
            item.update({'V2SLEW': v2_values, 'V3SLEW': v3_values})

        return data_results
