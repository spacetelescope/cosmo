import pandas as pd

from monitorframe.datamodel import BaseDataModel
from typing import List

from ..sms import SMSTable
from ..filesystem import FileDataFinder
from .. import SETTINGS

FILES_SOURCE = SETTINGS['filesystem']['source']


def get_lampflash_data(source: str, cosmo_layout: bool) -> List[dict]:
    """Retrieve required data for the OSM Drift and Shift monitors from the lampflash files."""
    header_keys = (
        'ROOTNAME', 'EXPSTART', 'DETECTOR', 'LIFE_ADJ', 'OPT_ELEM', 'CENWAVE', 'FPPOS', 'PROPOSID', 'OBSET_ID'
    )
    header_extensions = (0, 1, 0, 0, 0, 0, 0, 0, 0)

    data_keys = ('TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT')
    data_extensions = (1, 1, 1, 1)

    # Find data from lampflash files
    finder = FileDataFinder(
        source,
        '*lampflash*',
        header_keys,
        header_extensions,
        data_keywords=data_keys,
        data_extensions=data_extensions,
        cosmo_layout=cosmo_layout
    )

    return finder.get_data_from_files()


class OSMShiftDataModel(BaseDataModel):
    """Data model for all OSM Shift monitors."""
    files_source = FILES_SOURCE
    cosmo_layout = True

    def get_new_data(self):
        """Retrieve data."""
        return get_lampflash_data(self.files_source, self.cosmo_layout)


class OSMDriftDataModel(BaseDataModel):
    """Data model for all OSM Drift monitors."""
    files_source = FILES_SOURCE
    cosmo_layout = True

    def get_new_data(self):
        """Retrieve data."""
        # Get lampflash data from the files
        file_data = pd.DataFrame(get_lampflash_data(self.files_source, self.cosmo_layout))

        # Grab data from the SMSTable.
        sms_data = pd.DataFrame(
            list(
                SMSTable.select(SMSTable.ROOTNAME, SMSTable.TSINCEOSM1, SMSTable.TSINCEOSM2)
                .where(SMSTable.ROOTNAME + 'q' << file_data.ROOTNAME.to_list())  # x << y -> x IN y (y must be a list)
                .dicts()
            )
        )

        # Need to add the 'q' at the end of the rootname.. For some reason those are missing from the SMS rootnames
        sms_data.ROOTNAME += 'q'

        # Combine the data from the files with the data from the SMS table with an inner merge between the two.
        # NOTE: this means that if a file does not have a corresponding entry in the SMSTable, it will not be in the
        # dataset used for monitoring.
        merged = pd.merge(file_data, sms_data, on='ROOTNAME')

        return merged
