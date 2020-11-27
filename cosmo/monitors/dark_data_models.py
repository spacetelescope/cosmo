import pandas as pd
import os
from glob import glob
from monitorframe.datamodel import BaseDataModel

from ..filesystem import find_files, data_from_exposures
from .. import SETTINGS

FILES_SOURCE = SETTINGS['filesystem']['source']


class DarkDataModel(BaseDataModel):
    cosmo_layout = False
    program_id = ['15771', '15533/', '14940/', '14520/', '14436/', '13968/', '13521/', '13121/', '12716/', '12423/',
                  '11895/']
    # subdir_pattern = '?????'

    def get_new_data(self):
        header_request = {
            0: ['ROOTNAME', 'SEGMENT'],
            1: ['EXPTIME', 'EXPSTART']
        }

        table_request = {
            1: ['PHA', 'XCORR', 'YCORR', 'TIME'],
            3: ['TIME', 'LATITUDE', 'LONGITUDE']
        }

        files = []

        for prog_id in self.program_id:

            new_files_source = os.path.join(FILES_SOURCE, prog_id)
            files += find_files('*corrtag*', data_dir=new_files_source)

        if self.model is not None:
            currently_ingested = [item.FILENAME for item in self.model.select(self.model.FILENAME)]

            for file in currently_ingested:
                files.remove(file)

        if not files:   # No new files
            return pd.DataFrame()

        data_results = data_from_exposures(
            files,
            header_request=header_request,
            table_request=table_request
        )

        return data_results
