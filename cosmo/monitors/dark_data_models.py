import pandas as pd
import os

from monitorframe.datamodel import BaseDataModel

from ..filesystem import find_files, get_file_data
from .. import SETTINGS

FILES_SOURCE = SETTINGS['filesystem']['source']


class DarkDataModel(BaseDataModel):
    cosmo_layout = False
    program_id = ['15771', '15533/', '14940/', '14520/', '14436/', '13968/', '13521/', '13121/', '12716/', '12423/',
                  '11895/']

    def get_new_data(self):
        header_keys = (
            'ROOTNAME', 'EXPTIME', 'SEGMENT', 'EXPSTART'
        )
        header_extensions = (0, 1, 0, 1)

        data_keys = ('TIME', 'LATITUDE', 'LONGITUDE', 'PHA', 'XCORR', 'YCORR', 'TIME')
        data_extensions = ('timeline', 'timeline', 'timeline', 'events', 'events', 'events', 'events')

        results = []

        for prog_id in self.program_id:

            new_files_source = os.path.join(FILES_SOURCE, prog_id)
            results += find_files('*corrtag*', data_dir=new_files_source)

        if self.model is not None:
            currently_ingested = [item.FILENAME for item in self.model.select(self.model.FILENAME)]

            for file in currently_ingested:
                results.remove(file)

        if not results:  # No new files
            return pd.DataFrame()

        file_data = get_file_data(
            results,
            header_keys,
            header_extensions,
            data_keywords=data_keys,
            data_extensions=data_extensions
        )

        return file_data
