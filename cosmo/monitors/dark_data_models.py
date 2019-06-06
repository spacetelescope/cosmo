import pandas as pd
import os

from monitorframe.monitor import BaseDataModel

from cosmo.filesystem import FileDataFinder
from cosmo import FILES_SOURCE
from cosmo.monitor_helpers import explode_df


class DarkDataModel(BaseDataModel):

    def get_data(self):
        header_keys = (
            'ROOTNAME', 'EXPTIME', 'SEGMENT', 'EXPSTART'
        )
        header_extensions = (0, 1, 0, 1)

        data_keys = ('TIME', 'LATITUDE', 'LONGITUDE', 'PHA', 'XCORR', 'YCORR', 'TIME')
        data_extensions = ('timeline', 'timeline', 'timeline', 'events', 'events', 'events', 'events')

        results = []
        program_id = ['15533/', '14940/', '14520/', '14436/', '13968/', '13521/', '13121/', '12716/', '12423/', '11895/']
        for prog_id in program_id:
            print(prog_id)
            new_files_source = os.path.join(FILES_SOURCE, prog_id)
            finder = FileDataFinder(
                new_files_source,
                '*corrtag*',
                header_keys,
                header_extensions,
                data_keys=data_keys,
                data_extensions=data_extensions,
                cosmo_layout=False
            )
            results += finder.get_data_from_files()
        df = pd.DataFrame(results)

        return df
