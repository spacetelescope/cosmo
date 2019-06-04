import pandas as pd

from monitorframe.monitor import BaseDataModel

from cosmo.filesystem import FileDataFinder
from cosmo import FILES_SOURCE
from cosmo.monitor_helpers import explode_df


class OSMDataModel(BaseDataModel):
    """Data model for all OSM Shift monitors."""

    def get_data(self):
        header_keys = (
            'ROOTNAME', 'EXPSTART', 'DETECTOR', 'LIFE_ADJ', 'OPT_ELEM', 'CENWAVE', 'FPPOS', 'PROPOSID', 'OBSET_ID'
        )
        header_extensions = (0, 1, 0, 0, 0, 0, 0, 0, 0)

        data_keys = ('TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT')
        data_extensions = (1, 1, 1, 1)

        # Find data from lampflash files
        finder = FileDataFinder(
            FILES_SOURCE,
            '*lampflash*',
            header_keys,
            header_extensions,
            data_keys=data_keys,
            data_extensions=data_extensions
        )

        df = pd.DataFrame(finder.data_from_files())

        return explode_df(df, list(data_keys))
