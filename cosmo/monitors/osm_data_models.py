
from monitorframe.monitor import BaseDataModel

from cosmo.filesystem import FileDataFinder
from cosmo import FILES_SOURCE


class OSMDataModel(BaseDataModel):
    """Data model for all OSM Shift monitors."""
    files_source = FILES_SOURCE
    cosmo_layout = True

    def get_data(self):
        header_keys = (
            'ROOTNAME', 'EXPSTART', 'DETECTOR', 'LIFE_ADJ', 'OPT_ELEM', 'CENWAVE', 'FPPOS', 'PROPOSID', 'OBSET_ID'
        )
        header_extensions = (0, 1, 0, 0, 0, 0, 0, 0, 0)

        data_keys = ('TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT')
        data_extensions = (1, 1, 1, 1)

        # Find data from lampflash files
        finder = FileDataFinder(
            self.files_source,
            '*lampflash*',
            header_keys,
            header_extensions,
            data_keywords=data_keys,
            data_extensions=data_extensions,
            cosmo_layout=self.cosmo_layout
        )

        return finder.get_data_from_files()
