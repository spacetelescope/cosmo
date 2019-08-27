import pandas as pd

from typing import List
from monitorframe.datamodel import BaseDataModel

from ..filesystem import find_files, get_file_data
from ..sms import SMSTable
from .. import SETTINGS

FILES_SOURCE = SETTINGS['filesystem']['source']


def dgestar_to_fgs(results: List[dict]) -> None:
    """Add a FGS key to each row dictionary."""
    for item in results:
        item.update({'FGS': item['DGESTAR'][-2:]})  # The dominant guide star key is the last 2 values in the string


class AcqDataModel(BaseDataModel):
    """Datamodel for Acq files."""
    files_source = FILES_SOURCE
    cosmo_layout = True

    primary_key = 'ROOTNAME'

    def get_new_data(self):
        acq_keywords = (
            'ACQSLEWX', 'ACQSLEWY', 'EXPSTART', 'ROOTNAME', 'PROPOSID', 'OBSTYPE', 'NEVENTS', 'SHUTTER', 'LAMPEVNT',
            'ACQSTAT', 'EXTENDED', 'LINENUM', 'APERTURE', 'OPT_ELEM', 'LIFE_ADJ', 'CENWAVE', 'DETECTOR', 'EXPTYPE'
        )

        acq_extensions = (0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        defaults = {'ACQSLEWX': 0.0, 'ACQSLEWY': 0.0, 'NEVENTS': 0.0, 'LAMPEVNT': 0.0}

        # SPT file header keys, extensions
        spt_keywords, spt_extensions = ('DGESTAR',), (0,)

        files = find_files('*rawacq*', data_dir=self.files_source, cosmo_layout=self.cosmo_layout)

        if self.model is not None:
            currently_ingested = [item.FILENAME for item in self.model.select(self.model.FILENAME)]

            for file in currently_ingested:
                files.remove(file)

        data_results = get_file_data(
            files,
            acq_keywords,
            acq_extensions,
            header_defaults=defaults,
            spt_keys=spt_keywords,
            spt_exts=spt_extensions,
        )

        dgestar_to_fgs(data_results)

        return data_results


class OSMDataModel(BaseDataModel):
    """Data model for all OSM Shift monitors."""
    files_source = FILES_SOURCE
    cosmo_layout = True

    primary_key = 'ROOTNAME'

    def get_new_data(self):
        """Retrieve data."""
        header_keys = (
            'ROOTNAME', 'EXPSTART', 'DETECTOR', 'LIFE_ADJ', 'OPT_ELEM', 'CENWAVE', 'FPPOS', 'PROPOSID', 'OBSET_ID'
        )
        header_extensions = (0, 1, 0, 0, 0, 0, 0, 0, 0)

        data_keys = ('TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT')
        data_extensions = (1, 1, 1, 1)

        files = find_files('*lampflash*', data_dir=self.files_source, cosmo_layout=self.cosmo_layout)

        if self.model is not None:
            currently_ingested = [item.FILENAME for item in self.model.select(self.model.FILENAME)]

            for file in currently_ingested:
                files.remove(file)

        data_results = pd.DataFrame(
            get_file_data(
                files,
                header_keys,
                header_extensions,
                data_keys=data_keys,
                data_exts=data_extensions
            )
        )

        # Remove any rows that have empty data columns
        data_results = data_results.drop(
            data_results[data_results.apply(lambda x: not bool(len(x.SHIFT_DISP)), axis=1)].index.values
        ).reset_index(drop=True)

        # Add tsince data from SMSTable.
        sms_data = pd.DataFrame(
                SMSTable.select(SMSTable.ROOTNAME, SMSTable.TSINCEOSM1, SMSTable.TSINCEOSM2).where(
                    # x << y -> x IN y (y must be a list)
                    SMSTable.ROOTNAME + 'q' << data_results.ROOTNAME.to_list()).dicts()
        )

        # It's possible that there could be a lag in between when the SMS data is updated and when new lampflashes
        # are added.
        # Returning the empty data frame ensures that only files with a match in the SMS data are added...
        # This may not be the best idea
        if sms_data.empty:
            return sms_data

        # Need to add the 'q' at the end of the rootname.. For some reason those are missing from the SMS rootnames
        sms_data.ROOTNAME += 'q'

        # Combine the data from the files with the data from the SMS table with an inner merge between the two.
        # NOTE: this means that if a file does not have a corresponding entry in the SMSTable, it will not be in the
        # dataset used for monitoring.
        merged = pd.merge(data_results, sms_data, on='ROOTNAME')

        return merged
