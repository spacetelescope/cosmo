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

        if not files:  # No new files
            return pd.DataFrame()

        data_results = get_file_data(
            files,
            acq_keywords,
            acq_extensions,
            header_defaults=defaults,
            spt_keywords=spt_keywords,
            spt_extensions=spt_extensions,
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

        if not files:   # No new files
            return pd.DataFrame()

        data_results = pd.DataFrame(
            get_file_data(
                files,
                header_keys,
                header_extensions,
                data_keywords=data_keys,
                data_extensions=data_extensions
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


class JitterDataModel(BaseDataModel):
    files_source = FILES_SOURCE
    cosmo_layout = True

    def get_new_data(self):
        header_keys = ('EXPNAME', 'PROPOSID', 'CONFIG')
        header_extensions = (0, 0, 0)

        data_keys = ('SI_V2_AVG', 'SI_V3_AVG')




        # def create_jitter_filenames(source, cosmo_layout):
        #     jitter_files = []
        #     associations = find_files('*asn*', data_dir=source, cosmo_layout=cosmo_layout)
        #     for asn in associations:
        #         jitter_files.append(asn.replace('asn.fits.gz', 'jit.fits.gz'))
        #
        #     return jitter_files
        #
        # def get_expstart(jitter_path, jitter_hdu):
        #     rawacq = 'rawacq.fits.gz'
        #     rawtag = 'rawtag.fits.gz'
        #     rawtag_a = 'rawtag_a.fits.gz'
        #     rawtag_b = 'rawtag_b.fits.gz'
        #
        #     exposure = jitter_hdu.header['EXPNAME'].strip('j') + 'q'
        #
        #     for possible_file in [rawacq, rawtag, rawtag_a, rawtag_b]:
        #         co_file = os.path.join(jitter_path, f'{exposure}_{possible_file}')
        #
        #         try:
        #             return fits.getval(co_file, 'EXPSTART', 1)
        #
        #         except FileNotFoundError:
        #             continue
        #     return
        #
        # @dask.delayed
        # def get_data(jitter_file):
        #     print(f'Getting data for {jitter_file}')
        #
        #     results = []
        #     try:
        #         with fits.open(jitter_file) as jit:
        #             for i in range(len(jit)):
        #
        #                 if i == 0:
        #                     continue
        #
        #                 data = {
        #                     'PROPOSID': jit[0].header['PROPOSID'],
        #                     'CONFIG': jit[0].header['CONFIG'],
        #                     'EXPSTART': get_expstart(os.path.dirname(jitter_file), jit[i]),
        #                     'ROOTNAME': jit[i].header['EXPNAME'],
        #                     'TIME': jit[i].data['Seconds'],
        #                     'SI_V2_AVG': jit[i].data['SI_V2_AVG'],
        #                     'SI_V3_AVG': jit[i].data['SI_V3_AVG']
        #                 }
        #
        #                 if data['ROOTNAME'] is None:
        #                     continue
        #
        #                 if len(data['SI_V2_AVG']) == 0:
        #                     continue
        #
        #                 results.append(data)
        #
        #     except (OSError, FileNotFoundError):
        #         return
        #
        #     if results:
        #         return results
        #
        #     return
        #
        # def get_jitter_data(source=FILES_SOURCE, cosmo_layout=True, short=True):
        #     if short:
        #         jitter_files = []
        #         for program in ['15626', '15091', '15627', '15643', '15533', '15538', '15656', '15680']:
        #             new_source = os.path.join(source, program)
        #             jitter_files += create_jitter_filenames(source, cosmo_layout)
        #
        #         jitter_files = sorted(jitter_files)
        #
        #     else:
        #         jitter_files = sorted(create_jitter_filenames(source, cosmo_layout))
        #
        #     print(f'Found {len(jitter_files)} jitter files\n\n')
        #
        #     delayed_results = [get_data(file) for file in jitter_files]
        #     results = dask.compute(*delayed_results)
        #     as_list = []
        #     for item in results:
        #         if item is not None:
        #             for sub in item:
        #                 if sub is not None:
        #                     as_list.append(sub)
        #
        #     return pd.DataFrame(as_list).drop_duplicates('ROOTNAME', keep=False).dropna()
        #
        # def plot_jitter(jitter_data):
        #     jitter_data['hover_text'] = [
        #         '<br>'.join(str(row).replace('\n', '<br>').split('<br>')[:-1])
        #         for _, row in jitter_data[['ROOTNAME', 'PROPOSID', 'CONFIG']].iterrows()
        #     ]
        #     exploded = explode_df(jitter_data, ['TIME', 'SI_V2_AVG', 'SI_V3_AVG'])
        #     exploded = exploded[(exploded.SI_V2_AVG < 1e30) & (exploded.SI_V3_AVG < 1e30)]
        #     abstime = absolute_time(exploded)
        #
        #     figure = make_subplots(2, 1, shared_xaxes=True)
        #
        #     figure.add_trace(
        #         go.Scattergl(
        #             x=abstime.to_datetime(),
        #             y=exploded.SI_V2_AVG,
        #             mode='markers',
        #             hovertext=exploded.hover_text,
        #             name='V2 Jitter'
        #         ),
        #         row=1,
        #         col=1
        #     )
        #
        #     figure.add_trace(
        #         go.Scattergl(
        #             x=abstime.to_datetime(),
        #             y=exploded.SI_V3_AVG,
        #             mode='markers',
        #             hovertext=exploded.hover_text,
        #             name='V3 Jitter'
        #         ),
        #         row=2,
        #         col=1
        #     )
        #
        #     layout = go.Layout(
        #         xaxis=dict(title='Datetime'),
        #         xaxis2=dict(title='Datetime'),
        #         yaxis=dict(title='V2 jitter (3 second average)'),
        #         yaxis2=dict(title='V3 jitter (3 second average)')
        #     )
        #
        #     figure.update_layout(layout)
        #
        #     figure.show(renderer='browser')
        #
        #     return figure, exploded