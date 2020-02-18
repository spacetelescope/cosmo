from typing import Any

import numpy as np
import plotly.graph_objs as go
import pandas as pd

from itertools import repeat

from monitorframe.monitor import BaseMonitor
from .data_models import FUVDarkDataModel
from ..monitor_helpers import explode_df, absolute_time
from .. import SETTINGS

COS_MONITORING = SETTINGS['output']


def dark_filter(df_row, filter_pha, location):
    good_pha = (2, 23)
    time_step = 25
    time_bins = df_row['TIME_3'][::time_step]
    lat = df_row['LATITUDE'][::time_step][:-1]
    lon = df_row['LONGITUDE'][::time_step][:-1]
    event_df = df_row[['SEGMENT', 'XCORR', 'YCORR', 'PHA', 'TIME']].to_frame().T
    event_df = explode_df(event_df, ['XCORR', 'YCORR', 'PHA', 'TIME'])
    npix = (location[1] - location[0]) * (location[3] - location[2])
    index = np.where((event_df['XCORR'] > location[0]) &
                     (event_df['XCORR'] < location[1]) &
                     (event_df['YCORR'] > location[2]) &
                     (event_df['YCORR'] < location[3]))
    filtered_row = event_df.iloc[index].reset_index(drop=True)

    if filter_pha:
        filtered_row = filtered_row[(filtered_row['PHA'] > good_pha[0]) & (filtered_row['PHA'] < good_pha[1])]

    counts = np.histogram(filtered_row.TIME, bins=time_bins)[0]

    date = absolute_time(
        expstart=list(repeat(df_row['EXPSTART'], len(time_bins))), time=time_bins.tolist()
    ).to_datetime()[:-1]

    dark_rate = counts / npix / time_step

    return pd.DataFrame({'segment': df_row['SEGMENT'], 'darks': [dark_rate], 'date': [date],
                        'ROOTNAME': df_row['ROOTNAME']})


class FUVDarkMonitor(BaseMonitor):
    """Abstracted FUV Dark Monitor. Not meant to be used directly but rather inherited by specific segment and region
    dark monitors"""
    labels = ['ROOTNAME']
    output = COS_MONITORING
    docs = "https://spacetelescope.github.io/cosmo/monitors.html#fuv-dark-rate-monitors"

    segment = None
    location = None

    plottype = 'scatter'
    x = 'date'
    y = 'darks'

    def get_data(self) -> Any:
        filtered_rows = []
        for _, row in self.model.new_data.iterrows():
            if row.EXPSTART == 0:
                continue
            if row.SEGMENT == self.segment:
                filtered_rows.append(dark_filter(row, True, self.location))
        filtered_df = pd.concat(filtered_rows).reset_index(drop=True)

        return explode_df(filtered_df, ['darks', 'date'])

    def store_results(self):
        # TODO: Define results to store
        pass

    def track(self):
        # TODO: Define something to track
        pass


class FUVABottomDarkMonitor(FUVDarkMonitor):
    """FUVA dark monitor for bottom edge"""
    data_model = FUVDarkDataModel
    segment = 'FUVA'
    location = (1060, 15250, 296, 375)
    name = f'FUVA Dark Monitor - Bottom'


class FUVALeftDarkMonitor(FUVDarkMonitor):
    """FUVA dark monitor for left edge"""
    name = 'FUVA Dark Monitor - Left'
    data_model = FUVDarkDataModel
    segment = 'FUVA'
    location = (1060, 1260, 296, 734)


class FUVATopDarkMonitor(FUVDarkMonitor):
    """FUVA dark monitor for top edge"""
    name = 'FUVA Dark Monitor - Top'
    data_model = FUVDarkDataModel
    segment = 'FUVA'
    location = (1060, 15250, 660, 734)


class FUVARightDarkMonitor(FUVDarkMonitor):
    """FUVA dark monitor for right edge"""
    name = 'FUVA Dark Monitor - Right'
    data_model = FUVDarkDataModel
    segment = 'FUVA'
    location = (15119, 15250, 296, 734)


class FUVAInnerDarkMonitor(FUVDarkMonitor):
    """FUVA dark monitor for inner region"""
    name = 'FUVA Dark Monitor - Inner'
    data_model = FUVDarkDataModel
    segment = 'FUVA'
    location = (1260, 15119, 375, 660)


class FUVBBottomDarkMonitor(FUVDarkMonitor):
    """FUVB dark monitor for bottom edge"""
    name = 'FUVB Dark Monitor - Bottom'
    data_model = FUVDarkDataModel
    segment = 'FUVB'
    location = (809, 15182, 360, 405)


class FUVBLeftDarkMonitor(FUVDarkMonitor):
    """FUVB dark monitor for left edge"""
    name = 'FUVB Dark Monitor - Left'
    data_model = FUVDarkDataModel
    segment = 'FUVB'
    location = (809, 1000, 360, 785)


class FUVBTopDarkMonitor(FUVDarkMonitor):
    """FUVB dark monitor for top edge"""
    name = 'FUVB Dark Monitor - Top'
    data_model = FUVDarkDataModel
    segment = 'FUVB'
    location = (809, 15182, 740, 785)


class FUVBRightDarkMonitor(FUVDarkMonitor):
    """FUVB dark monitor for right edge"""
    name = 'FUVB Dark Monitor - Right'
    data_model = FUVDarkDataModel
    segment = 'FUVB'
    location = (14990, 15182, 360, 785)


class FUVBInnerDarkMonitor(FUVDarkMonitor):
    """FUVB dark monitor for inner region"""
    name = 'FUVB Dark Monitor - Inner'
    data_model = FUVDarkDataModel
    segment = 'FUVB'
    location = (1000, 14990, 405, 740)
