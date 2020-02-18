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


class FUVALeftDarkMonitor(BaseMonitor):
    name = 'FUVA Dark Monitor - Left'
    data_model = FUVDarkDataModel
    labels = ['ROOTNAME']
    output = COS_MONITORING

    location = (1060, 1260, 296, 734)
    plottype = 'scatter'
    x = 'date'
    y = 'darks'

    def get_data(self) -> Any:
        filtered_rows = []
        for _, row in self.model.new_data.iterrows():
            if row.EXPSTART == 0:
                continue
            if row.SEGMENT == 'FUVA':
                filtered_rows.append(dark_filter(row, True, self.location))
        filtered_df = pd.concat(filtered_rows).reset_index(drop=True)

        return explode_df(filtered_df, ['darks', 'date'])

    def store_results(self):
        # TODO: Define results to store
        pass

    def track(self):
        # TODO: Define something to track
        pass


class FUVABottomDarkMonitor(BaseMonitor):
    name = 'FUVA Dark Monitor - Bottom'
    data_model = FUVDarkDataModel
    labels = ['ROOTNAME']
    output = COS_MONITORING

    location = (1060, 15250, 296, 375)
    plottype = 'scatter'
    x = 'date'
    y = 'darks'

    def get_data(self) -> Any:
        filtered_rows = []
        for _, row in self.model.new_data.iterrows():
            if row.EXPSTART == 0:
                continue
            if row.SEGMENT == 'FUVA':
                filtered_rows.append(dark_filter(row, True, self.location))
        filtered_df = pd.concat(filtered_rows).reset_index(drop=True)

        return explode_df(filtered_df, ['darks', 'date'])

    def store_results(self):
        # TODO: Define results to store
        pass

    def track(self):
        # TODO: Define something to track
        pass


