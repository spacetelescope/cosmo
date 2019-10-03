from typing import Any

import numpy as np
import plotly.graph_objs as go
import pandas as pd

from itertools import repeat

from monitorframe.monitor import BaseMonitor
from .dark_data_models import DarkDataModel
from ..monitor_helpers import fit_line, convert_day_of_year, explode_df, absolute_time


def dark_filter(df_row, filter_pha, location):
    good_pha = (2,23)
    time_step = 25
    time_bins = df_row['TIME'][::time_step]
    lat = df_row['LATITUDE'][::time_step][:-1]
    lon = df_row['LONGITUDE'][::time_step][:-1]
    event_df = df_row[['SEGMENT', 'XCORR', 'YCORR', 'PHA', 'TIME_events']].to_frame().T
    event_df = explode_df(event_df, ['XCORR', 'YCORR', 'PHA', 'TIME_events'])
    npix = (location[1] - location[0]) * (location[3] - location[2])
    index = np.where((event_df['SEGMENT'] == 'FUVA') &
                     (event_df['XCORR'] > location[0]) &
                     (event_df['XCORR'] < location[1]) &
                     (event_df['YCORR'] > location[2]) &
                     (event_df['YCORR'] < location[3]))
    filtered_row = event_df.iloc[index].reset_index(drop=True)

    if filter_pha:
        filtered_row = filtered_row[(filtered_row['PHA'] > good_pha[0]) & (filtered_row['PHA'] < good_pha[1])]

    counts = np.histogram(filtered_row.TIME_events, bins=time_bins)[0]

    _, mjd = absolute_time(expstart=df_row['EXPSTART'], time=time_bins)
    # _, mjd = compute_absolute_time(expstart=df_row['EXPSTART'], time_array=time_bins)
    date = mjd.to_datetime()[:-1]
    dark_rate = counts / npix / time_step

    return pd.DataFrame({'segment': df_row['SEGMENT'], 'darks': [dark_rate], 'date': [date],
                        'file': df_row['ROOTNAME'], 'hover_text': df_row['hover_text']})


def filter_data(self):
    filtered_rows = []
    for _, row in self.data.iterrows():
        filtered_rows.append(dark_filter(row, True, self.location))
    filtered_df = pd.concat(filtered_rows).reset_index(drop=True)

    return explode_df(filtered_df, ['darks', 'date'])


class FUVALeftDarkMonitor(BaseMonitor):
    data_model = DarkDataModel
    labels = ['ROOTNAME']
    output = '/Users/dashtamirova/Desktop/test_dark.html'
    location = (1060, 1260, 296, 734)

    def get_data(self) -> Any:
        pass

    def plot(self):
        self.plottype = 'scatter'
        self.x = filter_data(self).date
        self.y = filter_data(self).darks

    def store_results(self):
        # TODO: Define results to store
        pass

    def track(self):
        # TODO: Define something to track
        pass
