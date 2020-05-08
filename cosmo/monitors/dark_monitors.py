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
    # time step stuff
    time_step = 25
    time_bins = df_row['TIME_3'][::time_step]
    lat = df_row['LATITUDE'][::time_step][:-1]
    lon = df_row['LONGITUDE'][::time_step][:-1]

    # try commenting these out, since lat and lon don't seem to be used
    #     lat = df_row['LATITUDE'][::time_step][:-1]
    #     lon = df_row['LONGITUDE'][::time_step][:-1]

    # filtering pha
    if filter_pha:
        event_df = df_row[
            ['SEGMENT', 'XCORR', 'YCORR', 'PHA', 'TIME']].to_frame().T
        event_df = explode_df(event_df, ['XCORR', 'YCORR', 'PHA', 'TIME'])
    else:
        event_df = df_row[['SEGMENT', 'XCORR', 'YCORR', 'TIME']].to_frame().T
        event_df = explode_df(event_df, ['XCORR', 'YCORR', 'TIME'])

    # creating event dataframe and filtering it by location on the detector
    npix = (location[1] - location[0]) * (location[3] - location[2])
    index = np.where((event_df['XCORR'] > location[0]) &
                     (event_df['XCORR'] < location[1]) &
                     (event_df['YCORR'] > location[2]) &
                     (event_df['YCORR'] < location[3]))
    filtered_row = event_df.iloc[index].reset_index(drop=True)

    # filtered events only need to be further filtered by PHA if not NUV
    if filter_pha:
        filtered_row = filtered_row[(filtered_row['PHA'] > good_pha[0]) & (
                    filtered_row['PHA'] < good_pha[1])]

    counts = np.histogram(filtered_row.TIME, bins=time_bins)[0]

    date = absolute_time(
        expstart=list(repeat(df_row['EXPSTART'], len(time_bins))),
        time=time_bins.tolist()).to_datetime()[:-1]

    dark_rate = counts / npix / time_step

    return pd.DataFrame(
        {'segment': df_row['SEGMENT'], 'darks': [dark_rate],
         'date': [date], 'ROOTNAME': df_row['ROOTNAME']}
        )


class DarkMonitor(BaseMonitor):
    """Abstracted FUV Dark Monitor. Not meant to be used directly but rather inherited by specific segment and region
    dark monitors"""
    labels = ['ROOTNAME']
    output = COS_MONITORING
    docs = "https://spacetelescope.github.io/cosmo/monitors.html#dark-rate-monitors"
    segment = None
    location = None
    data_model = DarkDataModel
    plottype = 'scatter'
    x = 'date'
    y = 'darks'

    def get_data(self) -> Any:
        filtered_rows = []
        for _, row in self.model.new_data.iterrows():
            if row.EXPSTART == 0:
                continue
            if row.SEGMENT == self.segment:
                if row.SEGMENT == "N/A":  # NUV
                    filtered_rows.append(
                        dark_filter(row, False, self.location))
                else:  # Any of the FUV situations
                    filtered_rows.append(dark_filter(row, True, self.location))
        filtered_df = pd.concat(filtered_rows).reset_index(drop=True)

        return explode_df(filtered_df, ['darks', 'date'])

    def store_results(self):
        # TODO: Define results to store
        pass

    def track(self):
        # TODO: Define something to track
        pass


class FUVABottomDarkMonitor(DarkMonitor):
    """FUVA dark monitor for bottom edge"""
    segment = 'FUVA'
    location = (1060, 15250, 296, 375)
    name = f'FUVA Dark Monitor - Bottom'


class FUVALeftDarkMonitor(DarkMonitor):
    """FUVA dark monitor for left edge"""
    name = 'FUVA Dark Monitor - Left'
    segment = 'FUVA'
    location = (1060, 1260, 296, 734)


class FUVATopDarkMonitor(DarkMonitor):
    """FUVA dark monitor for top edge"""
    name = 'FUVA Dark Monitor - Top'
    segment = 'FUVA'
    location = (1060, 15250, 660, 734)


class FUVARightDarkMonitor(DarkMonitor):
    """FUVA dark monitor for right edge"""
    name = 'FUVA Dark Monitor - Right'
    segment = 'FUVA'
    location = (15119, 15250, 296, 734)


class FUVAInnerDarkMonitor(DarkMonitor):
    """FUVA dark monitor for inner region"""
    name = 'FUVA Dark Monitor - Inner'
    segment = 'FUVA'
    location = (1260, 15119, 375, 660)


class FUVBBottomDarkMonitor(DarkMonitor):
    """FUVB dark monitor for bottom edge"""
    name = 'FUVB Dark Monitor - Bottom'
    segment = 'FUVB'
    location = (809, 15182, 360, 405)


class FUVBLeftDarkMonitor(DarkMonitor):
    """FUVB dark monitor for left edge"""
    name = 'FUVB Dark Monitor - Left'
    segment = 'FUVB'
    location = (809, 1000, 360, 785)


class FUVBTopDarkMonitor(DarkMonitor):
    """FUVB dark monitor for top edge"""
    name = 'FUVB Dark Monitor - Top'
    segment = 'FUVB'
    location = (809, 15182, 740, 785)


class FUVBRightDarkMonitor(DarkMonitor):
    """FUVB dark monitor for right edge"""
    name = 'FUVB Dark Monitor - Right'
    segment = 'FUVB'
    location = (14990, 15182, 360, 785)


class FUVBInnerDarkMonitor(DarkMonitor):
    """FUVB dark monitor for inner region"""
    name = 'FUVB Dark Monitor - Inner'
    segment = 'FUVB'
    location = (1000, 14990, 405, 740)


class NUVDarkMonitor(DarkMonitor):
    name = "NUV Dark Monitor"
    segment = "N/A"
    location = (0, 1024, 0, 1024)
