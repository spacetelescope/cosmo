
import os
import json
import datetime

import numpy as np
import pandas as pd
import plotly.io as pio
import plotly.express as px
import plotly.graph_objs as go

# from tqdm import tqdm
from typing import Any
from urllib import request
from itertools import repeat
from plotly.subplots import make_subplots
from monitorframe.monitor import BaseMonitor
from astropy.convolution import Box1DKernel, convolve

from .. import SETTINGS
from .data_models import DarkDataModel
from ..monitor_helpers import explode_df, absolute_time

COS_MONITORING = SETTINGS['output']
NOAA_URL = 'https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json'

# ----------------------------------------------------------------------------#


# def run_all_dark_monitors():
#     fuva_bottom_monitor = FUVABottomDarkMonitor()
#     fuva_left_monitor = FUVALeftDarkMonitor()
#     fuva_top_monitor = FUVATopDarkMonitor()
#     fuva_right_monitor = FUVARightDarkMonitor()
#     fuva_inner_monitor = FUVAInnerDarkMonitor()
#     fuvb_bottom_monitor = FUVBBottomDarkMonitor()
#     fuvb_left_monitor = FUVBLeftDarkMonitor()
#     fuvb_top_monitor = FUVBTopDarkMonitor()
#     fuvb_right_monitor = FUVBRightDarkMonitor()
#     fuvb_inner_monitor = FUVBInnerDarkMonitor()
#     nuv_monitor = NUVDarkMonitor()
#     for monitor in tqdm([fuva_bottom_monitor, fuva_left_monitor,
#                          fuva_top_monitor, fuva_right_monitor,
#                          fuva_inner_monitor, fuvb_bottom_monitor,
#                          fuvb_left_monitor, fuvb_top_monitor,
#                          fuvb_right_monitor, fuvb_inner_monitor,
#                          nuv_monitor]):
#         monitor.monitor()


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


def get_solar_data(url, datemin, datemax, box=4):
    """Download the most recent solar data, save as file and dataframe,
    filter dataframe to date range. Also replace -1 values in the smoothed
    flux."""
    response = request.urlopen(url)
    if response.status == 200:
        data = json.loads(response.read())
    else:
        print("Invalid response! HTTP Status Code: {}".format(response.status))
    df = pd.DataFrame(data)
    dates = [datetime.datetime.strptime(val, '%Y-%m') for val in
             df['time-tag']]
    df.index = pd.DatetimeIndex(dates)

    todays_date = datetime.datetime.today().strftime('%b%d_%Y')
    outfile = os.path.join(COS_MONITORING,
                           "noaa_solar_indices_{}.txt".format(todays_date))
    # print("Saving outfile: {}".format(outfile))
    df.to_csv(outfile, header=True, index=True)

    # print("Filtering the dataframe to the date range: {}, {}".format(datemin,
    #                                                                  datemax))
    df = df.loc[datemin:datemax]
    # smoothing the f10.7 data
    kernel = Box1DKernel(box)
    smoothed_107 = convolve(df["f10.7"], kernel)
    df["box_convolved_f10.7"] = smoothed_107

    return df


class DarkMonitor(BaseMonitor):
    """Abstracted Dark Monitor. Not meant to be used directly but rather
    inherited by specific segment and region dark monitors"""
    labels = ['ROOTNAME']
    output = COS_MONITORING
    docs = "https://spacetelescope.github.io/cosmo/monitors.html#dark-rate-monitors"
    segment = None
    location = None
    data_model = DarkDataModel
    plottype = 'scatter'
    x = 'date'
    y = 'darks'

    def get_data(self):  # -> Any: fix this later,
        # should be fine in the monitor, just not in jupyter notebook
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

    def plot(self):
        # make the interactive plots with sub-solar plots
        pio.templates.default = "simple_white"
        self.figure = make_subplots(rows=2, cols=1, subplot_titles=(
        self.name, "Solar Radio Flux"))

        self.figure.add_trace(
            go.Scatter(x=self.data[self.x], y=self.data[self.y],
                       mode="markers", marker=dict(color="black", size=5),
                       hovertext=self.labels, hoverinfo="x+y+text",
                       name="Mean Dark Rate"), row=1, col=1)

        datemin = self.data[self.x].min()
        datemax = self.data[self.x].max()

        # sunpy_data = sunpy_retriever(date_min, date_max)
        solar_data = get_solar_data(NOAA_URL, datemin, datemax)
        solar_time = solar_data.index
        solar_flux = solar_data["f10.7"]
        solar_flux_smooth = solar_data["box_convolved_f10.7"]

        self.figure.add_trace(
            go.Scatter(x=solar_time, y=solar_flux, mode="lines",
                       name="10.7 cm"), row=2, col=1)

        self.figure.add_trace(
            go.Scatter(x=solar_time, y=solar_flux_smooth, mode="lines",
                       name="10.7 cm Smoothed"), row=2, col=1)

        self.figure['layout']['xaxis1'].update(title="Year")
        self.figure['layout']['yaxis1'].update(
            title="Mean Dark Rate (counts/pix/sec)")
        self.figure['layout']['xaxis2'].update(title="Year",
                                               range=[datemin, datemax])
        self.figure['layout']['yaxis2'].update(title="Solar Radio Flux")

        self.figure.update_xaxes(showgrid=True, showline=True, mirror=True)
        self.figure.update_yaxes(showgrid=True, showline=True, mirror=True)

    def store_results(self):
        # TODO: Define results to store
        pass

    def track(self):
        # TODO: Define something to track
        pass


# ----------------------------------------------------------------------------#


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


