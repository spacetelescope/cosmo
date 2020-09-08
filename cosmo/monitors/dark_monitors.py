
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
    docs = "https://spacetelescope.github.io/cosmo/monitors.html#dark-rate" \
           "-monitors"
    segment = None
    location = None
    multi = False
    sub_names = None
    data_model = DarkDataModel
    plottype = 'scatter'
    x = 'date'
    y = 'darks'

    def get_data(self):  # -> Any: fix this later,
        # should be fine in the monitor, just not in jupyter notebook
        if self.multi:
            # prime the pump
            exploded_df = self.filter_data(self.location[0])
            exploded_df["region"] = 0
            for index, location in enumerate(self.location[1:]):
                sub_exploded_df = self.filter_data(location)
                sub_exploded_df["region"] = index + 1
                exploded_df = exploded_df.append(sub_exploded_df)

        else:
            exploded_df = self.filter_data(self.location)

        return exploded_df

    def filter_data(self, location):
        filtered_rows = []
        for _, row in self.model.new_data.iterrows():
            if row.EXPSTART == 0:
                continue
            if row.SEGMENT == self.segment:
                if row.SEGMENT == "N/A":  # NUV
                    filtered_rows.append(dark_filter(row, False, location))
                else:  # Any of the FUV situations
                    filtered_rows.append(dark_filter(row, True, location))
        filtered_df = pd.concat(filtered_rows).reset_index(drop=True)

        return explode_df(filtered_df, ['darks', 'date'])

    def plot(self):
        # make the interactive plots with sub-solar plots
        if self.multi:
            rows = len(self.location) + 1
            self.sub_names += ["Solar Radio Flux"]
            titles = tuple(self.sub_names)
        else:
            # only one region means two subplots
            rows = 2
            titles = (self.name, "Solar Radio Flux")

        fig_height = 750
        delta = 250
        if rows > 3:
            fig_height = delta * rows

        pio.templates.default = "simple_white"

        self.figure = make_subplots(rows=rows, cols=1, shared_xaxes=True,
                                    subplot_titles=titles, x_title="Year",
                                    vertical_spacing=0.05)
        self.figure.update_layout(height=fig_height, width=1200,
                                  title_text=self.name)

        if self.multi:
            # prime the pump again
            region_x_data = self.data[self.x].where(self.data["region"] == 0)
            region_y_data = self.data[self.y].where(self.data["region"] == 0)
            self.figure.add_trace(
                go.Scatter(x=region_x_data, y=region_y_data, mode="markers",
                           marker=dict(color="black", size=5),
                           hovertext=self.labels, hoverinfo="x+y+text",
                           name="Mean Dark Rate"), row=1, col=1)
            self.figure.update_yaxes(
                title_text="Mean Dark Rate<br>(counts/pix/sec)", row=1, col=1)
            for index, location in enumerate(self.location[1:]):
                index = index + 1
                region_x_data = self.data[self.x].where(
                    self.data["region"] == index)
                region_y_data = self.data[self.y].where(
                    self.data["region"] == index)
                self.figure.add_trace(
                    go.Scatter(x=region_x_data, y=region_y_data,
                               showlegend=False, mode="markers",
                               marker=dict(color="black", size=5),
                               hovertext=self.labels, hoverinfo="x+y+text",
                               name="Mean Dark Rate"), row=index + 1, col=1)
                self.figure.update_yaxes(
                    title_text="Mean Dark Rate<br>(counts/pix/sec)",
                    row=index + 1, col=1)

        else:
            # single plot
            self.figure.add_trace(
                go.Scatter(x=self.data[self.x], y=self.data[self.y],
                           mode="markers", marker=dict(color="black", size=5),
                           hovertext=self.labels, hoverinfo="x+y+text",
                           name="Mean Dark Rate"), row=1, col=1)
            self.figure.update_yaxes(
                title_text="Mean Dark Rate<br>(counts/pix/sec)", row=1, col=1)

        ## this is solar stuff only until the next ##

        datemin = self.data[self.x].min()
        datemax = self.data[self.x].max()

        # sunpy_data = sunpy_retriever(date_min, date_max)
        solar_data = get_solar_data(NOAA_URL, datemin, datemax)
        solar_time = solar_data.index
        solar_flux = solar_data["f10.7"]
        solar_flux_smooth = solar_data["box_convolved_f10.7"]

        self.figure.add_trace(
            go.Scatter(x=solar_time, y=solar_flux, mode="lines",
                       line=dict(dash="longdash", color="#0F2080"),
                       name="10.7 cm"), row=rows, col=1)

        self.figure.add_trace(
            go.Scatter(x=solar_time, y=solar_flux_smooth, mode="lines",
                       line=dict(color="#85C0F9"), name="10.7 cm Smoothed"),
            row=rows, col=1)

        self.figure.update_yaxes(title_text="Solar Radio Flux", row=rows,
                                 col=1)

        ##

        self.figure.update_xaxes(showgrid=True, showline=True, mirror=True)
        self.figure.update_yaxes(showgrid=True, showline=True, mirror=True)

    def plot_histogram(self, nbins=100):
        if self.data is None:
            self.data = self.get_data()

        dist995, lines = self.calculate_histogram(nbins)
        full_names = [f"Mean: {lines[0]:.2e}", f"Median: {lines[0]:.2e}",
                      f"2 sigma: {lines[2]:.2e}", f"3 sigma: {lines[3]:.2e}",
                      f"95%: {lines[4]:.2e}", f"99%: {lines[5]:.2e}"]

        # histogram
        fig = go.Figure(data=[
            go.Histogram(x=self.data[self.y], nbinsx=nbins, showlegend=False)])

        # value lines--have to do a shape and trace for both of them until
        # plotly adds vertical line plotting features (because shapes can't
        # be in the legend, only traces)
        # also f strings and latex together is super annoying. have to
        # triple bracket the latex part
        fig.add_trace(
            go.Scatter(x=[lines[0], lines[0]], y=[0, 1], mode="lines",
                       line=dict(color="#DC267F"), name=full_names[0]))
        fig.add_shape(
            dict(type="line", xref="x", yref="paper", x0=lines[0], y0=0,
                 x1=lines[0], y1=1, line=dict(color="#DC267F")))

        fig.add_trace(
            go.Scatter(x=[lines[1], lines[1]], y=[0, 1], mode="lines",
                       line=dict(color="#DC267F", dash="dash"),
                       name=full_names[1]))
        fig.add_shape(
            dict(type="line", xref="x", yref="paper", x0=lines[1], y0=0,
                 x1=lines[1], y1=1, line=dict(color="#DC267F", dash="dash")))

        fig.add_trace(
            go.Scatter(x=[lines[2], lines[2]], y=[0, 1], mode="lines",
                       line=dict(color="#FE6100"), name=full_names[2]))
        fig.add_shape(
            dict(type="line", xref="x", yref="paper", x0=lines[2], y0=0,
                 x1=lines[2], y1=1, line=dict(color="#FE6100")))

        fig.add_trace(
            go.Scatter(x=[lines[3], lines[3]], y=[0, 1], mode="lines",
                       line=dict(color="#FE6100", dash="dash"),
                       name=full_names[3]))
        fig.add_shape(
            dict(type="line", xref="x", yref="paper", x0=lines[3], y0=0,
                 x1=lines[3], y1=1, line=dict(color="#FE6100", dash="dash")))

        fig.add_trace(
            go.Scatter(x=[lines[4], lines[4]], y=[0, 1], mode="lines",
                       line=dict(color="#FFB000"), name=full_names[4]))
        fig.add_shape(
            dict(type="line", xref="x", yref="paper", x0=lines[4], y0=0,
                 x1=lines[4], y1=1, line=dict(color="#FFB000")))

        fig.add_trace(
            go.Scatter(x=[lines[5], lines[5]], y=[0, 1], mode="lines",
                       line=dict(color="#FFB000", dash="dash"),
                       name=full_names[5]))
        fig.add_shape(
            dict(type="line", xref="x", yref="paper", x0=lines[5], y0=0,
                 x1=lines[5], y1=1, line=dict(color="#FFB000", dash="dash")))

        datemin = self.data[self.x].min()
        datemax = self.data[self.x].max()
        fig.update_xaxes(range=[0, dist995], title_text="Counts/Pix/Sec")
        fig.update_yaxes(rangemode="tozero", title_text="Frequency")
        fig.update_layout(
            title_text=self.name + f" Histogram: {datemin:%Y-%m-%d} - "
                                   f"{datemax:%Y-%m-%d}")
        fig.update_layout(xaxis=dict(showexponent='all', exponentformat='e'))
        fig.update_layout(yaxis_showgrid=True)

        # fix this naming convention later
        if not self.output:
            output = f'{os.path.join(os.getcwd(), f"{self._filename}_hist.html")}'
        else:
            # you would think you could use self.output but that gets
            # updated somewhere
            # in the superclass to include the full plot name so we can't
            # use that.
            # kind of a bug
            output = os.path.join(COS_MONITORING,
                                  f"{self._filename}_hist.html")

        fig.write_html(output)

    def calculate_histogram(self, nbins=100):
        if self.data is None:
            self.data = self.get_data()

        counts, bins = np.histogram(self.data[self.y], bins=nbins)
        cuml_dist = np.cumsum(counts)
        count_99 = abs(cuml_dist / float(cuml_dist.max()) - .99).argmin()
        count_95 = abs(cuml_dist / float(cuml_dist.max()) - .95).argmin()
        # only used for plotting
        count995 = abs(cuml_dist / float(cuml_dist.max()) - .995).argmin()

        mean = self.data[self.y].mean()
        med = np.median(self.data[self.y])
        std = self.data[self.y].std()
        onesig = med + std
        twosig = med + (2 * std)
        threesig = med + (3 * std)
        dist95 = bins[count_95]
        dist99 = bins[count_99]
        dist995 = bins[count995]
        values = [mean, med, onesig, twosig, threesig, dist95, dist99]

        return dist995, values

    def track(self, nbins=100):
        _, track_list = self.calculate_histogram(nbins)
        return track_list

    def store_results(self):
        # TODO: Define results to store
        pass


# ----------------------------------------------------------------------------#


class FUVADarkMonitor(DarkMonitor):
    name = 'FUVA Dark Monitor'
    segment = 'FUVA'
    multi = True
    location = [(1060, 15250, 296, 375), (1060, 1260, 296, 734),
                (1060, 15250, 660, 734), (15119, 15250, 296, 734),
                (1260, 15119, 375, 660)]
    sub_names = ["FUVA Dark Monitor - Bottom", "FUVA Dark Monitor - Left",
                 "FUVA Dark Monitor - Top", "FUVA Dark Monitor - Right",
                 "FUVA Dark Monitor - Inner"]


class FUVBDarkMonitor(DarkMonitor):
    name = 'FUVB Dark Monitor'
    segment = 'FUVB'
    multi = True
    location = [(809, 15182, 360, 405), (809, 1000, 360, 785),
                (809, 15182, 740, 785), (14990, 15182, 360, 785),
                (1000, 14990, 405, 740)]
    sub_names = ["FUVB Dark Monitor - Bottom", "FUVB Dark Monitor - Left",
                 "FUVB Dark Monitor - Top", "FUVB Dark Monitor - Right",
                 "FUVB Dark Monitor - Inner"]


class FUVABottomDarkMonitor(DarkMonitor):
    """FUVA dark monitor for bottom edge"""
    segment = 'FUVA'
    location = (1060, 15250, 296, 375)
    name = 'FUVA Dark Monitor - Bottom'


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


