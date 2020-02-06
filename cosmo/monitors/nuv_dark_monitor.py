import numpy as np
import plotly.graph_objs as go
import plotly.express as px
import datetime
import pandas as pd

from peewee import Model
from monitorframe.monitor import BaseMonitor
from astropy.time import Time
from typing import List, Union

from .data_models import NUVDarkDataModel
from ..monitor_helpers import fit_line, convert_day_of_year, create_visibility, v2v3
from .. import SETTINGS

COS_MONITORING = SETTINGS['output']


class NUVDarkMonitor(BaseMonitor):
    data_model = NUVDarkDataModel
    # TODO: update docs
    # docs = "https://spacetelescope.github.io/cosmo/monitors.html#<darks>"
    output = COS_MONITORING

    run = 'monthly'

    def get_data(self):
        # access data, perform any filtering required for analysis
        data = self.model.new_data
        dark_rate_column = []

        xlim = [0, 1024]
        ylim = [0, 1024]

        # parallelize, this is going to get bad when looking at a lot of data
        for index, row in data.iterrows():
            subdf = pd.DataFrame({
                "TIME": row["TIME"], "XCORR": row["XCORR"],
                "YCORR": row["YCORR"]
                })
            filtered = subdf.where(
                (subdf["XCORR"] > xlim[0]) & (subdf["XCORR"] < xlim[1]) & (
                            subdf["YCORR"] > ylim[0]) & (
                            subdf["YCORR"] < ylim[1]))
            dark_rate = self.calculate_dark_rate(filtered, xlim, ylim)
            dark_rate_column.append(dark_rate)

        data["DARK_RATE"] = dark_rate_column

        return data

    def calculate_dark_rate(self, dataframe, xlim, ylim):
        # calculate dark rate for one exposure, with a dataframe with TIME,
        # XCORR, and YCORR values

        # need to set this somewhere
        timestep = 25
        time_bins = dataframe["TIME"][::timestep]

        counts = np.histogram(dataframe["TIME"], bins=time_bins)[0]
        npix = float((xlim[1] - xlim[0]) * (ylim[1] - ylim[0]))
        dark_rate = np.median(counts / npix / timestep)
        # what is going on with the whole histogram, ok to use median?

        return dark_rate

    def track(self):
        # track something. perhaps current dark rate?
        pass

    def plot(self):
        # do some plots
        pass

    def store_results(self):
        # need to store results if not going in the database
        pass
