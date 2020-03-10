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
        dec_year_column = []

        xlim = [0, 1024]
        ylim = [0, 1024]

        # parallelize, this is going to get bad when looking at a lot of data
        for index, row in data.iterrows():
            subdf = pd.DataFrame({
                "EXPSTART": row["EXPSTART"], "TIME": row["TIME"],
                "XCORR": row["XCORR"], "YCORR": row["YCORR"],
                "TIME_3": row["TIME_3"]
                })
            filtered = subdf.where(
                (subdf["XCORR"] > xlim[0]) & (subdf["XCORR"] < xlim[1]) & (
                            subdf["YCORR"] > ylim[0]) & (
                            subdf["YCORR"] < ylim[1]))
            dark_rate_array, dec_year_array = self.calculate_dark_rate(
                filtered, xlim, ylim)
            dark_rate_column.append(dark_rate_array)
            dec_year_column.append(dec_year_array)

        data["DARK_RATE"] = dark_rate_column
        data["DECIMAL_YEAR"] = dec_year_column

        return data

    def calculate_dark_rate(self, dataframe, xlim, ylim):
        # calculate dark rate for one exposure, with a dataframe with TIME,
        # XCORR, and YCORR values

        # need to set this somewhere
        timestep = 25
        time_bins = dataframe["TIME"][::timestep]

        counts = np.histogram(dataframe["TIME"], bins=time_bins)[0]
        npix = float((xlim[1] - xlim[0]) * (ylim[1] - ylim[0]))
        dark_rate_array = counts / npix / timestep
        # save the whole histogram in time bins, and then plot each of them

        # make a decimal year array corresponding to the time bins of the
        # dark rates
        # do this with the expstart (mjd) and time array from the timeline
        # extension
        mjd_conversion_factor = 1.15741e-5
        # taking the expstart, binning the time array by the timestep,
        # removing the last element in the array (bin by front edge),
        # and then multiplying by the conversion factor
        mjd_array = dataframe["EXPSTART"] + dataframe["TIME_3"][::timestep][
                                            :-1] * mjd_conversion_factor
        mjd_array = Time(mjd_array, format='mjd')
        dec_year_array = mjd_array.decimalyear

        return dark_rate_array, dec_year_array

    def track(self):
        # track something. perhaps current dark rate?
        pass

    def plot(self):
        # do some plots

        pass

    def store_results(self):
        # need to store results if not going in the database
        pass