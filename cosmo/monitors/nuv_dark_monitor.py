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

import os
import yaml
import cosmo
import numpy
import itertools
from glob import glob
from astropy.io import fits
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
from cosmo.monitors.data_models import NUVDarkDataModel
from cosmo.filesystem import find_files, data_from_exposures, data_from_jitters
from cosmo.monitor_helpers import absolute_time, explode_df
from monitorframe.datamodel import BaseDataModel
from monitorframe.monitor import BaseMonitor

# these imports are so messy i will fix this later

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
                "EXPSTART": row["EXPSTART"], "TIME": [row["TIME"]],
                "XCORR": [row["XCORR"]], "YCORR": [row["YCORR"]],
                "TIME_3": [row["TIME_3"]]
                })

            # this is temporary until i understand james' suggestion to use
            # df.apply and lambda functions
            xcorr = subdf["XCORR"][0]
            ycorr = subdf["YCORR"][0]
            filtered_xcorr = xcorr[
                np.where((xcorr > xlim[0]) & (xcorr < xlim[1]))]
            filtered_ycorr = ycorr[
                np.where((ycorr > ylim[0]) & (ycorr < ylim[1]))]
            subdf["XCORR"] = [filtered_xcorr]
            subdf["YCORR"] = [filtered_ycorr]

            dark_rate_array, dec_year_array = self.calculate_dark_rate(subdf,
                                                                       xlim,
                                                                       ylim)
            dark_rate_column.append(dark_rate_array)
            dec_year_column.append(dec_year_array)

        data["DARK_RATE"] = dark_rate_column
        data["DECIMAL_YEAR"] = dec_year_column

        # when the monitor method of the monitor is called, it will
        # initialize the self.data attribute
        # with this method and then can be used by the other methods
        return data

    def calculate_dark_rate(self, dataframe, xlim, ylim):
        # calculate dark rate for one exposure, with a dataframe with TIME,
        # XCORR, and YCORR values

        # need to set this somewhere
        timestep = 25
        time_bins = dataframe["TIME_3"][0][::timestep]

        counts = np.histogram(dataframe["TIME_3"][0], bins=time_bins)[0]
        npix = float((xlim[1] - xlim[0]) * (ylim[1] - ylim[0]))
        dark_rate_array = counts / npix / timestep
        # save the whole histogram in time bins, and then plot each of them

        # make a decimal year array corresponding to the time bins of the
        # dark rates
        # do this with the expstart (mjd) and time array from the timeline
        # extension
        # taking the expstart, binning the time array by the timestep,
        # removing the last element in the array (bin by front edge),
        # and then multiplying by the conversion factor
        # this is done by the absolute_time helper function
        mjd_array = absolute_time(expstart=dataframe['EXPSTART'][0],
                                  time=dataframe['TIME_3'][0][::timestep][:-1])
        dec_year_array = mjd_array.decimalyear

        return dark_rate_array, dec_year_array

    def track(self):
        # track something. perhaps current dark rate?
        if self.data is None:
            self.data = self.get_data()

        plotdf = pd.DataFrame({
            "DECIMAL_YEAR": self.data["DECIMAL_YEAR"],
            "DARK_RATE": self.data["DARK_RATE"]
            })

        # i can't get explode_df to work so this is for now
        # i think i know why it doesn't work and i fixed it i just haven't
        # switched to using it
        all_dec_year = []
        all_dark_rates = []
        for index, row in plotdf.iterrows():
            all_dec_year = list(
                itertools.chain(all_dec_year, row["DECIMAL_YEAR"]))
            all_dark_rates = list(
                itertools.chain(all_dark_rates, row["DARK_RATE"]))

        dark_counts = np.asarray(all_dark_rates)
        fig = plt.figure(figsize=(12, 9))
        bin_size = 1e-8
        n_bins = int((dark_counts.max() - dark_counts.min()) / bin_size)
        ax = fig.add_subplot(2, 1, 1)
        ax.hist(dark_counts, bins=n_bins, align='mid', histtype='stepfilled')
        counts, bins = np.histogram(dark_counts, bins=100)
        cuml_dist = np.cumsum(counts)
        count_99 = abs(cuml_dist / float(cuml_dist.max()) - .99).argmin()
        count_95 = abs(cuml_dist / float(cuml_dist.max()) - .95).argmin()

        mean = dark_counts.mean()
        med = np.median(dark_counts)
        std = dark_counts.std()
        mean_obj = ax.axvline(x=mean, lw=2, ls='--', color='r', label='Mean ')
        med_obj = ax.axvline(x=med, lw=2, ls='-', color='r', label='Median')
        two_sig = ax.axvline(x=med + (2 * std), lw=2, ls='-', color='gold')
        three_sig = ax.axvline(x=med + (3 * std), lw=2, ls='-',
                               color='DarkOrange')
        dist_95 = ax.axvline(x=bins[count_95], lw=2, ls='-',
                             color='LightGreen')
        dist_99 = ax.axvline(x=bins[count_99], lw=2, ls='-', color='DarkGreen')

        ax.grid(True, which='both')
        ax.set_title('Histogram of Dark Rates', fontsize=15, fontweight='bold')
        ax.set_ylabel('Frequency', fontsize=15, fontweight='bold')
        ax.set_xlabel('Counts/pix/sec', fontsize=15, fontweight='bold')
        ax.set_xlim(dark_counts.min(), dark_counts.max())
        ax.xaxis.set_major_formatter(FormatStrFormatter('%3.2e'))

        ax = fig.add_subplot(2, 1, 2)
        # log_bins = np.logspace(np.log10(dark.min()), np.log10(dark.max()),
        # 100)
        ax.hist(dark_counts, bins=n_bins, align='mid', log=True,
                histtype='stepfilled')

        ax.axvline(x=mean, lw=2, ls='--', color='r', label='Mean')
        ax.axvline(x=med, lw=2, ls='-', color='r', label='Median')
        ax.axvline(x=med + (2 * std), lw=2, ls='-', color='gold')
        ax.axvline(x=med + (3 * std), lw=2, ls='-', color='DarkOrange')
        ax.axvline(x=bins[count_95], lw=2, ls='-', color='LightGreen')
        ax.axvline(x=bins[count_99], lw=2, ls='-', color='DarkGreen')

        # ax.set_xscale('log')
        ax.grid(True, which='both')
        ax.set_ylabel('Log Frequency', fontsize=15, fontweight='bold')
        ax.set_xlabel('Counts/pix/sec', fontsize=15, fontweight='bold')
        ax.set_xlim(dark_counts.min(), dark_counts.max())
        ax.xaxis.set_major_formatter(FormatStrFormatter('%3.2e'))

        fig.legend([med_obj, mean_obj, two_sig, three_sig, dist_95, dist_99],
                   ['Median: {0:.2e}'.format(med),
                    'Mean: {0:.2e}'.format(mean),
                    r'2$\sigma$: {0:.2e}'.format(med + (2 * std)),
                    r'3$\sigma$: {0:.2e}'.format(med + (3 * std)),
                    r'95$\%$: {0:.2e}'.format(bins[count_95]),
                    r'99$\%$: {0:.2e}'.format(bins[count_99])], shadow=True,
                   numpoints=1, bbox_to_anchor=[0.8, 0.8])

    def plot(self):
        # select the important columns from the dataframe
        if self.data is None:
            self.data = self.get_data()

        plotdf = pd.DataFrame({
            "DECIMAL_YEAR": self.data["DECIMAL_YEAR"],
            "DARK_RATE": self.data["DARK_RATE"]
            })

        # i can't get explode_df to work so this is for now
        all_dec_year = []
        all_dark_rates = []
        for index, row in plotdf.iterrows():
            all_dec_year = list(
                itertools.chain(all_dec_year, row["DECIMAL_YEAR"]))
            all_dark_rates = list(
                itertools.chain(all_dark_rates, row["DARK_RATE"]))

        # not sure what is happening here tbh, still figuring it out
        #         self.x = all_dec_year
        #         self.y = all_dark_rates

        #         self.basic_scatter()

        fig = plt.figure(figsize=(12, 9))
        plt.scatter(all_dec_year, all_dark_rates)
        # plt.xlim(min(all_dec_year), max(all_dec_year))
        plt.ylim(0, max(all_dark_rates) + 0.5e-7)
        plt.ticklabel_format(axis='y', style='sci', scilimits=(-2, 2))
        plt.ticklabel_format(axis='x', style='plain')
        plt.xlabel("Decimal Year")
        plt.ylabel("Dark Rate (c/p/s)")
        plt.grid(True)

    def store_results(self):
        # need to store results if not going in the database
        pass
