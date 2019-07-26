import numpy as np
import plotly.graph_objs as go

from itertools import repeat
from monitorframe.monitor import BaseMonitor
from astropy.time import Time

from .acq_data_models import AcqImageModel, AcqPeakdModel, AcqPeakxdModel
from ..monitor_helpers import fit_line, convert_day_of_year
from .. import SETTINGS

COS_MONITORING = SETTINGS['output']


# TODO: Remove example monitors and finalize remaining ones.
class AcqImageMonitor(BaseMonitor):
    """Simple example monitor."""
    data_model = AcqImageModel
    labels = ['ROOTNAME', 'PROPOSID']
    output = '/Users/jwhite/Desktop/test.html'

    notification_settings = {
        'active': True,
        'username': 'jwhite',
        'recipients': 'jwhite@stsci.edu'
    }

    plottype = 'scatter'
    x = 'ACQSLEWX'
    y = 'ACQSLEWY'
    z = 'EXPSTART'

    def get_data(self):
        return self.model.new_data

    def track(self):
        """Return the magnitude of the slew offset."""
        return np.sqrt(self.data.ACQSLEWX ** 2 + self.data.ACQSLEWY ** 2)

    def find_outliers(self):
        """Return mask defining outliers as acqs whose slew is greater than 2 arcseconds."""
        return self.results >= 2

    def set_notification(self):
        return (
            f'{np.count_nonzero(self.outliers)} AcqImages were found to have a total slew of greater than 2 arcseconds'
        )


class AcqImageSlewMonitor(BaseMonitor):
    """Example ACQIMAGE scatter plot monitor."""
    name = 'AcImage Slew Monitor'
    data_model = AcqImageModel
    subplots = True
    subplot_layout = (2, 1)
    output = COS_MONITORING
    labels = ['ROOTNAME', 'PROPOSID']

    def get_data(self):
        return self.model.new_data

    def track(self):
        """Track the fit and fit line of offset v time for each FGS."""
        groups = self.data.groupby('dom_fgs')

        fit_results = {}
        for name, group in groups:
            x_fit, x_line = fit_line(group.EXPSTART, -group.ACQSLEWX)
            y_fit, y_line = fit_line(group.EXPSTART, -group.ACQSLEWY)

            fit_results[name] = (x_line, y_line, x_fit, y_fit)

        return groups, fit_results

    def plot(self):
        """Plot offset (-slew) v time for the dispersion direction (top panel) and cross-dispersion direction (bottom
        panel). Separate FGS by button option.
        """
        groups, fit_results = self.results

        traces = []
        rows = []
        cols = []
        visibility = {key: [] for key in groups.groups.keys()}
        for name, group in groups:
            x_line, y_line, x_fit, y_fit = fit_results[name]

            x_scatter = go.Scatter(
                x=group.EXPSTART,
                y=-group.ACQSLEWX,
                name=f'{name} Slew X',
                mode='markers',
                text=group.hover_text,
                visible=False

            )

            y_scatter = go.Scatter(
                x=group.EXPSTART,
                y=-group.ACQSLEWY,
                name=f'{name} Slew Y',
                mode='markers',
                text=group.hover_text,
                visible=False
            )

            x_line_fit = go.Scatter(
                x=group.EXPSTART,
                y=x_line,
                mode='lines',
                name=f'Fit:\nslope: {x_fit[1]:.5f}\nintercept: {x_fit[0]:.3f}',
                visible=False
            )

            y_line_fit = go.Scatter(
                x=group.EXPSTART,
                y=y_line,
                mode='lines',
                name=f'Fit:\nslope: {y_fit[1]:.5f}\nintercept: {y_fit[0]:.3f}',
                visible=False
            )

            traces.extend([x_scatter, y_scatter, x_line_fit, y_line_fit])

            rows.extend([1, 2, 1, 2])  # Placement of the traces on the figure are in the order that they're created.
            cols.extend([1, 1, 1, 1])

            # Iteratively create visibility settings
            for key in visibility.keys():
                if key == name:
                    visibility[key].extend([True, True, True, True])

                else:
                    visibility[key].extend([False, False, False, False])

        # Create FGS buttons
        updatemenus = [
            dict(
                active=15,
                buttons=[
                    dict(
                        label=label,
                        method='update',
                        args=[
                            {'visible': visibility[fgs]},
                            {'title': f'{label} Slew vs Time {self.date.date().isoformat()}'}
                        ]
                    ) for label, fgs in zip(['FGS1', 'FGS2', 'FGS3'], ['F1', 'F2', 'F3'])
                ]
            ),
        ]

        # Create layout
        layout = go.Layout(updatemenus=updatemenus, hovermode='closest')

        self.figure.add_traces(traces, rows=rows, cols=cols)
        self.figure.update_layout(layout)


class AcqImageFGSMonitor(BaseMonitor):
    """ACQIMAGE FGS monitor."""
    name = 'AcqImage FGS Monitor'
    data_model = AcqImageModel
    labels = ['ROOTNAME', 'PROPOSID']
    output = COS_MONITORING

    def get_data(self):
        return self.model.new_data

    def track(self):
        """Track the average offset (-slew) for x and y directions per FGS."""
        groups = self.data.groupby('dom_fgs')
        return groups, -groups.ACQSLEWX.mean(), -groups.ACQSLEWY.mean()

    def plot(self):
        """Plot a scatter plot of y vs x offsets (-slews) per FGS. Separate FGS via button options."""
        groups, mean_x, mean_y = self.results

        average_text = {
            fgs: f'<br>Mean offset in X: {mean_x[fgs]:.3f}<br>Mean offset in Y: {mean_y[fgs]:.3f}'
            for fgs in self.data.dom_fgs.unique()
        }

        traces = []
        for name, group in groups:
            traces.append(
                go.Scatter(
                    x=-group.ACQSLEWX,
                    y=-group.ACQSLEWY,
                    mode='markers',
                    marker=dict(
                        color=group.EXPSTART,
                        colorscale='Viridis',
                        colorbar=dict(len=0.75),
                        showscale=True,
                    ),
                    name=name,
                    text=group.hover_text,
                    visible=False
                )
            )

        # Add vertical line for the mean x offset and a horizontal line for the mean y offset
        lines = {
            fgs: [
                {
                    'type': 'line',
                    'x0': mean_x[fgs],
                    'y0': 0,
                    'x1': mean_x[fgs],
                    'y1': 1,
                    'yref': 'paper',
                    'line': {
                        'color': 'red',
                        'width': 3,
                    }
                },

                {
                    'type': 'line',
                    'x0': 0,
                    'y0': mean_y[fgs],
                    'x1': 1,
                    'y1': mean_y[fgs],
                    'xref': 'paper',
                    'line': {
                        'color': 'red',
                        'width': 3,
                    },
                }
            ]
            for fgs in self.data.dom_fgs.unique()
        }

        # Add FGS buttons
        updatemenus = [
            dict(
                active=10,
                buttons=[
                    dict(
                        label='FGS1',
                        method='update',
                        args=[
                            {'visible': [True, False, False]},
                            {'title': 'FGS1' + average_text['F1'], 'shapes': lines['F1']}
                        ]
                    ),
                    dict(
                        label='FGS2',
                        method='update',
                        args=[
                            {'visible': [False, True, False]},
                            {'title': 'FGS2' + average_text['F2'], 'shapes': lines['F2']}
                        ]
                    ),
                    dict(
                        label='FGS3',
                        method='update',
                        args=[
                            {'visible': [False, False, True]},
                            {'title': 'FGS3' + average_text['F3'], 'shapes': lines['F3']}
                        ]
                    )
                ]
            ),
        ]

        # Create layout
        layout = go.Layout(updatemenus=updatemenus, hovermode='closest')

        self.figure.add_traces(traces)
        self.figure.update_layout(layout)

    def store_results(self):
        pass


class AcqImageV2V3Monitor(BaseMonitor):
    """V2V3 Offset Monitor."""
    name = 'V2V3 Offset Monitor'
    data_model = AcqImageModel
    labels = ['ROOTNAME', 'PROPOSID']
    subplots = True
    subplot_layout = (2, 1)
    output = COS_MONITORING

    # Define break points for fitting lines; these correspond to important catalogue or FGS dates.
    break_points = {
        'F1': [
            (None, 2011.172),
            (2011.172, 2013.205),  # FGS realignment
            (2013.205, 2014.055),  # FGS realignment
            (2014.055, None)  # SIAF update
        ],

        'F2': [
            (None, 2011.172),
            (2011.206, 2013.205),  # FGS2 turned back on + FGS realignment
            (2013.205, 2014.055),  # FGS realignment
            (2014.055, 2015.327),  # SIAF update
            (2016.123, None)
        ],

        'F3': []  # No current break points for F3 yet
    }

    # Define important events for vertical line placement.
    fgs_events = {
        'FGS Realignment 1': 2011.172,
        'FGS2 Activated': 2011.206,
        'FGS Realignment 2': 2013.205,
        'SIAF Update': 2014.055,
        'FGS2 Deactivated': 2015.327,
        'FGS2 Reactivated': 2016.123,
        'GAIA Guide Stars': 2017.272
    }

    def get_data(self):
        """Filter ACQIMAGE data for V2V3 plot. These filter options attempt to weed out outliers that might result from
        things besides FGS trends (such as bad coordinates).
        """
        data = self.model.new_data
        # Filters determined by the team. These options are meant to filter out most outliers to study FGS zero-point
        # offsets and rate of change with time.
        index = np.where(
            (data.OBSTYPE == 'IMAGING') &
            (data.NEVENTS >= 2000) &
            (np.sqrt(data.V2SLEW ** 2 + data.V3SLEW ** 2) < 2) &
            (data.SHUTTER == 'Open') &
            (data.LAMPEVNT >= 500) &
            (data.ACQSTAT == 'Success') &
            (data.EXTENDED == 'NO')
        )

        partially_filtered = data.iloc[index]

        # Filter on LINENUM endswith 1 as this indicates that it was the first ACQIMAGE taken in the set (it's a good
        # bet that this is the first ACQ, which you want to sample "blind pointings").
        filtered_df = partially_filtered[partially_filtered.LINENUM.str.endswith('1')]

        return filtered_df.sort_values('EXPSTART').reset_index(drop=True)

    def track(self):
        """Track the fit and fit-line for the period since the last FGS alignment."""
        groups = self.data.groupby('dom_fgs')

        last_updated_results = {}
        for name, group in groups:
            if name == 'F3':  # Skip F3; not enough data points for meaningful analysis for now.
                continue

            t_start = convert_day_of_year(self.break_points[name][-1][0]).mjd  # Last update date

            df = self.data[self.data.EXPSTART >= t_start]

            # Track V2V3 fit and fit-line since the last update for each FGS
            v2_line_fit = fit_line(df.EXPSTART, df.V2SLEW)
            v3_line_fit = fit_line(df.EXPSTART, df.V3SLEW)

            last_updated_results[name] = (v2_line_fit, v3_line_fit)

        return groups, last_updated_results

    def _create_traces(self, df, slew, n_breakpoints, rows, cols):
        """Create V2V3 traces for the monitor figure."""
        time = Time(df.EXPSTART, format='mjd')
        dt = time - time[0]
        line_fit, fit = fit_line(dt.sec, -df[slew])

        scatter = go.Scatter(  # scatter plot
            x=time.to_datetime(),
            y=-df[slew],
            mode='markers',
            hovertext=df.hover_text,
            visible=False,
            legendgroup=f'Group {n_breakpoints + 1}',
            name=f'{slew.strip("SLEW")} Group {n_breakpoints + 1}'
        )

        line = go.Scatter(  # line-fit plot
            x=time.to_datetime(),
            y=fit,
            name=(
                f'Slope: {line_fit[1] * 3.154e+7:.4f} arcsec/year<br>Zero Point: {fit[0]:.3f} arcsec'
            ),
            visible=False,
            legendgroup=f'Group {n_breakpoints + 1}'
        )

        self.figure.add_traces([scatter, line], rows=rows, cols=cols)

    def plot(self):
        """Plot V2 and V3 offset (-slew) vs time per 'breakpoint' period and per FGS. Separate FGS via a button option.
        V2 will be plotted in the top panel and V3 will be plotted in the bottom panel.
        """
        fgs_groups, _ = self.results  # retrieve the groups already found in track.

        for name, group in fgs_groups:
            # Skip FGS3; there are not enough data-points for meaningful analysis
            if name == 'F3':
                continue

            # Filter dataframe by time per breakpoint
            for n_breaks, points in enumerate(self.break_points[name]):
                t_start, t_end = points

                if t_start is None:
                    df = group[group.EXPSTART <= convert_day_of_year(t_end).mjd]

                elif t_end is None:
                    df = group[group.EXPSTART >= convert_day_of_year(t_start).mjd]

                else:
                    df = group.iloc[
                        np.where(
                            (group.EXPSTART >= convert_day_of_year(t_start).mjd) &
                            (group.EXPSTART <= convert_day_of_year(t_end).mjd)
                        )
                    ]

                if df.empty:  # Sometimes there may be no data; For example, FGS2 was not used for a while
                    continue

                # Plot V2 and V3 offsets v time
                for i, slew in enumerate(['V2SLEW', 'V3SLEW']):
                    self._create_traces(df, slew, n_breaks, rows=[i+1] * 2, cols=[1] * 2)

        # Create vertical lines
        lines = [
            {
                'type': 'line',
                'x0': convert_day_of_year(value).to_datetime(),
                'y0': self.figure['layout'][y_axis]['domain'][0],
                'x1': convert_day_of_year(value).to_datetime(),
                'y1': self.figure['layout'][y_axis]['domain'][1],
                'xref': xref,
                'yref': 'paper',
                'line': {
                    'width': 3,
                },
                'name': key
            } for key, value in self.fgs_events.items() for xref, y_axis in zip(['x1', 'x2'], ['yaxis1', 'yaxis2'])
        ]

        annotations = [
            {
                'x': convert_day_of_year(item[1]).to_datetime(),
                'y': self.figure.layout[yaxis]['domain'][1],
                'xref': xref,
                'yref': 'paper',
                'text': item[0],
                'showarrow': True,
                'ax': ax,
                'ay': -30,
            } for item, ax in zip(self.fgs_events.items(), [-60, 50, -20, 20, -50, 20, 50])
            for xref, yaxis in zip(['x1', 'x2'], ['yaxis1', 'yaxis2'])
        ]

        # Create visibility toggles for buttons
        n_f1_traces = len(self.break_points['F1']) * 4  # There are four traces plotted per breakpoint
        n_f2_traces = len(self.break_points['F2']) * 4

        # F1 traces are created first, so the order for the list of traces is f1 traces then f2 traces
        f1_visibility = list(repeat(True, n_f1_traces)) + list(repeat(False, n_f2_traces))
        f2_visibility = list(repeat(False, n_f1_traces)) + list(repeat(True, n_f2_traces))

        # Create buttons
        updatemenus = [
            go.layout.Updatemenu(
                active=-1,
                buttons=[
                    dict(
                        label='FGS1',
                        method='update',
                        args=[
                            {'visible': f1_visibility},
                            {
                                'title': 'FGS1 V2V3 Slew vs Time',
                                'annotations': annotations,
                                'shapes': lines
                            }
                        ]
                    ),
                    dict(
                        label='FGS2',
                        method='update',
                        args=[
                            {'visible': f2_visibility},
                            {
                                'title': 'FGS2 V2V3 Slew vs Time',
                                'annotations': annotations,
                                'shapes': lines
                            }
                        ]
                    ),
                ]
            ),
        ]

        # Create layout
        layout = go.Layout(
            updatemenus=updatemenus,
            hovermode='closest',
            xaxis=dict(title='Datetime'),
            xaxis2=dict(title='Datetime'),
            yaxis=dict(title='V2 Offset (-Slew) [arcseconds]'),
            yaxis2=dict(title='V3 Offset (-Slew) [arcseconds]'),
            legend=dict(tracegroupgap=15)
        )
        self.figure.update_layout(layout)

    def store_results(self):
        # TODO: define what results to store and how
        pass


class SpecAcqBaseMonitor(BaseMonitor):
    """Base monitor class for the spectroscopic Acq types: PEAKD and PEAKXD"""
    labels = ['ROOTNAME', 'PROPOSID']
    output = COS_MONITORING
    slew = None

    def get_data(self):
        return self.model.new_data

    def track(self):
        """Track the standard deviation of the slew per FGS."""
        groups = self.data.groupby('dom_fgs')
        scatter = groups[self.slew].std()

        return groups, scatter

    def plot(self):
        """Plot offset (-slew) v time per FGS. Separate FGS via button options. Color by LP-POS"""
        fgs_groups, std_results = self.results  # groups are stored in the results attribute since track returns them.

        traces = []
        for name, group in fgs_groups:
            scatter = go.Scatter(  # Scatter plot
                x=group.EXPSTART,
                y=-group[self.slew],
                mode='markers',
                text=group.hover_text,
                visible=False,
                marker=dict(
                    color=group.LIFE_ADJ,
                    colorscale='Viridis',
                    colorbar=dict(  # Color by LP and set labels to typical LP names
                        len=0.75,
                        tickmode='array',
                        nticks=len(group.LIFE_ADJ.unique()),
                        tickvals=group.LIFE_ADJ.unique(),
                        ticktext=[f'LP{l}' for l in group.LIFE_ADJ.unique()]
                    ),
                    showscale=True,
                ),
                name=name,
            )

            traces.append(scatter)

        fgs_labels = ['FGS1', 'FGS2', 'FGS3']
        visibility = [np.roll([True, False, False], index) for index in range(len(fgs_labels))]
        title = f'{self.name}; Slew vs Time'

        # Create buttons
        updatemenus = [
            dict(
                active=10,
                buttons=[
                    dict(
                        label=fgs,
                        method='update',
                        args=[
                            {'visible': visible},
                            {'title': f'{fgs} {title}'}
                        ]
                    ) for fgs, visible in zip(fgs_labels, visibility)
                ]
            )
        ]

        # Create layout
        layout = go.Layout(updatemenus=updatemenus, hovermode='closest')

        self.figure.add_traces(traces)
        self.figure.update_layout(layout)

    def store_results(self):
        # TODO: Define what to store
        pass


class AcqPeakdMonitor(SpecAcqBaseMonitor):
    """ACQPEAKD monitor."""
    name = 'AcqPeakd Monitor'
    data_model = AcqPeakdModel
    slew = 'ACQSLEWX'


class AcqPeakxdMonitor(SpecAcqBaseMonitor):
    """ACQPEAKXD monitor."""
    name = 'AcqPeakxd Monitor'
    data_model = AcqPeakxdModel
    slew = 'ACQSLEWY'
