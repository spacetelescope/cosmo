import numpy as np
import plotly.graph_objs as go

from itertools import repeat

from monitorframe import BaseMonitor
from .acq_data_models import AcqImageModel, AcqPeakdModel, AcqPeakxdModel
from cosmo.monitor_helpers import fit_line, convert_day_of_year

COS_MONITORING = '/grp/hst/cos2/monitoring'


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

    def define_plot(self):
        self.plottype = 'scatter'
        self.x = -self.data.ACQSLEWX
        self.y = -self.data.ACQSLEWY
        self.z = self.data.EXPSTART


class AcqImageSlewMonitor(BaseMonitor):
    """Example ACQIMAGE scatter plot monitor."""
    name = 'AcImage Slew Monitor'
    data_model = AcqImageModel
    subplots = True
    subplot_layout = (2, 1)
    output = COS_MONITORING
    labels = ['ROOTNAME', 'PROPOSID']

    def track(self):
        """Track the fit and fit line of offset v time for each FGS."""
        groups = self.data.groupby('dom_fgs')

        fit_results = {}
        for name, group in groups:
            xfit, xline = fit_line(group.EXPSTART, -group.ACQSLEWX)
            yfit, yline = fit_line(group.EXPSTART, -group.ACQSLEWY)

            fit_results[name] = (xline, yline, xfit, yfit)

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
            xline, yline, xfit, yfit = fit_results[name]

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

            xline_fit = go.Scatter(
                x=group.EXPSTART,
                y=xline,
                mode='lines',
                name=f'Fit:\nslope: {xfit[1]:.5f}\nintercept: {xfit[0]:.3f}',
                visible=False
            )

            yline_fit = go.Scatter(
                x=group.EXPSTART,
                y=yline,
                mode='lines',
                name=f'Fit:\nslope: {yfit[1]:.5f}\nintercept: {yfit[0]:.3f}',
                visible=False
            )

            traces.extend([x_scatter, y_scatter, xline_fit, yline_fit])

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
        self.figure['layout'].update(layout)


class AcqImageFGSMonitor(BaseMonitor):
    """ACQIMAGE FGS monitor."""
    name = 'AcqImage FGS Monitor'
    data_model = AcqImageModel
    labels = ['ROOTNAME', 'PROPOSID']
    output = COS_MONITORING

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
        self.figure['layout'].update(layout)

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
            ('start', 2011.172),
            (2011.172, 2013.205),  # FGS realignment
            (2013.205, 2014.055),  # FGS realignment
            (2014.055, 'end')  # SIAF update
        ],

        'F2': [
            ('start', 2011.172),
            (2011.206, 2013.205),  # FGS2 turned back on + FGS realignment
            (2013.205, 2014.055),  # FGS realignment
            (2014.055, 2015.327),  # SIAF update
            (2016.123, 'end')
        ],

        'F3': []  # No current break points for F3 yet
    }

    # Define important events for vertical line placement.
    fgs_events = {
        'FGS realignment 1': 2011.172,
        'FGS2 turned on': 2011.206,
        'FGS realignment 2': 2013.205,
        'SIAF update': 2014.055,
        'FGS2 turned off': 2015.327,
        'FGS2 turned back on': 2016.123,
        'GAIA guide stars': 2017.272
    }

    def filter_data(self):
        """Filter ACQIMAGE data for V2V3 plot. These filter options attempt to weed out outliers that might result from
        things besides FGS trends (such as bad coordinates).
        """
        index = np.where(
            (self.data.OBSTYPE == 'IMAGING') &
            (self.data.NEVENTS >= 2000) &
            (np.sqrt(self.data.V2SLEW ** 2 + self.data.V3SLEW ** 2) < 2) &
            (self.data.SHUTTER == 'Open') &
            (self.data.LAMPEVNT >= 500) &
            (self.data.ACQSTAT == 'Success') &
            (self.data.EXTENDED == 'NO')
        )

        partially_filtered = self.data.iloc[index]
        filtered_df = partially_filtered[partially_filtered.LINENUM.str.endswith('1')]

        return filtered_df

    def track(self):
        """Track the fit and fit-line for the period since the last FGS alignment."""
        groups = self.filtered_data.groupby('dom_fgs')

        last_updated_results = {}
        for name, group in groups:
            if name == 'F3':
                continue

            t_start = convert_day_of_year(self.break_points[name][-1][0], mjd=True)

            df = self.filtered_data[self.filtered_data.EXPSTART >= t_start]

            v2_line_fit = fit_line(df.EXPSTART, df.V2SLEW)
            v3_line_fit = fit_line(df.EXPSTART, df.V3SLEW)

            last_updated_results[name] = (v2_line_fit, v3_line_fit)

        return groups, last_updated_results

    def plot(self):
        """Plot V2 and V3 offset (-slew) vs time per 'breakpoint' period and per FGS. Separate FGS via a button option.
        V2 will be plotted in the top panel and V3 will be plotted in the bottom panel.
        """
        fgs_groups, _ = self.results  # retrive the groups already found in track.

        # TODO: Refactor how traces are collected and visibility options for the buttons are created.
        traces = {'F1': [], 'F2': []}  # store traces per fgs
        rows = []
        cols = []
        for name, group in fgs_groups:

            # Skip FGS3; there are not enough datapoints for meaningful analysis
            if name == 'F3':
                continue

            # Plot offset v time per breakpoint
            for points in self.break_points[name]:

                t_start, t_end = [
                    convert_day_of_year(point, mjd=True) if not isinstance(point, str) else None
                    for point in points
                ]

                if t_start is None:
                    df = group[group.EXPSTART <= t_end]

                elif t_end is None:
                    df = group[group.EXPSTART >= t_start]

                else:
                    df = group.iloc[np.where((group.EXPSTART >= t_start) & (group.EXPSTART <= t_end))]

                if df.empty:  # Sometimes there may be no data; For exampel, FGS2 was not used for a while
                    continue

                # Plot V2 and V3 offsets v time
                for i, slew in enumerate(['V2SLEW', 'V3SLEW']):
                    rows.append(i + 1)
                    rows.append(i + 1)
                    cols.append(1)
                    cols.append(1)

                    line_fit, fit = fit_line(df.EXPSTART, -df[slew])

                    scatter = go.Scatter(
                        x=df.EXPSTART,
                        y=-df[slew],
                        name=slew,
                        mode='markers',
                        text=df.hover_text,
                        visible=False
                    )

                    line = go.Scatter(
                        x=df.EXPSTART,
                        y=fit,
                        name=f'Slope: {line_fit[1]:.5f}; Zero Point: {fit[0]:.3f}',
                        visible=False
                    )

                    traces[name].append(scatter)
                    traces[name].append(line)

        self.figure.add_traces([item for sublist in traces.values() for item in sublist], rows=rows, cols=cols)

        # Create vertical lines
        lines = [
            {
                'type': 'line',
                'x0': convert_day_of_year(value, mjd=True),
                'y0': self.figure['layout'][yaxis]['domain'][0],
                'x1': convert_day_of_year(value, mjd=True),
                'y1': self.figure['layout'][yaxis]['domain'][1],
                'xref': xref,
                'yref': 'paper',
                'line': {
                    'width': 3,
                }
            } for value in self.fgs_events.values() for xref, yaxis in zip(['x1', 'x2'], ['yaxis1', 'yaxis2'])
        ]

        f1_visibility = list(repeat(True, len(traces['F1']))) + list(repeat(False, len(traces['F2'])))
        f2_visibility = list(repeat(False, len(traces['F1']))) + list(repeat(True, len(traces['F2'])))

        # Create buttons
        updatemenus = [
            dict(
                active=50,
                buttons=[
                    dict(
                        label='FGS1',
                        method='update',
                        args=[
                            {'visible': f1_visibility},
                            {'title': 'FGS1 V2V3 Slew vs Time'}
                        ]
                    ),
                    dict(
                        label='FGS2',
                        method='update',
                        args=[
                            {'visible': f2_visibility},
                            {'title': 'FGS2 V2V3 Slew vs Time'}
                        ]
                    ),
                ]
            ),
        ]

        # Create layout
        layout = go.Layout(updatemenus=updatemenus, hovermode='closest', shapes=lines)

        self.figure['layout'].update(layout)

    def store_results(self):
        # TODO: define what results to store and how
        pass


class AcqPeakdMonitor(BaseMonitor):
    """ACQPEAKD monitor."""
    name = 'AcqPeakd Monitor'
    data_model = AcqPeakdModel
    labels = ['ROOTNAME', 'PROPOSID']
    output = COS_MONITORING

    def track(self):
        """Track the standard deviation of the slew per FGS."""
        groups = self.data.groupby('dom_fgs')
        scatter = groups.ACQSLEWX.std()

        return groups, scatter

    def plot(self):
        """Plot offset (-slew) v time per FGS. Separate FGS via button options. Color by LP-POS"""
        fgs_groups, std_results = self.results

        traces = []
        for name, group in fgs_groups:
            scatter = go.Scatter(
                x=group.EXPSTART,
                y=-group.ACQSLEWX,
                mode='markers',
                text=group.hover_text,
                visible=False,
                marker=dict(
                    color=group.LIFE_ADJ,
                    colorscale='Viridis',
                    colorbar=dict(
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
        title = 'AcqPeakd Slew vs Time'

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
        self.figure['layout'].update(layout)


class AcqPeakxdMonitor(BaseMonitor):
    """ACQPEAKXD monitor."""
    name = 'AcqPeakxd Monitor'
    data_model = AcqPeakxdModel
    labels = ['ROOTNAME', 'PROPOSID']
    output = COS_MONITORING

    def track(self):
        """Track the standard deviation of the slew per FGS."""
        groups = self.data.groupby('dom_fgs')
        scatter = groups.ACQSLEWY.std()

        return groups, scatter

    def plot(self):
        """Plot offset (-slew) v time per FGS. Separate FGS via button options. Color by LP-POS"""
        fgs_groups, std_results = self.results

        traces = []
        for name, group in fgs_groups:
            scatter = go.Scatter(
                x=group.EXPSTART,
                y=-group.ACQSLEWY,
                mode='markers',
                text=group.hover_text,
                visible=False,
                marker=dict(
                    color=group.LIFE_ADJ,
                    colorscale='Viridis',
                    colorbar=dict(
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
        title = 'AcqPeakxd Slew vs Time'

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
        self.figure['layout'].update(layout)
