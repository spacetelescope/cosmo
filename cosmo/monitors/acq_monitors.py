import numpy as np
import plotly.graph_objs as go
import plotly.express as px
import datetime
import pandas as pd

from peewee import Model
from monitorframe.monitor import BaseMonitor
from astropy.time import Time
from typing import List, Union

from .data_models import AcqDataModel
from ..monitor_helpers import fit_line, convert_day_of_year, create_visibility, v2v3
from .. import SETTINGS

COS_MONITORING = SETTINGS['output']


def select_all_acq(model: Union[Model, None], exptype: str, new_data_df: pd.DataFrame = None) -> pd.DataFrame:
    """Get all ingested acq data of a particular exptype and combine it with any new data found."""
    data = pd.DataFrame()

    if model is not None:
        data = data.append(
            pd.DataFrame(model.select().where(model.EXPTYPE == exptype).dicts()),
            sort=True,
            ignore_index=True
        )

    if new_data_df is None:
        return data

    if not new_data_df.empty:
        new_data = new_data_df[new_data_df.EXPTYPE == exptype].reset_index(drop=True)
        data = data.append(new_data, sort=True, ignore_index=True)

    return data


class AcqImageMonitor(BaseMonitor):
    data_model = AcqDataModel
    docs = "https://spacetelescope.github.io/cosmo/monitors.html#acqimage-monitor"
    labels = ['ROOTNAME', 'PROPOSID', 'FGS']
    output = COS_MONITORING

    run = 'monthly'

    def get_data(self):
        data = select_all_acq(self.model.model, 'ACQ/IMAGE', self.model.new_data)

        # Add configuration column which is a combination of aperture-grating/mirror
        data['configuration'] = data.APERTURE.str.cat(data.OPT_ELEM, sep='-')

        return data

    def track(self):
        """Track the total offset (or slew) distance and basic statistics on x and y slews."""
        return {
            'distance': np.sqrt(self.data.ACQSLEWX ** 2 + self.data.ACQSLEWY ** 2),
            'stats': self.data.groupby('configuration')[['ACQSLEWX', 'ACQSLEWY']].describe()
        }

    def find_outliers(self):
        """Find offsets of 2 arcseconds or larger."""
        return {
            'slews': self.results['distance'] >= 2,
            'failed': self.data.ACQSTAT == 'Failure',
            'closed': self.data.SHUTTER == 'Closed'
        }

    def plot(self):
        # Filter out shutter closed and failed exposures
        filtered = self.data[~(self.outliers['failed']) & ~(self.outliers['closed'])]

        self.figure = px.scatter(
            filtered,
            x='ACQSLEWX',
            y='ACQSLEWY',
            color='configuration',
            hover_data=self.labels,
            title=f'<a href="{self.docs}">{self.name}</a>',
            height=900,
            marginal_x='histogram',
            marginal_y='histogram',
        )

        outliers = self.data[self.outliers['slews']]

        self.figure.add_trace(
            go.Scatter(
                x=outliers.ACQSLEWX,
                y=outliers.ACQSLEWY,
                mode='markers',
                marker_color='red',
                marker_size=10,
                hovertext=outliers.hover_text,
                name='Outliers'
            )
        )

        self.figure.update_layout(
            xaxis_title='ACQSLEWX [pix]',
            yaxis_title='ACQSLEWY [pix]'
        )

    def store_results(self):
        pass


class AcqImageV2V3Monitor(BaseMonitor):
    """V2V3 Offset Monitor."""
    name = 'V2V3 Offset Monitor'
    data_model = AcqDataModel
    docs = "https://spacetelescope.github.io/cosmo/monitors.html#fgs-monitoring-v2v3-offset-monitor"
    labels = ['ROOTNAME', 'PROPOSID']

    subplots = True
    subplot_layout = (2, 1)
    output = COS_MONITORING

    run = 'monthly'

    # Define break points for fitting lines; these correspond to important catalogue or FGS dates.
    # TODO Refactor this info into a better, more concise data structure
    break_points = {
        'F1': [
            (None, 2011.172),  # FGS realignment
            (2011.172, 2013.205),  # FGS realignment
            (2013.205, 2014.055),  # SIAF update
            (2014.055, 2019.352),  # FGS realignment
            (2019.352, 2020.150),  # FHST realignment
            (2020.150, None)
        ],

        'F2': [
            (None, 2013.205),  # FGS2 turned back on + FGS realignment
            (2013.205, 2014.055),  # FGS realignment
            (2014.055, 2015.327),  # SIAF update
            (2016.123, 2019.352),  # FGS realignment
            (2019.352, 2020.150),  # FHST realignment
            (2020.150, None)
        ],

        'F3': [(None, 2019.352), (2019.352, 2020.150), (2020.150, None)]
    }

    # Define important events for vertical line placement.
    fgs_events = {
        'FGS Realignment 1': 2011.172,
        'FGS2 Activated': 2011.206,
        'FGS Realignment 2': 2013.205,
        'SIAF Update': 2014.055,
        'FGS2 Deactivated': 2015.327,
        'FGS2 Reactivated': 2016.123,
        'GAIA Guide Stars': 2017.272,
        'FGS Realignment 3': 2019.352,
        'FHST Alignment': 2020.150
    }

    fgs1_breaks = ['FGS Realignment 1', 'FGS Realignment 2', 'SIAF Update', 'FGS Realignment 3', 'FHST Alignment']
    fgs2_breaks = ['FGS Realignment 2', 'SIAF Update', 'FGS2 Deactivated', 'FGS2 Reactivated', 'FGS Realignment 3',
                   'FHST Alignment']
    fgs3_breaks = ['FGS Realignment 3', 'FHST Alignment']

    def get_data(self):
        """Filter ACQIMAGE data for V2V3 plot. These filter options attempt to weed out outliers that might result from
        things besides FGS trends (such as bad coordinates).
        """
        data = select_all_acq(self.model.model, 'ACQ/IMAGE', self.model.new_data)
        data['V2SLEW'], data['V3SLEW'] = v2v3(data.ACQSLEWX, data.ACQSLEWY)

        # Filters determined by the team.
        # These options are meant to filter out most outliers to study FGS zero-point offsets and rate of change with
        # time.
        # Filter on LINENUM endswith 1 as this indicates that it was the first ACQIMAGE taken in the set (it's a good
        # bet that this is the first ACQ, which you want to sample "blind pointings").
        filtered_df = data[
            (data.OBSTYPE == 'IMAGING') &
            (data.NEVENTS >= 2000) &
            (np.sqrt(data.V2SLEW ** 2 + data.V3SLEW ** 2) < 2) &
            (data.SHUTTER == 'Open') &
            (data.LAMPEVNT >= 500) &
            (data.ACQSTAT == 'Success') &
            (data.EXTENDED == 'NO') &
            (data.LINENUM.str.endswith('1'))
            ]

        return filtered_df.sort_values('EXPSTART').reset_index(drop=True)

    def track(self):
        """Track the fit and fit-line for the period since the last FGS alignment."""
        groups = self.data.groupby('FGS')

        last_updated_results = {}
        for name, group in groups:
            t_start = convert_day_of_year(self.break_points[name][-1][0]).mjd  # Last update date

            df = group[group.EXPSTART >= t_start]

            if df.empty:
                continue

            # Track V2V3 fit and fit-line since the last update for each FGS
            v2_fit, v2_line = fit_line(Time(df.EXPSTART, format='mjd').byear, -df.V2SLEW)
            v3_fit, v3_line = fit_line(Time(df.EXPSTART, format='mjd').byear, -df.V3SLEW)

            # last_updated_results[name] = (v2_line_fit, v3_line_fit)
            last_updated_results[name] = {
                'V2': {'slope': v2_fit[1], 'start': v2_line[0], 'end': v2_line[-1]},
                'V3': {'slope': v3_fit[1], 'start': v3_line[0], 'end': v3_line[-1]}
            }

        return groups, last_updated_results

    def set_notification(self):
        """Set the notification to report line fit results for the last breakpoint group for V2 and V3 for each FGS.

        Example Notification
        --------------------
        V2V3 Offset Monitor 2019-07-31 Results

        FGS1 2014-02-24 - 2019-07-31 (Time of the most recent break point to now)
        V2:
            Slope: -0.0191 arcseconds/year
            Offset (from fit) at time of first data point: -0.061 arcseconds
            Offset (from fit) at time of last data point: -0.164 arcseconds

        V3:
            ...

        FGS2 ...
         ...
        """
        _, results = self.results
        notification = f'{self.name} Results\n\n'

        for fgs, v2v3_results in results.items():
            notification += (
                f'{fgs} {convert_day_of_year(self.break_points[fgs][-1][0]).to_datetime()} - {self.date} (Time of the '
                f'most recent break point to now)\n'
            )

            for direction, values in v2v3_results.items():
                notification += (
                    f'{direction}:\n'
                    f'\tSlope: {values["slope"]:.4f} arcseconds/year\n'
                    f'\tOffset (from fit) at time of first data point: {values["start"]:.3f} arcseconds\n'
                    f'\tOffset (from fit) at time of last data point: {values["end"]:.3f} arcseconds\n\n'
                )

        return notification

    def _create_traces(self, df: pd.DataFrame, breakpoint_index: int):
        """Create V2V3 traces for the monitor figure."""
        for i, slew in enumerate(['V2SLEW', 'V3SLEW']):
            time = Time(df.EXPSTART, format='mjd')
            line_fit, fit = fit_line(time.byear, -df[slew])

            scatter = go.Scatter(  # scatter plot
                x=time.to_datetime(),
                y=-df[slew],
                mode='markers',
                hovertext=df.hover_text,
                visible=False,
                legendgroup=f'Group {breakpoint_index + 1}',
                name=f'{slew.strip("SLEW")} Group {breakpoint_index + 1}'
            )

            line = go.Scatter(  # line-fit plot
                x=time.to_datetime(),
                y=fit,
                name=(
                    f'Slope: {line_fit[1]:.4f} arcsec/year<br>Offset (from fit) at time of first data point: '
                    f'{fit[0]:.3f}<br>'
                ),
                visible=False,
                legendgroup=f'Group {breakpoint_index + 1}',
                line=dict(width=4)
            )

            self.figure.add_traces([scatter, line], rows=[i + 1] * 2, cols=[1] * 2)

    def _create_breakpoint_lines(self, fgs_breakpoint_list: List[str]) -> List[dict]:
        return [
            {
                'type': 'line',
                'x0': convert_day_of_year(self.fgs_events[key]).to_datetime(),
                'y0': self.figure['layout'][y_axis]['domain'][0],
                'x1': convert_day_of_year(self.fgs_events[key]).to_datetime(),
                'y1': self.figure['layout'][y_axis]['domain'][1],
                'xref': xref,
                'yref': 'paper',
                'line': {
                    'width': 3,
                    'color': 'lightsteelblue',
                    'dash': 'dash'
                },
            } for key in fgs_breakpoint_list for xref, y_axis in zip(['x1', 'x2'], ['yaxis1', 'yaxis2'])
        ]

    def plot(self):
        """Plot V2 and V3 offset (-slew) vs time per 'breakpoint' period and per FGS. Separate FGS via a button option.
        V2 will be plotted in the top panel and V3 will be plotted in the bottom panel.
        """
        fgs_groups, _ = self.results  # retrieve the groups already found in track.

        traces_per_fgs = {'F1': 0, 'F2': 0, 'F3': 0}

        for name, group in fgs_groups:
            # Filter dataframe by time per breakpoint
            for i_breaks, points in enumerate(self.break_points[name]):
                t_start, t_end = points

                if t_start is None:
                    df = group[group.EXPSTART <= convert_day_of_year(t_end).mjd]

                elif t_end is None:
                    df = group[group.EXPSTART >= convert_day_of_year(t_start).mjd]

                else:
                    df = group[
                        (group.EXPSTART >= convert_day_of_year(t_start).mjd) &
                        (group.EXPSTART <= convert_day_of_year(t_end).mjd)
                        ]

                if df.empty:  # Sometimes there may be no data; For example, FGS2 was not used for a while
                    continue

                # Plot V2 and V3 offsets v time
                self._create_traces(df, i_breaks)
                traces_per_fgs[name] += 4  # There are four plots created with each call to _create_traces

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

        # Create vertical lines that are a different style for breakpoints (per FGS)
        fgs1_breaks = self._create_breakpoint_lines(self.fgs1_breaks)
        fgs2_breaks = self._create_breakpoint_lines(self.fgs2_breaks)
        fgs3_breaks = self._create_breakpoint_lines(self.fgs3_breaks)

        annotations = [
            {
                'x': convert_day_of_year(item[1]).to_datetime(),
                'y': self.figure.layout[yaxis]['domain'][1],
                'xref': xref,
                'yref': 'paper',
                'text': f'{item[0]}<br>{convert_day_of_year(item[1]).to_datetime().date()}',
                'showarrow': True,
                'ax': ax,
                'ay': -30,
            } for item, ax in zip(self.fgs_events.items(), [-60, 50, -20, 20, -50, 20, 50, -50, 60])
            for xref, yaxis in zip(['x1', 'x2'], ['yaxis1', 'yaxis2'])
        ]

        # Create visibility toggles for buttons
        # F1 traces are created first, so the order for the list of traces is f1 traces then f2 traces
        n_traces = list(traces_per_fgs.values())

        visibility = [
            create_visibility(n_traces, [True, False, False]),  # F1
            create_visibility(n_traces, [False, True, False]),  # F2
            create_visibility(n_traces, [False, False, True])  # F3
        ]

        labels = ['FGS1', 'FGS2', 'FGS3']
        titles = [f'<a href="{self.docs}">{fgs + self.name}</a>' for fgs in labels]
        shapes = [lines + fgs1_breaks, lines + fgs2_breaks, lines + fgs3_breaks]

        # Create buttons
        updatemenus = [
            go.layout.Updatemenu(
                active=-1,
                buttons=[
                    dict(
                        label=label,
                        method='update',
                        args=[
                            {'visible': visible},
                            {'title': title, 'annotations': annotations, 'shapes': shape_set}
                        ]
                    ) for label, visible, title, shape_set in zip(labels, visibility, titles, shapes)
                ]
            ),
        ]

        # Create layout
        layout = go.Layout(
            updatemenus=updatemenus,
            hovermode='closest',
            xaxis=dict(title='Datetime', matches='x2'),
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
    docs = (
        "https://spacetelescope.github.io/cosmo/monitors.html#spectroscopic-acquisition-monitors-acqpeakd-and-acqpeakxd"
    )
    labels = ['ROOTNAME', 'PROPOSID', 'LIFE_ADJ', 'OPT_ELEM', 'CENWAVE', 'DETECTOR']
    output = COS_MONITORING
    slew = None

    # PEAKD vs PEAKXD need different annotations and shapes
    annotations = None
    shapes = None

    run = 'monthly'

    def get_data(self):
        exptype = 'ACQ/PEAKD' if self.slew == 'ACQSLEWX' else 'ACQ/PEAKXD'

        return select_all_acq(self.model.model, exptype, self.model.new_data)

    def track(self):
        """Track the standard deviation of the slew per FGS."""
        groups = self.data.groupby('FGS')
        scatter = groups[self.slew].std()

        return groups, scatter

    def find_outliers(self):
        """Outliers are defined as those slews/offsets with a magnitude >= 1 arcsecond."""
        return self.data[self.slew].abs() >= 1

    def plot(self):
        """Plot offset (-slew) v time per FGS. Separate FGS via button options. Color by LP-POS"""
        fgs_groups, std_results = self.results  # groups are stored in the results attribute since track returns them.

        trace_count = {'F1': 0, 'F2': 0, 'F3': 0}
        lp_colors = ['#1f77b4', '#2ca02c', '#8c564b', '#bcbd22']  # blue, green, brown, yellow-green
        detector_symbols = {'NUV': 'x', 'FUV': 'circle'}
        for name, group in fgs_groups:
            lp_groups = group.groupby('LIFE_ADJ')

            for lp, lp_group in lp_groups:
                trace_count[name] += 1
                scatter = go.Scatter(  # Scatter plot
                    x=Time(lp_group.EXPSTART, format='mjd').to_datetime(),
                    y=-lp_group[self.slew],
                    mode='markers',
                    text=lp_group.hover_text,
                    visible=False,
                    name=f'{name} LP{lp}',
                    legendgroup=f'LP{lp}',
                    marker_color=lp_colors[lp - 1],
                    marker_symbol=[detector_symbols[detector] for detector in lp_group.DETECTOR]
                )

                self.figure.add_trace(scatter)

                outliers = lp_group[self.outliers.iloc[lp_group.index.values]]

                if not outliers.empty:
                    trace_count[name] += 1

                    outlier_trace = go.Scatter(
                        x=Time(outliers.EXPSTART, format='mjd').to_datetime(),
                        y=-outliers[self.slew],
                        mode='markers',
                        text=outliers.hover_text,
                        visible=False,
                        name=f'{name} LP{lp} Outliers',
                        legendgroup=f'LP{lp}',
                        marker_color='red',
                        marker_symbol='x',
                        marker_size=10
                    )

                    self.figure.add_trace(outlier_trace)

        fgs_labels = ['All FGS', 'FGS1', 'FGS2', 'FGS3']

        # Create visibility options for buttons
        n_traces = list(trace_count.values())
        all_fgs = create_visibility(n_traces, [True, True, True])
        f1_visible = create_visibility(n_traces, [True, False, False])
        f2_visible = create_visibility(n_traces, [False, True, False])
        f3_visible = create_visibility(n_traces, [False, False, True])

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
                            {'title': f'<a href="{self.docs}">{fgs} {self.name}</a>'}
                        ]
                    ) for fgs, visible in zip(fgs_labels, [all_fgs, f1_visible, f2_visible, f3_visible])
                ]
            )
        ]

        # Create layout
        layout = go.Layout(
            updatemenus=updatemenus,
            hovermode='closest',
            xaxis=dict(title='Datetime'),
            yaxis=dict(title='Offset (-Slew) [arcseconds]'),
            shapes=self.shapes,
            annotations=self.annotations
        )

        self.figure.update_layout(layout)

    def store_results(self):
        # TODO: Define what to store
        pass


class AcqPeakdMonitor(SpecAcqBaseMonitor):
    """ACQPEAKD monitor."""
    name = 'AcqPeakd Monitor'
    data_model = AcqDataModel
    slew = 'ACQSLEWX'

    # Transparent box highlighting good offset range
    shapes = [
        go.layout.Shape(
            type='rect',
            xref='paper',
            yref='y',
            y0=-1,
            y1=1,
            x0=0,
            x1=1,
            fillcolor='lightseagreen',
            opacity=0.3,
            layer='below',
        )
    ]


class AcqPeakxdMonitor(SpecAcqBaseMonitor):
    """ACQPEAKXD monitor."""
    name = 'AcqPeakxd Monitor'
    data_model = AcqDataModel
    slew = 'ACQSLEWY'

    peakxd_update = datetime.datetime.strptime('2017-10-02', '%Y-%m-%d')

    shapes = [
        go.layout.Shape(  # Transparent box highlighting "good" offset range
            type='rect',
            xref='paper',
            yref='y',
            y0=-1,
            y1=1,
            x0=0,
            x1=1,
            fillcolor='lightseagreen',
            opacity=0.3,
            layer='below',
        ),
        go.layout.Shape(  # Vertical line indicating when the new PEAKXD algorithm was activated
            type='line',
            x0=peakxd_update,
            y0=0,
            x1=peakxd_update,
            y1=1,
            yref='paper',
            xref='x1',
            line={
                'width': 2
            }
        )
    ]

    annotations = [
        {
            'x': peakxd_update,
            'y': 1,
            'xref': 'x',
            'yref': 'paper',
            'text': f'New PEAKXD Algorithm Activated<br>{peakxd_update}',
            'showarrow': True,
            'ax': -30,
            'ay': -30,
        }
    ]
