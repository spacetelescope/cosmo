import plotly.graph_objs as go
import datetime
import pandas as pd
import os

from itertools import repeat
from monitorframe.monitor import BaseMonitor
from typing import List, Union

from .osm_data_models import OSMShiftDataModel
from ..monitor_helpers import ExposureAbsoluteTime, explode_df
from .. import SETTINGS

COS_MONITORING = SETTINGS['output']

LP_MOVES = {
        i + 2: datetime.datetime.strptime(date, '%Y-%m-%d')
        for i, date in enumerate(['2012-07-23', '2015-02-09', '2017-10-02'])
    }


def match_dfs(df1: pd.DataFrame, df2: pd.DataFrame, key: str) -> pd.DataFrame:
    """Filter df1 based on which values in key are available in df2."""
    return df1[df1.apply(lambda x: x[key] in df2[key].values, axis=1)]


def create_visibility(trace_lengths: List[int], visible_list: List[bool]) -> List[bool]:
    """Create visibility lists for plotly buttons. trace_lengths and visible_list must be in the correct order.

    :param trace_lengths: List of the number of traces in each "button set".
    :param visible_list: Visibility setting for each button set (either True or False).
    """
    visibility = []  # Total visibility. Length should match the total number of traces in the figure.
    for visible, trace_length in zip(visible_list, trace_lengths):
        visibility += list(repeat(visible, trace_length))  # Set each trace per button.

    return visibility


def compute_segment_diff(df: pd.DataFrame, shift: str, segment1: str, segment2: str) -> Union[pd.DataFrame, None]:
    """Compute the difference (A-B) in the shift measurement between segments."""
    root_groups = df.groupby('ROOTNAME')  # group by rootname which may or may not have FUVA and FUVB shifts

    results_list = []
    for rootname, group in root_groups:
        if segment1 in group.SEGMENT.values and segment2 in group.SEGMENT.values:
            # absolute time calculated from FUVA
            lamp_time = ExposureAbsoluteTime.compute_from_df(group[group.SEGMENT == segment1])

            segmnet1_df, segment2_df = group[group.SEGMENT == segment1], group[group.SEGMENT == segment2]

            # Use the FUVA dataframe to create a "results" dataframe
            diff_df = segmnet1_df.assign(
                seg_diff=segmnet1_df[shift].values - segment2_df[shift].values
            ).reset_index(drop=True)

            diff_df['lamp_time'] = lamp_time.to_datetime()

            # Drop "segment specific" info
            diff_df.drop(columns=['TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT', 'DETECTOR'], inplace=True)
            results_list.append(diff_df)

    if results_list:
        return pd.concat(results_list, ignore_index=True)

    return


class FuvOsmShiftMonitor(BaseMonitor):
    """Abstracted FUV OSM Shift monitor. This monitor class is not meant to be used directly, but rather inherited from
    by specific Shift1 and Shift2 monitors (which share the same plots, but differ in which shift value is plotted and
    how outliers are defined).
    """
    data_model = OSMShiftDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID', 'SEGMENT', 'CENWAVE']

    subplots = True
    subplot_layout = (2, 1)

    shift = None  # SHIFT_DISP or SHIFT_XDISP

    def get_data(self) -> pd.DataFrame:
        """Get new data from the data model. Expand the data around individual flashes and filter on FUV."""
        exploded_data = explode_df(self.model.new_data, ['TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT'])

        return exploded_data[exploded_data.DETECTOR == 'FUV']

    def track(self) -> pd.DataFrame:
        """Track the difference in shift, A-B"""
        return compute_segment_diff(self.data, self.shift, 'FUVA', 'FUVB')

    def _plot_per_cenwave(self, df: pd.DataFrame, shift: str, outliers: pd.DataFrame = None) -> int:
        """Plot shift v time and A-B v time by grating/cenwave"""
        trace_number = 0  # Keep track of the number of traces created and added

        groups = df.groupby(['OPT_ELEM', 'CENWAVE'])

        # Set symbols for different FP-POS
        fp_symbols = {
            1: 'circle',
            2: 'cross',
            3: 'triangle-up',
            4: 'x'
        }

        # Compute A-B shift difference
        seg_diff_results = compute_segment_diff(df, shift, 'FUVA', 'FUVB')

        # Plot A-B v time
        self.figure.add_trace(
            go.Scattergl(
                x=seg_diff_results.lamp_time,
                y=seg_diff_results.seg_diff,
                name='FUVA - FUVB',
                mode='markers',
                text=seg_diff_results.hover_text,
                visible=False,
            ),
            row=1,
            col=1
        )
        trace_number += 1

        # Plot shift v time per grating/cenwave group
        for i, (name, group) in enumerate(groups):
            trace_number += 1

            grating, cenwave = name
            absolute_time = ExposureAbsoluteTime(df=group)
            lamp_time = absolute_time.compute_absolute_time()

            self.figure.add_trace(
                go.Scattergl(
                    x=lamp_time.to_datetime(),
                    y=group[shift],
                    name=f'{grating}-{cenwave}',
                    mode='markers',
                    text=group.hover_text,
                    visible=False,
                    marker=dict(  # Color markers based on cenwave
                        cmax=len(df.CENWAVE.unique()) - 1,  # Individual plots need to be on the same scale
                        cmin=0,
                        color=list(repeat(i, len(group))),
                        colorscale='Viridis',
                        symbol=[fp_symbols[fp] for fp in group.FPPOS],
                        size=[
                            10 if time > LP_MOVES[4] and lp == 3 else 6
                            for lp, time in zip(group.LIFE_ADJ, absolute_time.expstart_time.to_datetime())
                        ]  # Set the size to distinguish exposures taken at LP3 after the move to LP4
                    )
                ),
                row=2,
                col=1,
            )

        if outliers is not None:
            self.figure.add_trace(
                go.Scattergl(
                    x=outliers.lamp_time,
                    y=outliers.seg_diff,
                    name='A - B Outliers',
                    mode='markers',
                    text=outliers.hover_text,
                    visible=False,
                    marker=dict(color='red'),
                ),
                row=1,
                col=1
            )
            trace_number += 1

            # Plot outlier points in a different color
            outlier_mainplot = df[df.apply(lambda x: x.ROOTNAME in outliers.ROOTNAME.values, axis=1)]
            outlier_groups = outlier_mainplot.groupby(['OPT_ELEM', 'CENWAVE'])
            for name, group in outlier_groups:
                trace_number += 1

                grating, cenwave = name
                absolute_time = ExposureAbsoluteTime(df=group)
                lamp_time = absolute_time.compute_absolute_time()

                self.figure.add_trace(
                    go.Scattergl(
                        x=lamp_time.to_datetime(),
                        y=group[shift],
                        name=f'{grating}-{cenwave} Outliers',
                        mode='markers',
                        text=group.hover_text,
                        visible=False,
                        marker=dict(
                            color='red',
                            symbol=[fp_symbols[fp] for fp in group.FPPOS],
                            size=[
                                10 if time > LP_MOVES[4] and lp == 3 else 6
                                for lp, time in zip(group.LIFE_ADJ, absolute_time.expstart_time.to_datetime())
                            ]  # Set the size to distinguish exposures taken at LP3 after the move to LP4
                        )
                    ),
                    row=2,
                    col=1,
                )

        return trace_number

    def plot(self):
        """Plot shift v time and A-B v time per cenwave, and with each FP-POS separate by button options."""
        outliers = self.results[self.outliers]

        # First set of traces for the "All FPPOS" button
        all_n_traces = self._plot_per_cenwave(self.data, self.shift, outliers)

        # Plot traces per FPPOS for the other buttons
        fp_groups = self.data.groupby('FPPOS')
        fp_outliers = outliers.groupby('FPPOS')
        fp_trace_lengths = {}  # track the number of traces produced per fp; this differs between fp
        for fp, group in fp_groups:

            try:  # Collect matching outliers if they exist
                outlier_group = fp_outliers.get_group(fp)
            except KeyError:
                outlier_group = None

            n_fp_traces = self._plot_per_cenwave(group, self.shift, outlier_group)
            fp_trace_lengths[fp] = n_fp_traces

        # For each fp, set the visibility to True for the appropriate number of traces in the appropriate position.
        trace_lengths = [all_n_traces] + list(fp_trace_lengths.values())

        visibilities = [
            create_visibility(trace_lengths, [True, False, False, False, False]),  # All FPPOS
            create_visibility(trace_lengths, [False, True, False, False, False]),  # FPPOS 1
            create_visibility(trace_lengths, [False, False, True, False, False]),  # FPPOS 2
            create_visibility(trace_lengths, [False, False, False, True, False]),  # FPPOS 3
            create_visibility(trace_lengths, [False, False, False, False, True])  # FPPOS 4
        ]

        button_labels = ['All FPPOS', 'FPPOS 1', 'FPPOS 2', 'FPPOS 3', 'FPPOS 4']
        titles = [f'{self.name} All FPPOS'] + [f'{self.name} {label} Only' for label in button_labels[1:]]

        # Create the menu buttons
        updatemenus = [
            dict(
                type='buttons',
                buttons=[
                    dict(
                        label=label,
                        method='update',
                        args=[
                            {'visible': visibility},
                            {'title': button_title}
                        ]
                    ) for label, visibility, button_title in zip(button_labels, visibilities, titles)
                ]
            )
        ]

        # Create vertical lines for the LP moves
        lines = [
            {
                'type': 'line',
                'x0': lp_time,
                'y0': 0,
                'x1': lp_time,
                'y1': 1,
                'yref': 'paper',
                'line': {
                    'width': 2,
                }
            } for lp_time in LP_MOVES.values()
        ]

        annotations = [
            {
                'x': lp_time,
                'y': 1,
                'xref': 'x',
                'yref': 'paper',
                'text': f'Start of LP{key}<br>{lp_time.date()}',
                'showarrow': True,
                'ax': -ax,
                'ay': -30,
            } for (key, lp_time), ax in zip(LP_MOVES.items(), [30, 10, -10])
        ]

        self.figure.update_layout(
            go.Layout(
                xaxis=dict(title='Datetime', matches='x2'),
                xaxis2=dict(title='Datetime'),
                yaxis2=dict(title='Shift [pix]', domain=[0.3, 1], gridwidth=5),
                yaxis=dict(title='Shift (A - B) [pix]', anchor='x2', domain=[0, 0.18]),
                shapes=lines,
                updatemenus=updatemenus,
                annotations=annotations
            )
        )

    def store_results(self):
        """Store A - B outliers in a csv file."""
        outliers = self.results[self.outliers]
        outliers.to_csv(os.path.join(os.path.dirname(self.output), f'{self._filename}-outliers.csv'))


class FuvOsmShift1Monitor(FuvOsmShiftMonitor):
    """FUV OSM Shift1 (SHIFT_DISP) monitor."""
    shift = 'SHIFT_DISP'  # shift1

    def find_outliers(self):
        """Outliers for shift1 A-B are defined as any difference whose magnitude is greater than 10 pixels."""
        return self.results.seg_diff.abs() > 10


class FuvOsmShift2Monitor(FuvOsmShiftMonitor):
    """FUV OSM Shift2 (SHIFT_XDISP) monitor."""
    shift = 'SHIFT_XDISP'  # shift 2

    def find_outliers(self):
        """Outliers for shift2 A-B are defined as any difference whose magnitude is greater than 5 pixels."""
        return self.results.seg_diff.abs() > 5


class NuvOsmShiftMonitor(BaseMonitor):
    """Abstracted NUV OSM Shift monitor. This monitor class is not meant to be used directly, but rather inherited from
    by specific Shift1 and Shift2 monitors (which share the same plots, but differ in which shift value is plotted and
    how outliers are defined)."""
    data_model = OSMShiftDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID', 'CENWAVE']

    subplots = True,
    subplot_layout = (3, 1)

    shift = None  # SHIFT_DISP or SHIFT_XDISP

    def get_data(self):
        """Filter on detector."""
        exploded_data = explode_df(self.model.new_data, ['TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT'])

        return exploded_data[exploded_data.DETECTOR == 'NUV']

    def track(self) -> dict:
        """Track the difference in shift between stripes. B-C and C-A."""
        return {
            'B-C': compute_segment_diff(self.data, self.shift, 'NUVB', 'NUVC'),
            'C-A': compute_segment_diff(self.data, self.shift, 'NUVC', 'NUVA'),
        }

    def _plot_per_grating(self, df: pd.DataFrame, shift: str):
        trace_number = 0  # Keep track of the number of traces created and added

        all_b_c_outliers = self.results['B-C'][self.outliers['B-C']]
        all_c_a_outliers = self.results['C-A'][self.outliers['C-A']]

        # Find matching stripe differences and outliers
        b_c = match_dfs(self.results['B-C'], df, 'ROOTNAME')
        c_a = match_dfs(self.results['C-A'], df, 'ROOTNAME')

        b_c_outliers = match_dfs(all_b_c_outliers, df, 'ROOTNAME')
        c_a_outliers = match_dfs(all_c_a_outliers, df, 'ROOTNAME')

        # Plot diffs v time
        if not b_c.empty:
            self.figure.add_trace(
                go.Scattergl(
                    x=b_c.lamp_time,
                    y=b_c.seg_diff,
                    name='NUVB - NUVC',
                    mode='markers',
                    text=b_c.hover_text,
                    visible=False,
                    marker=dict(color='#1f77b4')  # "muted blue"
                ),
                row=1,
                col=1
            )
            trace_number += 1

        if c_a is not None and not c_a.empty:
            self.figure.add_trace(
                go.Scattergl(
                    x=c_a.lamp_time,
                    y=c_a.seg_diff,
                    name='NUVC - NUVA',
                    mode='markers',
                    text=c_a.hover_text,
                    visible=False,
                    marker=dict(color='#1f77b4')
                ),
                row=2,
                col=1
            )
            trace_number += 1

        # Plot shift v time per grating group
        groups = df.groupby('OPT_ELEM')

        for i, (grating, group) in enumerate(groups):
            trace_number += 2

            absolute_time = ExposureAbsoluteTime(df=group)
            group = group.set_index(absolute_time.compute_absolute_time().to_datetime())
            group = group.sort_index()

            rolling_mean = group.rolling('365D').mean()

            self.figure.add_trace(
                go.Scattergl(
                    x=group.index,
                    y=group[shift],
                    name=grating,
                    mode='markers',
                    text=group.hover_text,
                    visible=False,
                    legendgroup=grating,
                    marker=dict(
                        cmax=len(df.OPT_ELEM.unique()) - 1,  # Individual plots need to be on the same scale
                        cmin=0,
                        color=list(repeat(i, len(group))),
                        colorscale='Viridis',
                        opacity=0.5
                    )
                ),
                row=3,
                col=1,
            )

            self.figure.add_trace(
                go.Scattergl(
                    x=rolling_mean.index,
                    y=rolling_mean[shift],
                    name='Rolling Mean',
                    mode='lines',
                    visible=False,
                    legendgroup=grating
                ),
                row=3,
                col=1
            )

        # Plot each set of potential outliers
        outlier_sets = [b_c_outliers, c_a_outliers]
        position = [(1, 1), (2, 1)]
        labels = ['B-C Outliers', 'C-A Outliers']
        for outliers, (row, col), label in zip(outlier_sets, position, labels):
            if outliers is not None and not outliers.empty:
                self.figure.add_trace(
                    go.Scattergl(
                        x=outliers.lamp_time,
                        y=outliers.seg_diff,
                        name=label,
                        mode='markers',
                        text=outliers.hover_text,
                        visible=False,
                        marker=dict(color='red'),
                    ),
                    row=row,
                    col=col
                )
                trace_number += 1

                # Plot outlier points in a different color
                outliers_main = match_dfs(df, outliers, 'ROOTNAME')
                outlier_groups = outliers_main.groupby('OPT_ELEM')
                for grating, group in outlier_groups:
                    trace_number += 1

                    absolute_time = ExposureAbsoluteTime(df=group)
                    lamp_time = absolute_time.compute_absolute_time()

                    self.figure.add_trace(
                        go.Scattergl(
                            x=lamp_time.to_datetime(),
                            y=group[shift],
                            name=f'{grating} {label}',
                            mode='markers',
                            text=group.hover_text,
                            visible=False,
                            marker=dict(color='red'),
                            legendgroup=f'{grating} outliers'
                        ),
                        row=3,
                        col=1,
                    )

        return trace_number

    def plot(self):
        """Plot shift v time per grating/cenwave."""
        all_stripes_traces = self._plot_per_grating(self.data, self.shift)

        # Plot traces per FPPOS for the other buttons
        stripe_groups = self.data.groupby('SEGMENT')

        stripe_trace_lengths = {}  # track the number of traces produced per stripe; this differs between stripe
        for stripe, group in stripe_groups:
            n_stripe_traces = self._plot_per_grating(group, self.shift)
            stripe_trace_lengths[stripe] = n_stripe_traces

        # For each stripe, set the visibility to True for the appropriate number of traces in the appropriate position.
        trace_lengths = [all_stripes_traces] + list(stripe_trace_lengths.values())

        visibilities = [
            create_visibility(trace_lengths, [True, False, False, False]),  # All stripes
            create_visibility(trace_lengths, [False, True, False, False]),  # NUVA
            create_visibility(trace_lengths, [False, False, True, False]),  # NUVB
            create_visibility(trace_lengths, [False, False, False, True]),  # NUVC
        ]

        button_labels = ['All Stripes', 'NUVA', 'NUVB', 'NUVC']
        titles = [f'{self.name} All Stripes'] + [f'{self.name} {label} Only' for label in button_labels[1:]]

        # Create the menu buttons
        updatemenus = [
            dict(
                type='buttons',
                buttons=[
                    dict(
                        label=label,
                        method='update',
                        args=[
                            {'visible': visibility},
                            {'title': button_title}
                        ]
                    ) for label, visibility, button_title in zip(button_labels, visibilities, titles)
                ]
            )
        ]

        # Set layout
        layout = go.Layout(
            xaxis=dict(title='Datetime', matches='x2'),
            xaxis2=dict(title='Datetime', matches='x3'),
            xaxis3=dict(title='Datetime', matches='x1'),
            yaxis=dict(title='Shift (B -C) [pix]', domain=[0, 0.18]),
            yaxis2=dict(title='Shift (C - A) [pix]', domain=[0.3, .48]),
            yaxis3=dict(title='Shift [pix]', domain=[0.6, 1]),
            updatemenus=updatemenus
        )

        self.figure.update_layout(layout)

    def store_results(self):
        # TODO: decide on what results to store and how
        pass


class NuvOsmShift1Monitor(NuvOsmShiftMonitor):
    """NUV OSM Shift1 (SHIFT_DISP) monitor."""
    shift = 'SHIFT_DISP'  # shift1

    def find_outliers(self) -> dict:
        return {
            'B-C': self.results['B-C'].seg_diff.abs() >= 10,
            'C-A': self.results['C-A'].seg_diff.abs() >= 10
        }


class NuvOsmShift2Monitor(NuvOsmShiftMonitor):
    """NUV OSM Shift2 (SHIFT_XDISP) monitor."""
    shift = 'SHIFT_XDISP'  # shift2

    def find_outliers(self) -> dict:
        return {
            'B-C': self.results['B-C'].seg_diff.abs() >= 5,
            'C-A': self.results['C-A'].seg_diff.abs() >= 5
        }
