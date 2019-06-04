import plotly.graph_objs as go
import datetime
import pandas as pd
import os

from itertools import repeat

from monitorframe import BaseMonitor
from cosmo.monitor_helpers import compute_lamp_on_times
from .osm_data_models import OSMDataModel

COS_MONITORING = '/grp/hst/cos2/monitoring'

LP_MOVES = lp_moves = {
        i + 2: datetime.datetime.strptime(date, '%Y-%m-%d')
        for i, date in enumerate(['2012-07-23', '2015-02-09', '2017-10-02'])
    }


def plot_fuv_osm_shift_cenwaves(df: pd.DataFrame, shift: str) -> [list, go.Layout]:
    """Plot shift v time and A-B v time by grating/cenwave"""
    groups = df.groupby(['OPT_ELEM', 'CENWAVE'])

    # Set symbols for different FP-POS
    fp_symbols = {
        1: 'circle',
        2: 'cross',
        3: 'triangle-up',
        4: 'x'
    }

    traces = []

    # Compute A-B shift difference
    seg_diff_results = compute_segment_diff(df, shift)

    # Plot A-B v time
    traces.append(
        go.Scattergl(
            x=seg_diff_results.lamp_time,
            y=seg_diff_results.seg_diff,
            name='FUVA - FUVB',
            mode='markers',
            text=seg_diff_results.hover_text,
            visible=False
        )
    )

    # Plot shfit v time per grating/cenwave group
    for i, group_info in enumerate(groups):
        name, group = group_info

        start_time, lamp_time = compute_lamp_on_times(group)

        traces.append(
            go.Scattergl(
                x=lamp_time.to_datetime(),
                y=group[shift],
                name='-'.join([str(item) for item in name]),
                mode='markers',
                text=group.hover_text,
                xaxis='x',
                yaxis='y2',
                visible=False,
                marker=dict(
                    cmax=len(df.CENWAVE.unique()) - 1,
                    cmin=0,
                    color=list(repeat(i, len(group))),
                    colorscale='Viridis',
                    symbol=[fp_symbols[fp] for fp in group.FPPOS],
                    size=[
                        10 if time > LP_MOVES[4] and lp == 3 else 6
                        for lp, time in zip(group.LIFE_ADJ, start_time.to_datetime())
                    ]  # Set the size to distinguish exposures taken at LP3 after the move to LP4
                ),
            )
        )

    # Set layout; Link the x-axes; increase the horizontal gridline width for the shift v time plot
    layout = go.Layout(
        xaxis=dict(title='Datetime'),
        yaxis2=dict(title='Shift [pix]', anchor='x', domain=[0.3, 1], gridwidth=5),
        yaxis=dict(title='Shift (A - B) [pix]', anchor='x2', domain=[0, 0.18])
    )

    return traces, layout


def compute_segment_diff(df: pd.DataFrame, shift: str) -> pd.DataFrame:
    """Compute the difference (A-B) in the shift measurement betwen segments."""
    root_groups = df.groupby('ROOTNAME')  # group by rootname which may or may not have FUVA and FUVB shifts

    results_list = []
    for rootname, group in root_groups:
        if 'FUVA' in group.SEGMENT.values and 'FUVB' in group.SEGMENT.values:
            _, lamp_time = compute_lamp_on_times(group[group.SEGMENT == 'FUVA'])  # absolute time calculated from FUVA

            fuva, fuvb = group[group.SEGMENT == 'FUVA'], group[group.SEGMENT == 'FUVB']

            # Use the FUVA dataframe to create a "results" dataframe
            diff_df = fuva.assign(seg_diff=fuva[shift].values - fuvb[shift].values).reset_index(drop=True)

            # Add the cenwave to the hover text
            diff_df['hover_text'] = [
                item.hover_text + f'<br>CENWAVE    {item.CENWAVE}' for _, item in fuva.iterrows()
            ]

            diff_df['lamp_time'] = lamp_time.to_datetime()

            # Drop "segment specific" info
            diff_df.drop(columns=['TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT', 'DETECTOR'], inplace=True)
            results_list.append(diff_df)

    results_df = pd.concat(results_list, ignore_index=True)

    return results_df


class FuvOsmShiftMonitor(BaseMonitor):
    """Abstracted FUV OSM Shift monitor. This monitor class is not meant to be used directly, but rather inhereted from
    by specific Shift1 and Shift2 monitors (which share the same plots, but differ in which shift value is plotted and
    how outliers are defined).
    """
    data_model = OSMDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID']

    shift = None  # SHIFT_DISP or SHIFT_XDISP

    def track(self):
        """Track the difference in shift, A-B"""
        return compute_segment_diff(self.filtered_data, self.shift)

    def filter_data(self):
        """Filter on detector."""
        return self.data[self.data.DETECTOR == 'FUV']

    def plot(self):
        """Plot shift v time and A-B v time per cenwave, and with each FP-POS separate by button options."""
        # First set of layouts for the "All FPPOS" button
        traces, layout = plot_fuv_osm_shift_cenwaves(self.filtered_data, self.shift)

        trace_length = len(traces)  # Number of traces in the first group
        all_visible = list(repeat(True, trace_length))  # Visible option for the first group

        fp_groups = self.filtered_data.groupby('FPPOS')
        fp_trace_lengths = {}  # track the number of traces produced per fp; this differs between fp
        for fp, group in fp_groups:
            fp_traces, _ = plot_fuv_osm_shift_cenwaves(group, self.shift)
            traces.extend(fp_traces)  # Add new traces to the set
            all_visible.extend(list(repeat(False, len(fp_traces))))  # Add new groups to the first
            fp_trace_lengths[fp] = len(fp_traces)

        self.figure.add_traces(traces)  # Add all traces
        self.figure['layout'].update(layout)  # Add initial layout which determines how the subplots are configured

        # Create a "baseline" where all traces are set to be inactive
        all_hidden = list(repeat(False, len(all_visible)))

        # TODO: Find a better way to track which traces to turn "on" and "off". This is gross.

        # Create copies of the baseline to modify per fp
        fp1_visible = all_hidden.copy()
        fp2_visible = all_hidden.copy()
        fp3_visible = all_hidden.copy()
        fp4_visible = all_hidden.copy()

        # For each fp, set the visibility to True for the appropriate number of traces in the appropriate position.
        fp1_visible[trace_length:trace_length + fp_trace_lengths[1]] = list(repeat(True, fp_trace_lengths[1]))

        fp2_visible[
            trace_length + fp_trace_lengths[1]:trace_length + fp_trace_lengths[1] + fp_trace_lengths[2]
        ] = list(repeat(True, fp_trace_lengths[2]))

        fp3_visible[
            trace_length + fp_trace_lengths[1] + fp_trace_lengths[2]:
            trace_length + fp_trace_lengths[1] + fp_trace_lengths[2] + fp_trace_lengths[3]
        ] = list(repeat(True, fp_trace_lengths[3]))

        fp4_visible[
            trace_length + fp_trace_lengths[1] + fp_trace_lengths[2] + fp_trace_lengths[3]:
        ] = list(repeat(True, fp_trace_lengths[4]))

        button_labels = ['All FPPOS', 'FPPOS 1', 'FPPOS 2', 'FPPOS 3', 'FPPOS 4']
        vsibilities = [all_visible, fp1_visible, fp2_visible, fp3_visible, fp4_visible]
        titles = [f'{self.name} All FPPOS'] + [f'{self.name} {label} Only' for label in button_labels[1:]]

        # Create the menu buttons
        updatemenues = [
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
                    ) for label, visibility, button_title in zip(button_labels, vsibilities, titles)
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

        self.figure['layout'].update({'shapes': lines, 'updatemenus': updatemenues})

    def store_results(self):
        """Store A-B outliers in a csv file."""
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
        """Outliers for shift2 A-B are defined as any differenc whose magnitude is greater than 5 pixels"""
        return self.results.seg_diff.abs() > 5


class NuvOsmShiftMonitor(BaseMonitor):
    """Abstracted NUV OSM Shift monitor. This monitor class is not meant to be used directly, but rather inhereted from
    by specific Shift1 and Shift2 monitors (which share the same plots, but differ in which shift value is plotted and
    how outliers are defined)."""
    data_model = OSMDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID']

    shift = None  # SHIFT_DISP or SHIFT_XDISP

    def filter_data(self):
        """Filter on detector."""
        return self.data[self.data.DETECTOR == 'NUV']

    def plot(self):
        """Plot shift v time per grating/cenwave."""
        groups = self.filtered_data.groupby(['OPT_ELEM', 'CENWAVE'])

        # Plot shift v time for each grating/cenwave
        traces = []
        for i, group_info in enumerate(groups):
            name, group = group_info

            start_time, lamp_time = compute_lamp_on_times(group)

            traces.append(
                go.Scattergl(
                    x=lamp_time.to_datetime(),
                    y=group[self.shift],
                    name='-'.join([str(item) for item in name]),
                    mode='markers',
                    text=group.hover_text,
                    marker=dict(
                        cmax=len(self.filtered_data.CENWAVE.unique()) - 1,
                        cmin=0,
                        color=list(repeat(i, len(group))),
                        colorscale='Viridis',
                    ),
                )
            )

        # Set layout
        layout = go.Layout(
            xaxis=dict(title='Datetime'),
            yaxis=dict(title='Shift [pix]', gridwidth=5),
        )

        self.figure.add_traces(traces)
        self.figure['layout'].update(layout)

    def store_results(self):
        # TODO: decide on what results to store and how
        pass

    def track(self):
        # TODO: decide on what to track
        pass


class NuvOsmShift1Monitor(NuvOsmShiftMonitor):
    """NUV OSM Shift1 (SHIFT_DISP) monitor."""
    shift = 'SHIFT_DISP'  # shift1


class NuvOsmShift2Monitor(NuvOsmShiftMonitor):
    """NUV OSM Shift2 (SHIFT_XDISP) monitor."""
    shift = 'SHIFT_XDISP'  # shift2
