import plotly.graph_objs as go
import datetime

from itertools import repeat
from astropy.time import Time, TimeDelta

from monitorframe import BaseMonitor
from .osm_data_models import OSMDataModel

COS_MONITORING = '/grp/hst/cos2/monitoring'

LP_MOVES = lp_moves = {
        i + 2: datetime.datetime.strptime(date, '%Y-%m-%d')
        for i, date in enumerate(['2012-07-23', '2015-02-09', '2017-10-02'])
    }


def compute_start_times(df):
    start_time = Time(df.EXPSTART, format='mjd')
    lamp_dt = TimeDelta(df.TIME, format='sec')
    lamp_time = start_time + lamp_dt

    return start_time, lamp_time


def plot_fuv_osm_shift(df, shift):
    groups = df.groupby(['OPT_ELEM', 'CENWAVE'])

    fp_symbols = {
        1: 'circle',
        2: 'cross',
        3: 'triangle-up',
        4: 'x'
    }

    traces = []

    segment_diff = []
    time = []
    hover_text = []
    root_groups = df.groupby('ROOTNAME')
    for rootname, group in root_groups:
        if 'FUVA' in group.SEGMENT.values and 'FUVB' in group.SEGMENT.values:
            _, lamp_time = compute_start_times(group[group.SEGMENT == 'FUVA'])

            fuva, fuvb = group[group.SEGMENT == 'FUVA'], group[group.SEGMENT == 'FUVB']

            segment_diff.extend(fuva[shift].values - fuvb[shift].values)
            hover_text.extend([item.hover_text + f'<br>CENWAVE    {item.CENWAVE}' for _, item in fuva.iterrows()])
            time.extend(lamp_time.to_datetime())

    traces.append(
        go.Scattergl(
            x=time,
            y=segment_diff,
            name='FUVA - FUVB',
            mode='markers',
            text=hover_text,
        )
    )

    for i, group_info in enumerate(groups):
        name, group = group_info

        start_time, lamp_time = compute_start_times(group)

        traces.append(
            go.Scattergl(
                x=lamp_time.to_datetime(),
                y=group[shift],
                name='-'.join([str(item) for item in name]),
                mode='markers',
                text=group.hover_text,
                xaxis='x',
                yaxis='y2',
                marker=dict(
                    cmax=len(df.CENWAVE.unique()) - 1,
                    cmin=0,
                    color=list(repeat(i, len(group))),
                    colorscale='Viridis',
                    symbol=[fp_symbols[fp] for fp in group.FPPOS],
                    size=[
                        10 if time > LP_MOVES[4] and lp == 3 else 6
                        for lp, time in zip(group.LIFE_ADJ, start_time.to_datetime())
                    ]
                ),
            )
        )

    layout = go.Layout(
        xaxis=dict(title='Datetime'),
        yaxis2=dict(title='Shift [pix]', anchor='x', domain=[0.3, 1], gridwidth=5),
        yaxis=dict(title='Shift Difference A - B [pix]', anchor='x2', domain=[0, 0.18])
    )

    return traces, layout


class FuvAdOsmShiftMonitor(BaseMonitor):
    data_model = OSMDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID']

    def track(self):
        # TODO: Define interesting things to track
        pass

    def filter_data(self):
        return self.data[self.data.DETECTOR == 'FUV']

    def plot(self):
        traces, layout = plot_fuv_osm_shift(self.filtered_data, 'SHIFT_DISP')

        trace_length = len(traces)
        all_visible = list(repeat(True, trace_length))

        fp_groups = self.filtered_data.groupby('FPPOS')
        fp_trace_lengths = {}
        for fp, group in fp_groups:
            fp_traces, _ = plot_fuv_osm_shift(group, 'SHIFT_DISP')
            traces.extend(fp_traces)
            all_visible.extend(list(repeat(False, len(fp_traces))))
            fp_trace_lengths[fp] = len(fp_traces)

        self.figure.add_traces(traces)
        self.figure['layout'].update(layout)

        all_hidden = list(repeat(False, len(all_visible)))

        fp1_visible = all_hidden.copy()
        fp2_visible = all_hidden.copy()
        fp3_visible = all_hidden.copy()
        fp4_visible = all_hidden.copy()

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

        updatemenues = [
            dict(
                type='buttons',
                buttons=[
                    dict(
                        label='All FPPOS',
                        method='update',
                        args=[
                            {'visible': all_visible},
                            {'title': f'{self.name} All FPPOS'}
                        ]
                    ),
                    dict(
                        label='FPPOS 1',
                        method='update',
                        args=[
                            {'visible': fp1_visible},
                            {'title': f'{self.name} FPPOS 1 Only'}
                        ]
                    ),
                    dict(
                        label='FPPOS 2',
                        method='update',
                        args=[
                            {'visible': fp2_visible},
                            {'title': f'{self.name} FPPOS 2 Only'}
                        ]
                    ),
                    dict(
                        label='FPPOS 3',
                        method='update',
                        args=[
                            {'visible': fp3_visible},
                            {'title': f'{self.name} FPPOS 3 Only'}
                        ]
                    ),
                    dict(
                        label='FPPOS 4',
                        method='update',
                        args=[
                            {'visible': fp4_visible},
                            {'title': f'{self.name} FPPOS 4 Only'}
                        ]
                    )
                ]
            )
        ]

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
        # TODO: decide on how to store results and what needs to be stored
        pass


class FuvXdOsmShiftMonitor(BaseMonitor):
    data_model = OSMDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID', 'OBSET_ID']

    def track(self):
        # TODO: Define interesting things to track
        pass

    def filter_data(self):
        return self.data[self.data.DETECTOR == 'FUV']

    def plot(self):
        traces, layout = plot_fuv_osm_shift(self.filtered_data, 'SHIFT_XDISP')

        self.figure.add_traces(traces)
        self.figure['layout'].update(layout)

    def store_results(self):
        # TODO: decide on how to store results and what needs to be stored
        pass
