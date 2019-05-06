import numpy as np
import plotly.graph_objs as go

from datetime import datetime
from astropy.time import Time
from itertools import repeat


from monitorframe import BaseMonitor
from .acq_data_models import AcqImageModel, AcqPeakdModel, AcqPeakxdModel

COS_MONITORING = '/grp/hst/cos2/monitoring'


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
    name = 'AcImage Slew Monitor'
    data_model = AcqImageModel
    subplots = True
    subplot_layout = (2, 1)
    output = COS_MONITORING
    labels = ['ROOTNAME', 'PROPOSID']

    def track(self):
        groups = self.data.groupby('dom_fgs')

        fit_results = dict()
        for name, group in groups:
            xline = np.poly1d(np.polyfit(group.EXPSTART, -group.ACQSLEWX, 1))
            yline = np.poly1d(np.polyfit(group.EXPSTART, -group.ACQSLEWY, 1))

            fit_results[name] = (xline(group.EXPSTART), yline(group.EXPSTART), xline, yline)

        return groups, fit_results

    def plot(self):
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

            rows.extend([1, 2, 1, 2])
            cols.extend([1, 1, 1, 1])

            for key in visibility.keys():
                if key == name:
                    visibility[key].extend([True, True, True, True])

                else:
                    visibility[key].extend([False, False, False, False])

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

        layout = go.Layout(updatemenus=updatemenus, hovermode='closest')

        self.figure.add_traces(traces, rows=rows, cols=cols)
        self.figure['layout'].update(layout)


class AcqImageFGSMonitor(BaseMonitor):
    name = 'AcqImage FGS Monitor'
    data_model = AcqImageModel
    labels = ['ROOTNAME', 'PROPOSID']
    output = COS_MONITORING

    def track(self):
        groups = self.data.groupby('dom_fgs')
        return groups, -groups.ACQSLEWX.mean(), -groups.ACQSLEWY.mean()

    def plot(self):
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

        layout = go.Layout(updatemenus=updatemenus, hovermode='closest')

        self.figure.add_traces(traces)
        self.figure['layout'].update(layout)


class AcqImageV2V3Monitor(BaseMonitor):
    name = 'V2V3 Offset Monitor'
    data_model = AcqImageModel
    labels = ['ROOTNAME', 'PROPOSID']
    subplots = True
    subplot_layout = (2, 1)
    output = COS_MONITORING

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

    fgs_events = {
        'FGS realignment 1': 2011.172,
        'FGS2 turned on': 2011.206,
        'FGS realignment 2': 2013.205,
        'SIAF update': 2014.055,
        'FGS2 turned off': 2015.327,
        'FGS2 turned back on': 2016.123,
        'GAIA guide stars': 2017.272
    }

    @staticmethod
    def convert_day_of_year(date, mjd=False):
        t = datetime.strptime(str(date), '%Y.%j')

        if mjd:
            return Time(t, format='datetime').mjd

        return t

    @staticmethod
    def line(x, y):
        fit = np.poly1d(np.polyfit(x, y, 1))

        return fit, fit(x)

    def filter_data(self):

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
        groups = self.filtered_data.groupby('dom_fgs')

        last_updated_results = dict()
        for name, group in groups:
            if name == 'F3':
                continue

            t_start = self.convert_day_of_year(self.break_points[name][-1][0], mjd=True)

            df = self.filtered_data[self.filtered_data.EXPSTART >= t_start]

            v2_line_fit = self.line(df.EXPSTART, df.V2SLEW)
            v3_line_fit = self.line(df.EXPSTART, df.V3SLEW)

            last_updated_results[name] = (v2_line_fit, v3_line_fit)

        return groups, last_updated_results

    def plot(self):
        fgs_groups, _ = self.results

        traces = {'F1': [], 'F2': []}
        rows = []
        cols = []
        for name, group in fgs_groups:

            if name == 'F3':
                continue

            for points in self.break_points[name]:

                t_start, t_end = [
                    self.convert_day_of_year(point, mjd=True) if not isinstance(point, str) else None
                    for point in points
                ]

                if t_start is None:
                    df = group[group.EXPSTART <= t_end]

                elif t_end is None:
                    df = group[group.EXPSTART >= t_start]

                else:
                    df = group.iloc[np.where((group.EXPSTART >= t_start) & (group.EXPSTART <= t_end))]

                if df.empty:
                    continue

                for i, slew in enumerate(['V2SLEW', 'V3SLEW']):
                    rows.append(i + 1)
                    rows.append(i + 1)
                    cols.append(1)
                    cols.append(1)

                    line_fit, fit = self.line(df.EXPSTART, -df[slew])

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

        lines = [
            {
                'type': 'line',
                'x0': self.convert_day_of_year(value, mjd=True),
                'y0': self.figure['layout'][yaxis]['domain'][0],
                'x1': self.convert_day_of_year(value, mjd=True),
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

        layout = go.Layout(updatemenus=updatemenus, hovermode='closest', shapes=lines)
        self.figure['layout'].update(layout)

    def store_results(self):
        pass


class AcqPeakdMonitor(BaseMonitor):
    name = 'AcqPeakd Monitor'
    data_model = AcqPeakdModel
    labels = ['ROOTNAME', 'PROPOSID']
    output = COS_MONITORING

    def track(self):
        groups = self.data.groupby('dom_fgs')
        scatter = groups.ACQSLEWX.std()

        return groups, scatter

    def plot(self):
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

        layout = go.Layout(updatemenus=updatemenus, hovermode='closest')
        self.figure.add_traces(traces)
        self.figure['layout'].update(layout)


class AcqPeakxdMonitor(BaseMonitor):
    name = 'AcqPeakxd Monitor'
    data_model = AcqPeakxdModel
    labels = ['ROOTNAME', 'PROPOSID']
    output = COS_MONITORING

    def track(self):
        groups = self.data.groupby('dom_fgs')
        scatter = groups.ACQSLEWY.std()

        return groups, scatter

    def plot(self):
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

        layout = go.Layout(updatemenus=updatemenus, hovermode='closest')
        self.figure.add_traces(traces)
        self.figure['layout'].update(layout)

# TODO: Add OSM Shift monitor and datamodel. Draft output below after data is retrieved and put in a dataframe
# traces = []
# # figure = go.Figure()
# # idx = 0
# # for name, group in groups:
# #     group = group.sort_values('EXPSTART')
# #
# #     t = []
# #     lp = []
# #     for i, row in group.iterrows():
# #         row_t = [Time(row.EXPSTART, format='mjd').datetime + datetime.timedelta(seconds=lil_t) for lil_t in row.TIME]
# #         row_lp = [f'LP{row.LIFE_ADJ}' for _ in range(len(row.TIME))]
# #         lp += row_lp
# #         t += row_t
# #
# #     traces.append(
# #         go.Scattergl(
# #             x=t,
# #             y=[item for sublist in group.SHIFT_DISP.values for item in sublist],
# #             name='-'.join([str(item) for item in name]),
# #             mode='markers',
# #             marker=dict(
# #                 cmax=19,
# #                 cmin=0,
# #                 color=list(repeat(idx, len(t))),
# #                 colorscale='Viridis'
# #             ),
# #             text=lp
# #         )
# #     )
# #     idx += 1