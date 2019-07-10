import plotly.graph_objs as go

from monitorframe import BaseMonitor

from .osm_data_models import OSMDriftDataModel
from ..monitor_helpers import explode_df
from .. import SETTINGS

COS_MONITORING = SETTINGS['output']


class OSMDriftMonitor(BaseMonitor):
    data_model = OSMDriftDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID', 'OPT_ELEM']

    detector = None
    subplots = True

    def track(self):
        """Track the drift for Shift1 and Shift2."""
        # Calculate the relative shift for AD and XD
        self.filtered_data['REL_SHIFT_DISP'] = self.filtered_data.apply(
            lambda x: x.SHIFT_DISP - x.SHIFT_DISP[0] if len(x.SHIFT_DISP) else x.SHIFT_DISP, axis=1
        )

        self.filtered_data['REL_SHIFT_XDISP'] = self.filtered_data.apply(
            lambda x: x.SHIFT_XDISP - x.SHIFT_XDISP[0] if len(x.SHIFT_XDISP) else x.SHIFT_XDISP, axis=1
        )

        # Expand the dataframe
        exploded = explode_df(
            self.filtered_data, ['TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT', 'REL_SHIFT_DISP', 'REL_SHIFT_XDISP']
        )

        exploded = exploded.assign(
            SHIFT1_DRIFT=lambda x: x.REL_SHIFT_DISP / x.TIME,
            SHIFT2_DRIFT=lambda x: x.REL_SHIFT_XDISP / x.TIME,
            REL_TSINCEOSM1=lambda x: x.TIME + x.TSINCEOSM1,
            REL_TSINCEOSM2=lambda x: x.TIME + x.TSINCEOSM2,
        )

        return exploded

    def filter_data(self):
        return self.data[self.data.DETECTOR == self.detector].reset_index(drop=True)

    def store_results(self):
        # TODO: define what to store and how
        pass


class FUVOSMDriftMonitor(OSMDriftMonitor):
    detector = 'FUV'
    subplot_layout = (2, 1)

    def plot(self):
        locations = [(1, 1), (2, 1)]
        ynames = ['SHIFT1_DRIFT', 'SHIFT2_DRIFT']
        titles = ['OSM1 SHIFT1', 'OSM1 SHIFT2']

        for y, name, axes in zip(ynames, titles, locations):
            trace = go.Scattergl(
                x=self.results.REL_TSINCEOSM1,
                y=self.results[y],
                mode='markers',
                name=name,
                text=self.results.hover_text,
                marker=dict(
                    color=self.results.EXPSTART,
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(
                        len=0.75,
                        title='EXPSTART [mjd]'
                    ),
                ),
            )

            self.figure.append_trace(trace, *axes)

        layout = go.Layout(
            title=f'{self.detector} {self.name}',
            xaxis=dict(title='Time since last OSM1 move [s]'),
            xaxis2=dict(title='Time since last OSM1 move [s]'),
            yaxis=dict(title='SHIFT1 drift rate [pixels/sec]'),
            yaxis2=dict(title='SHIFT2 drift rate [pixels/sec]')
        )

        self.figure['layout'].update(layout)


class NUVOSMDriftMonitor(OSMDriftMonitor):
    detector = 'NUV'
    subplot_layout = (2, 2)

    def plot(self):
        xnames = ['REL_TSINCEOSM1', 'REL_TSINCEOSM1', 'REL_TSINCEOSM2', 'REL_TSINCEOSM2']
        ynames = ['SHIFT1_DRIFT', 'SHIFT2_DRIFT', 'SHIFT1_DRIFT', 'SHIFT2_DRIFT']
        locations = [(1, 1), (2, 1), (1, 2), (2, 2)]

        for x, y, axes in zip(xnames, ynames, locations):
            trace = go.Scattergl(
                x=self.results[x],
                y=self.results[y],
                mode='markers',
                text=self.results.hover_text,
                marker=dict(
                    color=self.results.EXPSTART,
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(
                        len=0.75,
                        title='EXPSTART [mjd]'
                    )
                ),
            )

            self.figure.append_trace(trace, *axes)

        layout = go.Layout(
            title=f'{self.detector} {self.name}',
            xaxis=dict(title='Time since last OSM1 move [s]'),
            xaxis2=dict(title='Time since last OSM1 move [s]'),
            xaxis3=dict(title='Time since last OSM2 move [s]'),
            xaxis4=dict(title='Time since last OSM2 move [s]'),
            yaxis=dict(title='SHIFT1 drift rate [pixels/sec]'),
            yaxis2=dict(title='SHIFT2 drift rate [pixels/sec]'),
            yaxis3=dict(title='SHIFT1 drift rate [pixels/sec]'),
            yaxis4=dict(title='SHIFT2 drift rate [pixels/sec]')
        )

        self.figure['layout'].update(layout)