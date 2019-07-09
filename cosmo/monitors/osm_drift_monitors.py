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

    def track(self):
        """Track the drift for Shift1 and Shift2."""
        exploded = explode_df(self.data, ['TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT'])
        exploded = exploded.assign(
            SHIFT1_DRIFT=lambda x: x.SHIFT_DISP / x.TIME,
            SHIFT2_DRIFT=lambda x: x.SHIFT_XDISP / x.TIME,
            REL_TSINCEOSM1=lambda x: x.TIME + x.TSINCEOSM1,
            REL_TSINCEOSM2=lambda x: x.TIME + x.TSINCEOSM2
        )

        return exploded

    def plot(self):
        fuv, nuv = self.results[self.results.DETECTOR == 'FUV'], self.results[self.results.DETECTOR == 'NUV']

        fuv_traces = [
            go.Scattergl(
                x=fuv.REL_TSINCEOSM1,
                y=fuv[y],
                mode='markers',
                visible=False,
                name=name,
                text=fuv.hover_text,
                xaxis=axes[0],
                yaxis=axes[1],
                marker=dict(
                    color=fuv.EXPSTART,
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(
                        len=0.75,
                        title='EXPSTART [mjd]'
                    ),
                ),
            )
            for y, name, axes in zip(
                ['SHIFT1_DRIFT', 'SHIFT2_DRIFT'],
                ['OSM1 SHIFT1', 'OSM1 SHIFT2'],
                [('x', 'y'), ('x2', 'y2')]
            )
        ]

        nuv_traces = [
            go.Scattergl(
                x=nuv[x],
                y=nuv[y],
                mode='markers',
                visible=False,
                text=nuv.hover_text,
                xaxis=axes[0],
                yaxis=axes[1],
                marker=dict(
                    color=nuv.EXPSTART,
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(
                        len=0.75,
                        title='EXPSTART [mjd]'
                    )
                ),
            )
            for x, y, axes in zip(
                ['REL_TSINCEOSM1', 'REL_TSINCEOSM1', 'REL_TSINCEOSM2', 'REL_TSINCEOSM2'],
                ['SHIFT1_DRIFT', 'SHIFT2_DRIFT', 'SHIFT1_DRIFT', 'SHIFT2_DRIFT'],
                [('x', 'y'), ('x2', 'y2'), ('x3', 'y3'), ('x4', 'y4')]
            )
        ]

        traces = fuv_traces + nuv_traces

        fuv_visible = [True] * len(fuv_traces) + [False] * len(nuv_traces)
        nuv_visible = [False] * len(fuv_traces) + [True] * len(nuv_traces)

        updatemenus = [
            dict(
                active=-1,
                type='buttons',
                buttons=[
                    dict(
                        label=label,
                        method='update',
                        args=[
                            {'visible': visibility},
                            {'title': f'{label} {self.name}', **axes}
                        ]
                    ) for label, visibility, axes in zip(
                        ['FUV', 'NUV'],
                        [fuv_visible, nuv_visible],
                        [
                            {
                               'xaxis': {'anchor': 'y', 'domain': [0.0, 1.0]},
                               'xaxis2': {'anchor': 'y2', 'domain': [0.0, 1.0]},
                               'yaxis': {'anchor': 'x', 'domain': [0.575, 1.0]},
                               'yaxis2': {'anchor': 'x2', 'domain': [0.0, 0.425]},
                               'xaxis3': None,
                               'xaxis4': None,
                               'yaxis3': None,
                               'yaxis4': None

                            },
                            {
                                'xaxis': {'anchor': 'y', 'domain': [0.0, 0.45]},
                                'xaxis2': {'anchor': 'y2', 'domain': [0.55, 1.0]},
                                'xaxis3': {'anchor': 'y3', 'domain': [0.0, 0.45]},
                                'xaxis4': {'anchor': 'y4', 'domain': [0.55, 1.0]},
                                'yaxis': {'anchor': 'x', 'domain': [0.575, 1.0]},
                                'yaxis2': {'anchor': 'x2', 'domain': [0.575, 1.0]},
                                'yaxis3': {'anchor': 'x3', 'domain': [0.0, 0.425]},
                                'yaxis4': {'anchor': 'x4', 'domain': [0.0, 0.425]}
                            }
                        ]
                    )
                ]
            )
        ]

        layout = go.Layout(updatemenus=updatemenus)
        go.Layout()

        self.figure.add_traces(traces)
        self.figure['layout'].update(layout)

    def store_results(self):
        # TODO: define what to store and how
        pass
