import plotly.graph_objs as go
import datetime

from itertools import repeat
from astropy.time import Time, TimeDelta

from monitorframe import BaseMonitor
from .osm_data_models import OSMDataModel

COS_MONITORING = '/grp/hst/cos2/monitoring'


class FUVADOSMShiftMonitor(BaseMonitor):
    data_model = OSMDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID', 'OBSET_ID']

    def track(self):
        # TODO: Define interesting things to track
        pass

    def filter_data(self):
        return self.data[self.data.DETECTOR == 'FUV']

    def plot(self):  # TODO: Add A - B subplot
        groups = self.filtered_data.groupby(['OPT_ELEM', 'CENWAVE'])
        lp4_move = datetime.datetime.strptime('2017-10-02', '%Y-%m-%d')

        traces = list()
        for i, group_info in enumerate(groups):
            name, group = group_info

            start_time = Time(group.EXPSTART, format='mjd')
            lamp_dt = TimeDelta(group.TIME, format='sec')
            lamp_time = start_time + lamp_dt

            traces.append(
                go.Scattergl(
                    x=lamp_time.to_datetime(),
                    y=group.SHIFT_DISP,
                    name='-'.join([str(item) for item in name]),
                    mode='markers',
                    marker=dict(
                        cmax=18,
                        cmin=0,
                        color=list(repeat(i, len(group))),
                        colorscale='Viridis',
                        symbol=group.FPPOS * 4,
                        size=[
                            10 if time > lp4_move and lp == 3 else 6
                            for lp, time in zip(group.LIFE_ADJ, start_time.to_datetime())
                        ]
                    ),
                    text=group.hover_text
                )
            )

        layout = go.Layout(
                xaxis=dict(title='Datetime'),
                yaxis=dict(title='AD Shift [pix]'),
            )

        self.figure.add_traces(traces)
        self.figure['layout'].update(layout)

    def store_results(self):
        # TODO: decide on how to store results and what needs to be stored
        pass
