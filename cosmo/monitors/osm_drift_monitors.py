import plotly.graph_objs as go

from monitorframe import BaseMonitor

from .osm_data_models import OSMDriftDataModel
from ..monitor_helpers import explode_df
from .. import SETTINGS

COS_MONITORING = SETTINGS['output']


class OSMDriftMonitor(BaseMonitor):
    """Baseclass for the FUV and NUV Drift Monitor. This class is a partial implementation and is intended to be
    inherited.
    """
    data_model = OSMDriftDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID', 'OPT_ELEM']

    detector = None
    subplots = True  # Both monitors will have subplots. The number and organization will depend on the detector.

    def track(self):
        """Track the drift for Shift1 and Shift2."""
        # Calculate the relative shift (relative to the first shift measurement for each set of flashes) for AD and XD
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

        # Add drift columns and time since OSM move columns
        exploded = exploded.assign(
            SHIFT1_DRIFT=lambda x: x.REL_SHIFT_DISP / x.TIME,
            SHIFT2_DRIFT=lambda x: x.REL_SHIFT_XDISP / x.TIME,
            REL_TSINCEOSM1=lambda x: x.TIME + x.TSINCEOSM1,
            REL_TSINCEOSM2=lambda x: x.TIME + x.TSINCEOSM2,
        )

        # Add SEGMENT to the hover text
        exploded.hover_text = exploded.apply(lambda x: f'{x.SEGMENT}<br>' + x.hover_text, axis=1)

        return exploded

    def filter_data(self):
        """Filter data on detector."""
        return self.data[self.data.DETECTOR == self.detector].reset_index(drop=True)

    def store_results(self):
        # TODO: define what to store and how
        pass


class FUVOSMDriftMonitor(OSMDriftMonitor):
    """FUV OSM Drift monitor. Includes the drift for both along and cross-dispersion directions as a function of time
    since the last OSM1 move.
    """
    detector = 'FUV'
    subplot_layout = (2, 1)  # 2 rows, 1 column
    # This is the format of your plot grid:
    # [ (1,1)  x1,y1 ]
    # [ (2,1) x2,y2 ]

    def plot(self):
        """Plot the Drift rate (from SHIFT1 and SHIFT2) as a function of the time since the last OSM1 move."""
        locations = [(1, 1), (2, 1)]  # row and column positions for the plot
        ynames = ['SHIFT1_DRIFT', 'SHIFT2_DRIFT']
        titles = ['OSM1 SHIFT1', 'OSM1 SHIFT2']

        # Set the min and max for the scale so that each plot is plotted on the same scale
        cmin = self.results.EXPSTART.min()
        cmax = self.results.EXPSTART.max()

        # Plot drift v time per grating
        for grating, group in self.results.groupby('OPT_ELEM'):
            for y, name, axes in zip(ynames, titles, locations):
                trace = go.Scattergl(
                    x=group.REL_TSINCEOSM1,
                    y=group[y],
                    mode='markers',
                    name=f'{grating} {name}',
                    text=group.hover_text,
                    legendgroup=grating,
                    marker=dict(
                        color=group.EXPSTART,
                        cmin=cmin,
                        cmax=cmax,
                        colorscale='Viridis',
                        showscale=True,
                        colorbar=dict(
                            len=0.65,
                            title='EXPSTART [mjd]'
                        ),
                    ),
                )

                self.figure.append_trace(trace, *axes)

        # Set the layout.
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
    # This is the format of your plot grid:
    # [ (1,1) x1,y1 ]  [ (1,2) x2,y2 ]
    # [ (2,1) x3,y3 ]  [ (2,2) x4,y4 ]  The axes labels and x/y combinations need to match this layout.

    def plot(self):
        """Plot drift rate (from SHIFT1 and SHIFT2) as a function of time since the last OSM1 move and the time since
        the last OSM2 move. NUV requires the movement of both OSM1 and OSM2, so both should be looked at.
        """
        # Set up the different parameterizations for the four plots.
        xnames = ['REL_TSINCEOSM1', 'REL_TSINCEOSM1', 'REL_TSINCEOSM2', 'REL_TSINCEOSM2']
        ynames = ['SHIFT1_DRIFT', 'SHIFT2_DRIFT', 'SHIFT1_DRIFT', 'SHIFT2_DRIFT']
        titles = ['OSM1 SHIFT1', 'OSM1 SHIFT2', 'OSM2 SHIFT1', 'OSM2 SHIFT2']
        locations = [(1, 1), (2, 1), (1, 2), (2, 2)]

        # Set the min and max for the scale so that each plot is plotted on the same scale
        cmin = self.results.EXPSTART.min()
        cmax = self.results.EXPSTART.max()

        # Plot drift v time for each grating
        for grating, group in self.results.groupby('OPT_ELEM'):
            for x, y, axes, name in zip(xnames, ynames, locations, titles):
                trace = go.Scattergl(
                    x=group[x],
                    y=group[y],
                    mode='markers',
                    text=group.hover_text,
                    name=f'{grating} {name}',
                    legendgroup=grating,
                    marker=dict(
                        color=group.EXPSTART,
                        cmin=cmin,
                        cmax=cmax,
                        colorscale='Viridis',
                        showscale=True,
                        colorbar=dict(  # TODO: Move the colorbar location down
                            len=0.65,
                            y=0,
                            yanchor='bottom',
                            title='EXPSTART [mjd]'
                        )
                    ),
                )

                self.figure.append_trace(trace, *axes)

        # Set the layout. Refer to the commented plot grid to make sense of this.
        layout = go.Layout(
            title=f'{self.detector} {self.name}',
            xaxis=dict(title='Time since last OSM1 move [s]'),
            xaxis3=dict(title='Time since last OSM1 move [s]'),
            xaxis2=dict(title='Time since last OSM2 move [s]'),
            xaxis4=dict(title='Time since last OSM2 move [s]'),
            yaxis=dict(title='SHIFT1 drift rate [pixels/sec]'),
            yaxis3=dict(title='SHIFT2 drift rate [pixels/sec]'),
            yaxis2=dict(title='SHIFT1 drift rate [pixels/sec]'),
            yaxis4=dict(title='SHIFT2 drift rate [pixels/sec]')
        )

        self.figure['layout'].update(layout)
