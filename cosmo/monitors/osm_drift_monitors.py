import plotly.graph_objs as go

from astropy.time import Time
from monitorframe.monitor import BaseMonitor

from .osm_data_models import OSMDriftDataModel
from ..monitor_helpers import explode_df
from .. import SETTINGS

COS_MONITORING = SETTINGS['output']


def get_osmdrift_data(data, detector):
    """Get OSM Drift monitoring data."""
    df = data[data.DETECTOR == detector].reset_index(drop=True)

    # Calculate the relative shift (relative to the first shift measurement for each set of flashes) for AD and XD
    df['REL_SHIFT_DISP'] = df.apply(lambda x: x.SHIFT_DISP[1:] - x.SHIFT_DISP[0], axis=1)
    df['REL_SHIFT_XDISP'] = df.apply(lambda x: x.SHIFT_XDISP[1:] - x.SHIFT_XDISP[0], axis=1)

    # Drop the first value for the other data columns
    for col in ['TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT']:
        df[col] = df.apply(lambda x: x[col][1:], axis=1)

    # Expand the dataframe
    exploded = explode_df(
        df, ['TIME', 'SHIFT_DISP', 'SHIFT_XDISP', 'SEGMENT', 'REL_SHIFT_DISP', 'REL_SHIFT_XDISP']
    )

    # Add drift columns and time since OSM move columns
    exploded = exploded.assign(
        SHIFT1_DRIFT=lambda x: x.REL_SHIFT_DISP / x.TIME,
        SHIFT2_DRIFT=lambda x: x.REL_SHIFT_XDISP / x.TIME,
        REL_TSINCEOSM1=lambda x: x.TIME + x.TSINCEOSM1,
        REL_TSINCEOSM2=lambda x: x.TIME + x.TSINCEOSM2,
    )

    return exploded


class FUVOSMDriftMonitor(BaseMonitor):
    """FUV OSM Drift monitor. Includes the drift for both along and cross-dispersion directions as a function of time
    since the last OSM1 move.
    """
    data_model = OSMDriftDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID', 'OPT_ELEM', 'SEGMENT']

    subplots = True
    subplot_layout = (2, 1)  # 2 rows, 1 column
    # This is the format of your plot grid:
    # [ (1,1)  x1,y1 ]
    # [ (2,1) x2,y2 ]

    def get_data(self):
        return get_osmdrift_data(self.model.new_data, 'FUV')

    def track(self):
        """Track statistics for the drift for SHIFT1 and SHIFT2 for each LP."""
        lp_groups = self.data.groupby('LIFE_ADJ')
        lp_stats = {
            'SHIFT1_DRIFT': lp_groups.SHIFT1_DRIFT.describe(),
            'SHIFT2_DRIFT': lp_groups.SHIFT2_DRIFT.describe()
        }

        return lp_groups, lp_stats

    def plot(self):
        """Plot the Drift rate (from SHIFT1 and SHIFT2) as a function of the time since the last OSM1 move."""
        locations = [(1, 1), (2, 1)]  # row and column positions for the plot
        y_names = ['SHIFT1_DRIFT', 'SHIFT2_DRIFT']
        titles = ['OSM1 SHIFT1', 'OSM1 SHIFT2']

        # Set the min and max for the scale so that each plot is plotted on the same scale
        c_min = self.data.EXPSTART.min()
        c_max = self.data.EXPSTART.max()

        lp_groups, _ = self.results

        # Plot drift v time per grating
        for lp, group in lp_groups:
            for y, name, axes in zip(y_names, titles, locations):
                trace = go.Scattergl(
                    x=group.REL_TSINCEOSM1,
                    y=group[y],
                    mode='markers',
                    name=f'LP{lp} {name}',
                    text=group.hover_text,
                    legendgroup=lp,
                    marker=dict(
                        color=group.EXPSTART,
                        cmin=c_min,
                        cmax=c_max,
                        colorscale='Viridis',
                        showscale=True,
                        colorbar=dict(
                            title='Observation Date',
                            tickmode='array',
                            ticks='outside',
                            tickvals=[self.data.EXPSTART.min(), self.data.EXPSTART.mean(), self.data.EXPSTART.max()],
                            ticktext=[
                                f'{Time(self.data.EXPSTART.min(), format="mjd").to_datetime().date()}',
                                f'{Time(self.data.EXPSTART.mean(), format="mjd").to_datetime().date()}',
                                f'{Time(self.data.EXPSTART.max(), format="mjd").to_datetime().date()}'
                            ],
                            len=0.7,
                            y=0,
                            yanchor='bottom'
                        ),
                    ),
                )

                self.figure.append_trace(trace, *axes)

        # Set the layout.
        layout = go.Layout(
            title=self.name,
            xaxis=dict(title='Time since last OSM1 move [s]'),
            xaxis2=dict(title='Time since last OSM1 move [s]'),
            yaxis=dict(title='SHIFT1 drift rate [pixels/sec]'),
            yaxis2=dict(title='SHIFT2 drift rate [pixels/sec]')
        )

        self.figure.update_layout(layout)

    def store_results(self):
        # This will be implemented with the database backend.
        pass


class NUVOSMDriftMonitor(BaseMonitor):
    data_model = OSMDriftDataModel
    output = COS_MONITORING
    labels = ['ROOTNAME', 'LIFE_ADJ', 'FPPOS', 'PROPOSID', 'OPT_ELEM']

    subplots = True
    subplot_layout = (2, 2)
    # This is the format of your plot grid:
    # [ (1,1) x1,y1 ]  [ (1,2) x2,y2 ]
    # [ (2,1) x3,y3 ]  [ (2,2) x4,y4 ]  The axes labels and x/y combinations need to match this layout.

    def get_data(self):
        return get_osmdrift_data(self.model.new_data, 'NUV')

    def track(self):
        """Track SHIFT1 and SHIFT2 statistics per SEGMENT (stripe for NUV)."""
        segment_groups = self.data.groupby('SEGMENT')
        segment_stats = {
            'SHIFT1_DRIFT': segment_groups.SHIFT1_DRIFT.describe(),
            'SHIFT2_DRIFT': segment_groups.SHIFT2_DRIFT.describe()
        }

        return segment_groups, segment_stats

    def plot(self):
        """Plot drift rate (from SHIFT1 and SHIFT2) as a function of time since the last OSM1 move and the time since
        the last OSM2 move. NUV requires the movement of both OSM1 and OSM2, so both should be looked at.
        """
        # Set up the different parametrization for the four plots.
        x_names = ['REL_TSINCEOSM1', 'REL_TSINCEOSM1', 'REL_TSINCEOSM2', 'REL_TSINCEOSM2']
        y_names = ['SHIFT1_DRIFT', 'SHIFT2_DRIFT', 'SHIFT1_DRIFT', 'SHIFT2_DRIFT']
        titles = ['OSM1 SHIFT1', 'OSM1 SHIFT2', 'OSM2 SHIFT1', 'OSM2 SHIFT2']
        locations = [(1, 1), (2, 1), (1, 2), (2, 2)]

        # Set the min and max for the scale so that each plot is plotted on the same scale
        c_min = self.data.DATETIME_START.min()
        c_max = self.data.DATETIME_START.max()

        segment_groups, _ = self.results

        # Plot drift v time for each grating
        for segment, group in segment_groups:
            for x, y, axes, name in zip(x_names, y_names, locations, titles):
                trace = go.Scattergl(
                    x=group[x],
                    y=group[y],
                    mode='markers',
                    text=group.hover_text,
                    name=f'{segment} {name}',
                    legendgroup=segment,
                    marker=dict(
                        color=group.DATETIME_START,
                        cmin=c_min,
                        cmax=c_max,
                        colorscale='Viridis',
                        showscale=True,
                        colorbar=dict(  # TODO: Move the colorbar location down
                            title='Date of Observation'
                        )
                    ),
                )

                self.figure.append_trace(trace, *axes)

        # Set the layout. Refer to the commented plot grid to make sense of this.
        layout = go.Layout(
            title=self.name,
            xaxis=dict(title='Time since last OSM1 move [s]'),
            xaxis3=dict(title='Time since last OSM1 move [s]'),
            xaxis2=dict(title='Time since last OSM2 move [s]'),
            xaxis4=dict(title='Time since last OSM2 move [s]'),
            yaxis=dict(title='SHIFT1 drift rate [pixels/sec]'),
            yaxis3=dict(title='SHIFT2 drift rate [pixels/sec]'),
            yaxis2=dict(title='SHIFT1 drift rate [pixels/sec]'),
            yaxis4=dict(title='SHIFT2 drift rate [pixels/sec]')
        )

        self.figure.update_layout(layout)
