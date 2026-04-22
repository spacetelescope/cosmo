import plotly.graph_objs as go
import numpy as np
import pandas as pd

from astropy.time import Time
from monitorframe.monitor import BaseMonitor
from typing import List

from .data_models import ScienceDataModel
from ..monitor_helpers import create_visibility
from .. import SETTINGS

ANCILLARY_DB = SETTINGS['ancillary_db']
COS_MONITORING = SETTINGS['output']

def get_science_data(datamodel, detector: str) -> pd.DataFrame:
    """Query for science data."""

    data = pd.DataFrame()

    if datamodel.model is not None:
        query = datamodel.model.select().where(datamodel.model.DETECTOR == detector)

        # Need to convert the stored array columns back into... arrays
        # data = data.append(
        data = pd.concat(
            [data, datamodel.query_to_pandas(
                query,
                array_cols=[
                    'APERTURE',
                    # 'APERXPOS',
                    # 'APERYPOS',
                    'DETECTOR',
                    'EXPSTART'
                    'LIFE_ADJ',
                    'OPT_ELEM',
                    'PROPOSID',
                    # 'PROP_TYP',
                    'ROOTNAME',
                    'SEGMENT'
                ],
            )],
            sort=True,
            ignore_index=True
        )

    if datamodel.new_data is None:
        return data

    if not datamodel.new_data.empty:
        new_data = datamodel.new_data[datamodel.new_data.DETECTOR == detector].reset_index(drop=True)
        data = pd.concat([data, new_data], sort=True, ignore_index=True)

    return data

def get_aperture_drift_data(datamodel: ScienceDataModel, detector: str) -> pd.DataFrame:
    """Get Aperture Drift monitoring data."""

    # Read primary DataFrame.
    primary = get_science_data(datamodel, detector)

    # Read ancillary DataFrame.  Drop duplicate entries.
    file_path = ANCILLARY_DB
    ancillary = pd.read_csv(file_path, usecols=['APERXPOS', 'APERYPOS', 'ROOTNAME', 'PROP_TYP'])
    ancillary = ancillary.drop_duplicates(subset=["ROOTNAME"])

    # Merge data and ancillary DataFrames.
    df = primary.join(ancillary.set_index("ROOTNAME"), on="ROOTNAME")

    # Replace LP < 1 with NaN.
    df.loc[df['LIFE_ADJ'] < 1, 'LIFE_ADJ'] = pd.NA

    # Remove any rows for which necessary keywords are not defined.
    df = df.dropna(subset=['APERYPOS', 'LIFE_ADJ', 'APERTURE', 'DETECTOR'], ignore_index = True)

    # Extract expected position of aperture block based on LP, aperture, and detector.
    aperture_block_positions = np.array(
        [[[126, 126], [-153, -153], [-153, -153], [126, 126]],  # LP1: PSA, BOA, FCA, WCA: FUV, NUV
         [[ 53, 126], [-226, -153], [-226, -153], [ 53, 126]],  # LP2
         [[181, 126], [ -98, -153], [ -98, -153], [181, 126]],  # LP3
         [[234, 126], [ -45, -153], [ -45, -153], [234, 126]],  # LP4
         [[ 13, 126], [-226, -153], [-226, -153], [ 13, 126]],  # LP5
         [[-11, 126], [ -98, -153], [ -98, -153], [ 22, 126]],  # LP6
         [[-49, 126], [ -98, -153], [ -98, -153], [ 32, 126]],  # LP7
         [[206, 126], [ -73, -153], [ -73, -153], [206, 126]],  # LP8
         [[206, 126], [ -73, -153], [ -73, -153], [206, 126]],  # LP10
         [[270, 126], [  -9, -153], [  -9, -153], [270, 126]],  # LP11
         [[ 90, 126], [-189, -153], [-189, -153], [ 90, 126]]]  # LP12
        )

    length = len(df)
    i = np.empty(length, dtype=np.int_)
    j = np.empty(length, dtype=np.int_)
    k = np.empty(length, dtype=np.int_)

    for n, lp in enumerate([1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]):
        i[df.LIFE_ADJ == lp] = n

    for n, aperture in enumerate(['PSA', 'BOA', 'FCA', 'WCA']):
        j[df.APERTURE == aperture] = n

    for n, detector in enumerate(['FUV', 'NUV']):
        k[df.DETECTOR == detector] = n

    std_ypos = np.array([aperture_block_positions[i[l], j[l], k[l]] for l in range(length)]).flatten()

    # Append array of aperture shifts to data structure.
    df = df.assign(
        SHIFT_APERY=lambda x: x.APERYPOS - std_ypos,
        )

    return df


class BaseApertureShiftMonitor(BaseMonitor):
    """
    Abstracted Aperture Shift monitor. This monitor class is not meant to be
    used directly, but rather inherited by the individual aperture monitors.
    """

    data_model = ScienceDataModel
    output = COS_MONITORING
    docs = "https://spacetelescope.github.io/cosmo/monitors.html#aperture-shift-monitors"
    labels = ['ROOTNAME', 'LIFE_ADJ', 'PROPOSID', 'OPT_ELEM', 'SEGMENT', 'PROP_TYP']

    subplots = True,
    subplot_layout = (5, 2)     # 5 rows, 2 columns

    # This is the format of your plot grid:
    # [ (1,1) x1,y1 ]  [ (1,2) x2,y2 ]
    # [ (2,1) x3,y3 ]  [ (2,2) x4,y4 ]
    # [ (3,1) x5,y5 ]  [ (3,2) x6,y6 ]
    # [ (4,1) x7,y7 ]  [ (4,2) x8,y8 ]
    # [ (5,1) x9,y9 ]  [ (5,2) x10,y10 ]  The axes labels and x/y combinations need to match this layout.

    detector = None     # FUV or NUV
    shift = None        # For now, it's always SHIFT_APERY

    def get_data(self):
        return get_aperture_drift_data(self.model, self.detector)

    def track(self):
        pass

    def plot(self):
        """
        Plot aperture shift as a function of time.  Separate subplots for each lifetime position.
        Buttons allow the user to select among the four apertures.
        """
        locations = [(1,1), (1,2), (2,1), (2,2), (3,1), (3,2), (4,1), (4,2), (5,1), (5,2)]

        trace_counts = {}

        # Outer loop creates ten subplots for each aperture.  User selects aperture using buttons.
        for aperture, ap_group in self.data.groupby('APERTURE'):
            # Inner loop plots aperture shift v time with each lifetime position in a separate subplot.
            i = 0
            for life_adj, lp_group in ap_group.groupby('LIFE_ADJ'):
                axes = locations[i]
                self.figure.add_trace(
                    go.Scattergl(
                        x=Time(lp_group['EXPSTART'], format='mjd').to_datetime(),
                        y=lp_group['SHIFT_APERY'],
                        mode='markers',
                        text=lp_group.hover_text,
                        showlegend=False
                    ),
                    *axes
                )
                i += 1

            trace_counts[aperture] = i

        # Create buttons for each aperture.
        titles = [
            f'<a href="{self.docs}">{self.name} {aperture}</a>'
            for aperture in trace_counts.keys()
        ]

        labels = [f'{aperture}' for aperture in trace_counts.keys()]

        # Create trace visibility options
        traces = list(trace_counts.values())
        psa = create_visibility(traces, [True, False, False, False])
        boa = create_visibility(traces, [False, True, False, False])
        fca = create_visibility(traces, [False, False, True, False])
        wca = create_visibility(traces, [False, False, False, True])

        updatemenus = [
            go.layout.Updatemenu(
                buttons=[
                    dict(
                        label=label,
                        method='update',
                        args=[{'visible': visible}, {'title': title}]
                    ) for label, title, visible in zip(labels, titles, [psa, boa, fca, wca])
                ]
            )
        ]

        # Set the layout. Refer to the plot grid (above) to make sense of this.
        layout = go.Layout(
            title=f'<a href="{self.docs}">{self.name}</a>',
            xaxis=dict(title='Date'),
            xaxis2=dict(title='Date'),
            xaxis3=dict(title='Date'),
            xaxis4=dict(title='Date'),
            xaxis5=dict(title='Date'),
            xaxis6=dict(title='Date'),
            xaxis7=dict(title='Date'),
            xaxis8=dict(title='Date'),
            xaxis9=dict(title='Date'),
            xaxis10=dict(title='Date'),
            yaxis=dict(title='Aperture Shift [steps]'),
            yaxis2=dict(title='Aperture Shift [steps]'),
            yaxis3=dict(title='Aperture Shift [steps]'),
            yaxis4=dict(title='Aperture Shift [steps]'),
            yaxis5=dict(title='Aperture Shift [steps]'),
            yaxis6=dict(title='Aperture Shift [steps]'),
            yaxis7=dict(title='Aperture Shift [steps]'),
            yaxis8=dict(title='Aperture Shift [steps]'),
            yaxis9=dict(title='Aperture Shift [steps]'),
            yaxis10=dict(title='Aperture Shift [steps]'),
            width=1600,                                   # Width in pixels
            height=1200,                                  # Height in pixels
            autosize=False,
            # margin=dict(l=50, r=50, b=50, t=50),         # Optional: adjust margins
            updatemenus=updatemenus
        )

        self.figure.update_layout(layout)

        # Label the whole figure and each of its subplots.
        self.figure.add_annotation(
            text=self.name, # The text content
            x=0.5, # X position in subplot coordinates
            y=1.07, # Y position in subplot coordinates
            xref="paper", yref="paper", # Coordinate reference: subplot coordinates
            font=dict(size=20),
            showarrow=False # Do not show an arrow
        )
        self.figure.add_annotation(
            text="LP1 " + self.shift, # The text content
            x=1.0, # X position in subplot coordinates
            y=1.2, # Y position in subplot coordinates
            xref="x domain", yref="y domain", # Coordinate reference: subplot coordinates
            font=dict(size=15),
            showarrow=False # Do not show an arrow
        )
        if (self.detector == 'NUV'): return
        self.figure.add_annotation(
            text="LP2 " + self.shift, # The text content
            x=1.0, # X position in subplot coordinates
            y=1.2, # Y position in subplot coordinates
            xref="x2 domain", yref="y2 domain", # Coordinate reference: subplot coordinates
            font=dict(size=15),
            showarrow=False # Do not show an arrow
        )
        self.figure.add_annotation(
            text="LP3 " + self.shift, # The text content
            x=1.0, # X position in subplot coordinates
            y=1.2, # Y position in subplot coordinates
            xref="x3 domain", yref="y3 domain", # Coordinate reference: subplot coordinates
            font=dict(size=15),
            showarrow=False # Do not show an arrow
        )
        self.figure.add_annotation(
            text="LP4 " + self.shift, # The text content
            x=1.0, # X position in subplot coordinates
            y=1.2, # Y position in subplot coordinates
            xref="x4 domain", yref="y4 domain", # Coordinate reference: subplot coordinates
            font=dict(size=15),
            showarrow=False # Do not show an arrow
        )
        self.figure.add_annotation(
            text="LP5 " + self.shift, # The text content
            x=1.0, # X position in subplot coordinates
            y=1.2, # Y position in subplot coordinates
            xref="x5 domain", yref="y5 domain", # Coordinate reference: subplot coordinates
            font=dict(size=15),
            showarrow=False # Do not show an arrow
        )
        self.figure.add_annotation(
            text="LP6 " + self.shift, # The text content
            x=1.0, # X position in subplot coordinates
            y=1.2, # Y position in subplot coordinates
            xref="x6 domain", yref="y6 domain", # Coordinate reference: subplot coordinates
            font=dict(size=15),
            showarrow=False # Do not show an arrow
        )
        self.figure.add_annotation(
            text="LP7 " + self.shift, # The text content
            x=1.0, # X position in subplot coordinates
            y=1.2, # Y position in subplot coordinates
            xref="x7 domain", yref="y7 domain", # Coordinate reference: subplot coordinates
            font=dict(size=15),
            showarrow=False # Do not show an arrow
        )
        self.figure.add_annotation(
            text="LP10 " + self.shift, # The text content
            x=1.0, # X position in subplot coordinates
            y=1.2, # Y position in subplot coordinates
            xref="x8 domain", yref="y8 domain", # Coordinate reference: subplot coordinates
            font=dict(size=15),
            showarrow=False # Do not show an arrow
        )
        if (trace_counts['PSA'] > 8): self.figure.add_annotation(
            text="LP11 " + self.shift, # The text content
            x=1.0, # X position in subplot coordinates
            y=1.2, # Y position in subplot coordinates
            xref="x9 domain", yref="y9 domain", # Coordinate reference: subplot coordinates
            font=dict(size=15),
            showarrow=False # Do not show an arrow
        )
        if (trace_counts['PSA'] > 9): self.figure.add_annotation(
            text="LP12 " + self.shift, # The text content
            x=1.0, # X position in subplot coordinates
            y=1.2, # Y position in subplot coordinates
            xref="x10 domain", yref="y10 domain", # Coordinate reference: subplot coordinates
            font=dict(size=15),
            showarrow=False # Do not show an arrow
        )

    def store_results(self):
        # TODO: decide what results to store and how
        pass


class FuvApertureShiftMonitor(BaseApertureShiftMonitor):
    """Aperture Shift (APERYPOS) monitor."""
    detector = 'FUV'
    shift = 'Aperture Y Shift'

    def track(self):
        print("Tracking FUV " + self.shift)

    run = 'monthly'


class NuvApertureShiftMonitor(BaseApertureShiftMonitor):
    """Aperture Shift (APERYPOS) monitor."""
    detector = 'NUV'
    shift = 'Aperture Y Shift'

    def track(self):
        print("Tracking NUV " + self.shift)

    run = 'monthly'
