import os
import plotly.graph_objects as go

from plotly.subplots import make_subplots

from ..filesystem import JitterFileData
from ..monitor_helpers import absolute_time

# TODO: Jitter Monitor needs to include 2 (possibly 3) subplots of statistics on the jitter per exposure.
#  Plot 1: mean vs time with +/- std "line boundaries"
#  Plot 2: max vs time
#  Plot 3 (potentially): std vs time


def view_jitter(jitter_file: str) -> go.Figure:
    """Plot Jitter data for a single jitter root or association."""
    # Get the data
    jitter_data = JitterFileData(
        jitter_file,
        primary_header_keys=('PROPOSID', 'CONFIG'),
        ext_header_keys=('EXPNAME',),
        table_keys=('SI_V2_AVG', 'SI_V3_AVG', 'SI_V2_RMS', 'SI_V3_RMS', 'Seconds'),
        get_expstart=True
    )

    # Start the figure
    figure = make_subplots(2, 1, shared_xaxes=True)

    for jit in jitter_data:
        for direction, position in zip(['V2', 'V3'], [(1, 1), (2, 1)]):
            # Set time
            time = absolute_time(expstart=jit['EXPSTART'], time=jit['Seconds'])

            # Lower bound
            figure.add_trace(
                go.Scatter(
                    x=time.to_datetime(),
                    y=jit[f'SI_{direction}_AVG'] - jit[f'SI_{direction}_RMS'],
                    mode='lines',
                    line={'width': 0},
                    showlegend=False,
                    legendgroup=jit['EXPNAME'],
                    name=f'{jit["EXPNAME"]} -RMS'
                ),
                row=position[0],
                col=position[-1]
            )

            # Upper bound
            figure.add_trace(
                go.Scatter(
                    x=time.to_datetime(),
                    y=jit[f'SI_{direction}_AVG'] + jit[f'SI_{direction}_RMS'],
                    mode='lines',
                    line={'width': 0},
                    fillcolor='rgba(68, 68, 68, 0.1)',
                    fill='tonexty',
                    showlegend=False,
                    legendgroup=jit['EXPNAME'],
                    name=f'{jit["EXPNAME"]} +RMS'
                ),
                row=position[0],
                col=position[-1]
            )

            # Jitter
            figure.add_trace(
                go.Scatter(
                    x=time.to_datetime(),
                    y=jit[f'SI_{direction}_AVG'],
                    mode='lines',
                    legendgroup=jit['EXPNAME'],
                    name=f'{jit["EXPNAME"]} - {direction}',
                    hovertemplate='Time=%{x}<br>Jitter=%{y} arcseconds'
                ),
                row=position[0],
                col=position[-1]
            )

    figure.update_layout(
        {
            'title': f'{os.path.basename(jitter_file)} {jitter_data[0]["CONFIG"]}',
            'xaxis': {'title': 'Datetime'},
            'yaxis': {'title': 'V2 Jitter (averaged over 3 seconds) [arcseconds]'},
            'xaxis2': {'title': 'Datetime'},
            'yaxis2': {'title': 'V3 Jitter (averaged over 3 seconds) [arcseconds]'}
        }
    )

    return figure
