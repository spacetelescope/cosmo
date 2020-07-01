import pandas as pd
import numpy as np
import datetime

from itertools import repeat
from astropy.time import Time, TimeDelta
from typing import Union, Tuple, Sequence, List


def convert_day_of_year(date: Union[float, str]) -> Time:
    """Convert day of the year (defined as yyyy.ddd where ddd is the day number of that year) to an astropy Time object.
    Some important dates for the COS team were recorded in this format.
    """
    return Time(
         datetime.datetime.strptime(
             f'{date:.3f}' if isinstance(date, float) else date,
             '%Y.%j'
         ),
         format='datetime'
    )


def fit_line(x: Sequence, y: Sequence) -> Tuple[np.poly1d, np.ndarray]:
    """Given arrays x and y, fit a line."""
    fit = np.poly1d(np.polyfit(x, y, 1))

    return fit, fit(x)


def explode_df(df: pd.DataFrame, list_keywords: list) -> pd.DataFrame:
    """If a dataframe contains arrays for the element of a column or columns given by list_keywords, expand the
    dataframe to one row per array element. Each row in list_keywords must be the same length.
    """
    idx = df.index.repeat(df[list_keywords[0]].str.len())  # Repeat values based on the number of elements in the arrays
    unpacked = pd.concat([pd.DataFrame({x: np.concatenate(df[x].values)}) for x in list_keywords], axis=1)
    unpacked.index = idx  # assigns repeated index to the unpacked dataframe, unpacked.

    # Join unpacked df to the original df and drop the old columns
    exploded = unpacked.join(df.drop(list_keywords, 1), how='left').reset_index(drop=True)

    if exploded.isna().values.any():  # If there are NaNs, then it didn't make sense to "explode" the input df
        raise ValueError('Elements in columns to be exploded are not the same length across rows.')

    return exploded


def absolute_time(df: pd.DataFrame = None, expstart: Sequence = None, time: Sequence = None, time_key: str = None,
                  time_format: str = 'sec') -> TimeDelta:
    """Compute the time sequence relative to the start of the exposure (EXPSTART). Can be computed from a DataFrame that
    contains an EXPSTART column and some other time array column, or from an EXPSTART array and time array pair.
    """
    # If no input is given raise an error
    if df is None and expstart is None and time is None:
        raise TypeError('Computing and absolute time requires either a dataframe or set of arrays')

    # Check that expstart and time_array are used together
    if bool(expstart is not None or time is not None) and not (expstart is not None and time is not None):
        raise TypeError('expstart and time must be used together.')

    # Ingest given dataframe if one is given and check that it's not used with arrays at the same time
    if df is not None:
        if bool(expstart is not None or time is not None):
            raise ValueError('Cannot use a dataframe and arrays as input at the same time. Use one or the other.')

        expstart = df.EXPSTART
        time = df.TIME if not time_key else df[time_key]

    zero_points = Time(expstart, format='mjd')
    time_delta = TimeDelta(time, format=time_format)

    return zero_points + time_delta


def create_visibility(trace_lengths: List[int], visible_list: List[bool]) -> List[bool]:
    """Create visibility lists for plotly buttons. trace_lengths and visible_list must be in the correct order.

    :param trace_lengths: List of the number of traces in each "button set".
    :param visible_list: Visibility setting for each button set (either True or False).
    """
    visibility = []  # Total visibility. Length should match the total number of traces in the figure.
    for visible, trace_length in zip(visible_list, trace_lengths):
        visibility += list(repeat(visible, trace_length))  # Set each trace per button.

    return visibility


def v2v3(slew_x: Sequence, slew_y: Sequence) -> Tuple[Union[np.ndarray, pd.Series], Union[np.ndarray, pd.Series]]:
    """Detector coordinates to V2/V3 coordinates."""
    # If input are lists, convert to np arrays so that the operations are completed as expected
    if isinstance(slew_x, list):
        slew_x = np.array(slew_x)

    if isinstance(slew_y, list):
        slew_y = np.array(slew_y)

    rotation_angle = np.radians(45.0)  # rotation angle in degrees converted to radians
    x_conversion = slew_x * np.cos(rotation_angle)
    y_conversion = slew_y * np.sin(rotation_angle)

    v2 = x_conversion + y_conversion
    v3 = x_conversion - y_conversion

    return v2, v3


def get_osm_data(datamodel, detector: str) -> pd.DataFrame:
    """Query for OSM data and append any relevant new data to it."""
    data = pd.DataFrame()

    if datamodel.model is not None:
        query = datamodel.model.select().where(datamodel.model.DETECTOR == detector)

        # Need to convert the stored array columns back into... arrays
        data = data.append(
            datamodel.query_to_pandas(
                query,
                array_cols=[
                    'TIME',
                    'SHIFT_DISP',
                    'SHIFT_XDISP',
                    'SEGMENT',
                    'XC_RANGE',
                    'LAMPTAB_SEGMENT',
                    'SEARCH_OFFSET',
                    'FP_PIXEL_SHIFT'
                ],
            ),
            sort=True,
            ignore_index=True
        )

    if datamodel.new_data is None:
        return data

    if not datamodel.new_data.empty:
        new_data = datamodel.new_data[datamodel.new_data.DETECTOR == detector].reset_index(drop=True)
        data = data.append(new_data, sort=True, ignore_index=True)

    return data
