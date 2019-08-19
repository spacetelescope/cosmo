import pandas as pd
import numpy as np
import datetime

from itertools import repeat
from astropy.time import Time, TimeDelta
from typing import Union, Iterable, Tuple, Sequence, List


def convert_day_of_year(date: Union[float, str]) -> Time:
    """Convert day of the year (defined as yyyy.ddd where ddd is the day number of that year) to mjd or datetime object.
    Some important dates for the COS team were recorded in this format.
    """
    return Time(datetime.datetime.strptime(str(date), '%Y.%j'), format='datetime')


def fit_line(x: Union[Iterable, Sequence], y: Union[Iterable, Sequence]) -> Tuple[np.poly1d, np.ndarray]:
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


class ExposureAbsoluteTime:
    """Class that encapsulates the computation of an 'absolute time' for COS Data, where the 'absolute time' is defined
    as the data's time array (assumed to be relative to the start of the exposure, t=0s) relative to the file's
    EXPSTART [mjd].
    """
    def __init__(self, df: pd.DataFrame = None, expstart: Union[Sequence, pd.Series] = None,
                 time_array: Union[Sequence, pd.Series] = None, time_array_key: str = None):
        """Initialize AbsoluteTime from a dataframe or arrays.
        Optionally provide a time_array_key keyword if ingesting from a dataframe which contains a time array with a
        different name from 'TIME', or if the dataframe contains multiple 'time' columns.
        """
        # If no input is given raise an error
        if df is None and expstart is None and time_array is None:
            raise TypeError('Computing and absolute time requires either a dataframe or set of arrays')

        self.df = df
        self.expstart = expstart
        self.time = time_array
        self.time_key = time_array_key
        self.expstart_time = None

        # Check that expstart and time_array are used together
        if bool(self.expstart or self.time) and not (self.expstart and self.time):
            raise TypeError('expstart and time_array must be used together.')

        # Ingest given dataframe if one is given and check that it's not used with arrays at the same time
        if self.df is not None:
            if bool(self.expstart or self.time):
                raise ValueError('Cannot use a dataframe and arrays as input at the same time. Use one or the other.')

            self._ingest_df()

    def _ingest_df(self):
        """Ingest the expstart and time columns of the input dataframe."""
        self.expstart = self.df.EXPSTART
        self.time = self.df[self.time_key] if self.time_key else self.df.TIME

    def compute_absolute_time(self, time_delta_format: str = 'sec') -> TimeDelta:
        """Compute a time array relative to the exposure start time, EXPSTART to create an 'absolute' time."""
        self.expstart_time = Time(self.expstart, format='mjd')
        time_delta = TimeDelta(self.time, format=time_delta_format)

        return self.expstart_time + time_delta

    @classmethod
    def compute_from_df(cls, df: pd.DataFrame, time_array_key: str = None, time_format: str = 'sec') -> TimeDelta:
        """Compute the absolute time from a dataframe."""
        instance = cls(df=df, time_array_key=time_array_key)

        return instance.compute_absolute_time(time_delta_format=time_format)

    @classmethod
    def compute_from_arrays(cls, expstart: Union[Sequence, pd.Series], time_array: Union[Sequence, pd.Series],
                            time_format: str = 'sec') -> TimeDelta:
        """Compute the absolute time from arrays."""
        instance = cls(expstart=expstart, time_array=time_array)

        return instance.compute_absolute_time(time_delta_format=time_format)


def create_visibility(trace_lengths: List[int], visible_list: List[bool]) -> List[bool]:
    """Create visibility lists for plotly buttons. trace_lengths and visible_list must be in the correct order.

    :param trace_lengths: List of the number of traces in each "button set".
    :param visible_list: Visibility setting for each button set (either True or False).
    """
    visibility = []  # Total visibility. Length should match the total number of traces in the figure.
    for visible, trace_length in zip(visible_list, trace_lengths):
        visibility += list(repeat(visible, trace_length))  # Set each trace per button.

    return visibility
