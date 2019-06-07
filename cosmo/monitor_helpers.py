import pandas as pd
import numpy as np
import datetime

from astropy.time import Time, TimeDelta
from typing import Union, Iterable, Tuple, Sequence

ARRAY = Union[Sequence, Iterable]


def convert_day_of_year(date: Union[float, str], mjd=False) -> Union[int, datetime.datetime]:
    """Convert day of the year (defined as yyyy.ddd where ddd is the day number of that year) to mjd or datetime object.
    Some important dates for the COS team were recorded in this format.
    """
    if not (isinstance(date, float) or isinstance(date, str)):
        raise TypeError('date can only be float or string')

    t = datetime.datetime.strptime(str(date), '%Y.%j')

    if mjd:
        return Time(t, format='datetime').mjd

    return t


def fit_line(x: Union[Iterable, Sequence], y: Union[Iterable, Sequence]) -> Tuple[np.poly1d, np.ndarray]:
    """Given arrays x and y, fit a line."""
    if len(x) != len(y):
        raise ValueError('x and y must be the same length')

    fit = np.poly1d(np.polyfit(x, y, 1))

    return fit, fit(x)


def explode_df(df: pd.DataFrame, list_keywords: Union[list, tuple]) -> pd.DataFrame:
    """If a dataframe contains arrays for the element of a column or columns given by list_keywords, expand the
    dataframe to one row per array element. Each column in list_keywords must be the same length.
    """
    for column in list_keywords:
        if len(df[column]) != len(df[list_keywords[0]]):
            raise ValueError('Columns in list_keywords to be "exploded" must all be the same lenght')

    idx = df.index.repeat(df[list_keywords[0]].str.len())
    df1 = pd.concat([pd.DataFrame({x: np.concatenate(df[x].values)}) for x in list_keywords], axis=1)
    df1.index = idx

    return df1.join(df.drop(list_keywords, 1), how='left').reset_index(drop=True)


class AbsoluteTime:
    """Class that encapsulates the computation of an 'absolute time' for COS Data, where the 'absolute time' is defined
    as the data's time array (assumed to be relative to the start of the exposure, t=0s) relative to the file's
    EXPSTART [mjd].
    """
    def __init__(self, df: pd.DataFrame = None, expstart: ARRAY[float] = None, time_array: ARRAY = None,
                 time_array_key: str = None):
        """Initalize AbsoluteTime from a dataframe or arrays.
        Optionally provide a time_array_key keyword if ingesting from a dataframe which contains a time array with a
        different name from 'TIME', or if the dataframe contains multiple 'time' columns.
        """
        self.df = df
        self.expstart = expstart
        self.time = time_array
        self.time_key = time_array_key
        self.expstart_time = None

        if self.df is not None:
            if self.expstart is not None or self.time is not None:
                raise ValueError('Can use a dataframe or the expstart and time_array arguments, not both.')

            self._ingest_df()

        if (self.expstart is not None and self.time is None) or (self.expstart is None and self.time is not None):
            raise ValueError('If expstart and time_array must be used together.')

        if self.df is None and self.expstart is None and self.time is None:
            raise ValueError('Computing and absolute time requires either a dataframe or set of arrays')

    def _ingest_df(self):
        if 'EXPSTART' not in self.df:
            raise KeyError('Could not find an EXPSTART column.')

        self.expstart = self.df.EXPSTART

        if not self.time_key:
            try:
                self.time = self.df.TIME

            except (KeyError, AttributeError):
                raise KeyError(
                    'Could not find a TIME column. Try setting the time_key attribute if the TIME column has a '
                    'different name.'
                )

        else:
            self.time = self.df[self.time_key]

    def compute_absolute_time(self, time_delta_format: str = 'sec') -> TimeDelta:
        self.expstart_time = Time(self.expstart, format='mjd')
        time_delta = TimeDelta(self.time, format=time_delta_format)

        return self.expstart_time + time_delta

    @classmethod
    def from_df(cls, df: pd.DataFrame, time_array_key: str = None):
        """Initialize from a dataframe."""
        return cls(df=df, time_array_key=time_array_key)

    @classmethod
    def from_arrays(cls, expstart: ARRAY[float], time_array: ARRAY):
        """Initialize from arrays."""
        return cls(expstart=expstart, time_array=time_array)

    @classmethod
    def compute_from_df(cls, df: pd.DataFrame, time_array_key: str = None,
                        time_format: str = 'sec') -> TimeDelta:
        """Compute the absolute time and """
        instance = cls(df=df, time_array_key=time_array_key)

        return instance.compute_absolute_time(time_delta_format=time_format)

    @classmethod
    def compute_from_arrays(cls, expstart: ARRAY[float], time_array: ARRAY,
                            time_format: str = 'sec') -> TimeDelta:
        instance = cls(expstart=expstart, time_array=time_array)

        return instance.compute_absolute_time(time_delta_format=time_format)
