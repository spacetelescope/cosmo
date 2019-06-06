import pandas as pd
import numpy as np
import datetime

from astropy.time import Time, TimeDelta
from typing import Union, Iterable, Tuple, Sequence


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


def compute_absolute_time(df: pd.DataFrame) -> Tuple[Time, Time]:
    """Given a dataframe with EXPSTART keyword and a TIME column, compute the absolute time for the TIME column defined
    as EXPSTART + TIME[i] for each element, i in TIME.
    """
    if 'EXPSTART' not in df or 'TIME' not in df:
        raise KeyError('To compute the absolute time, EXPSTART and TIME must be present in the dataframe.')

    start_time = Time(df.EXPSTART, format='mjd')
    lamp_dt = TimeDelta(df.TIME, format='sec')
    lamp_time = start_time + lamp_dt

    return start_time, lamp_time
