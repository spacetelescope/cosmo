import pandas as pd
import numpy as np
import datetime

from astropy.time import Time, TimeDelta


def convert_day_of_year(date, mjd=False):
    t = datetime.datetime.strptime(str(date), '%Y.%j')

    if mjd:
        return Time(t, format='datetime').mjd

    return t


def fit_line(x, y):
    fit = np.poly1d(np.polyfit(x, y, 1))

    return fit, fit(x)


def explode_df(df, list_keywords):
    idx = df.index.repeat(df[list_keywords[0]].str.len())
    df1 = pd.concat([pd.DataFrame({x: np.concatenate(df[x].values)}) for x in list_keywords], axis=1)
    df1.index = idx

    return df1.join(df.drop(list_keywords, 1), how='left').reset_index(drop=True)


def compute_lamp_on_times(df):
    start_time = Time(df.EXPSTART, format='mjd')
    lamp_dt = TimeDelta(df.TIME, format='sec')
    lamp_time = start_time + lamp_dt

    return start_time, lamp_time
