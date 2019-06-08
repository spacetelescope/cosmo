import pytest
import pandas as pd

from datetime import datetime
from cosmo.monitor_helpers import convert_day_of_year, fit_line, explode_df, AbsoluteTime


class TestConvertDayofYear:

    def test_mjd(self):
        test_date = 2019.100
        test_mjd = 58484.0

        result = convert_day_of_year(test_date, mjd=True)

        assert isinstance(result, float) is True
        assert result == test_mjd

    def test_datetime(self):
        test_date = 2017.301
        test_datetime = datetime(2017, 10, 28, 0, 0)

        result = convert_day_of_year(test_date)

        assert isinstance(result, datetime)
        assert result == test_datetime

    def test_date_type_fail(self):
        with pytest.raises(TypeError):
            convert_day_of_year(1)  # date can only be a string or float. This should fail


class TestFitLine:

    def test_simple_fit(self):
        test_x = test_y = [1, 2, 3]
        fit, fitline = fit_line(test_x, test_y)

        assert len(fit.coeffs) == 2  # should be a linear fit
        assert fit[1] == pytest.approx(1)  # slope of 1
        assert fit[0] == pytest.approx(0)  # intercept of 0

    def test_different_lengths_fail(self):
        test_x = [1, 2]
        test_y = [1, 2, 3]

        with pytest.raises(ValueError):
            fit_line(test_x, test_y)


class TestExplodeDf:

    def test_exploded_length(self):
        test_df = pd.DataFrame({'a': 1, 'b': [[1, 2, 3]]})

        exploded = explode_df(test_df, ['b'])

        assert len(exploded) == 3
        assert all(exploded.a == 1)


class TestAbsoluteTime:

    def test_fails_on_missing_keys(self):
        test_df = pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], })

        with pytest.raises(KeyError):
            AbsoluteTime.from_df(test_df)

    def test_fails_on_df_and_arrays(self):
        test_df = pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'TIME': [1, 2, 3]})

        test_expstart = test_time = [1, 2, 3]

        with pytest.raises(ValueError):
            AbsoluteTime(df=test_df, expstart=test_expstart, time_array=test_time)

    def test_no_input(self):
        with pytest.raises(ValueError):
            AbsoluteTime()

    def test_ingest_expstart_missing(self):
        test_df = pd.DataFrame({'TIME': [1, 2, 3]})

        with pytest.raises(KeyError):
            AbsoluteTime(df=test_df)

    def test_ingest_unspecified_time(self):
        test_df = pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'some_other_time': [1, 2, 3]})

        with pytest.raises(KeyError):
            AbsoluteTime(df=test_df)

    def test_specified_time(self):
        test_df = pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'some_other_time': [1, 2, 3]})

        AbsoluteTime(df=test_df, time_array_key='some_other_time')

    def test_input_arrays(self):
        test_expstart = test_time_array = [1, 2, 3]

        AbsoluteTime(expstart=test_expstart, time_array=test_time_array)

    def test_init_from_df(self):
        test_df = pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'TIME': [1, 2, 3]})

        AbsoluteTime.from_df(df=test_df)

    def test_init_from_df_other_time(self):
        test_df = pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'some_other_time': [1, 2, 3]})

        AbsoluteTime.from_df(test_df, time_array_key='some_other_time')

    def test_init_from_arrays(self):
        test_expstart = test_time_array = [1, 2, 3]

        AbsoluteTime.from_arrays(test_expstart, test_time_array)


