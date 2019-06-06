import pytest
import pandas as pd

from datetime import datetime
from astropy.time import Time
from cosmo.monitor_helpers import convert_day_of_year, fit_line, explode_df, compute_absolute_time


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


class TestComputeAbsoluteTime:

    def test_fails_on_missing_keys(self):
        test_df = pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], })

        with pytest.raises(KeyError):
            compute_absolute_time(test_df)


