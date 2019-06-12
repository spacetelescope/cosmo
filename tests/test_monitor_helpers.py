import pytest
import pandas as pd

from cosmo.monitor_helpers import convert_day_of_year, fit_line, explode_df, ExposureAbsoluteTime

CONVERT_DOY_GOOD_DATES = (('date',), [(2017.301,), ('2017.301',)])
CONVERT_DOY_BAD_DATES = (('date',), [(1,), ('1',)])


class TestConvertDayofYear:

    @pytest.mark.parametrize(*CONVERT_DOY_GOOD_DATES)
    def test_ingest(self, date):
        convert_day_of_year(date)

    @pytest.mark.parametrize(*CONVERT_DOY_BAD_DATES)
    def test_date_type_fail(self, date):
        with pytest.raises(ValueError):  # The format doesn't match
            convert_day_of_year(date)


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

        with pytest.raises(TypeError):
            fit_line(test_x, test_y)


class TestExplodeDf:

    def test_exploded_length(self):
        test_df = pd.DataFrame({'a': 1, 'b': [[1, 2, 3]]})
        exploded = explode_df(test_df, ['b'])

        assert len(exploded) == 3
        assert all(exploded.a == 1)

    def test_different_lengths_fail(self):
        test_df = pd.DataFrame({'a': 1, 'b': [[1, 2, 3]]})

        with pytest.raises(AttributeError):
            explode_df(test_df, ['a', 'b'])

        test_df['c'] = [[1, 2]]

        with pytest.raises(ValueError):
            explode_df(test_df, ['c', 'b'])


ABSTIME_BAD_INPUT = (
    ('df', 'expstart', 'time_array', 'error'),
    [
        (pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], }), None, None, AttributeError),
        (pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'TIME': [1, 2, 3]}), [1, 2, 3], [1, 2, 3], ValueError),
        (None, None, None, TypeError),
        (pd.DataFrame({'TIME': [1, 2, 3]}), None, None, AttributeError),
        (pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'some_other_time': [1, 2, 3]}), None, None,
         AttributeError),
    ]
)

ABSTIME_GOOD_INPUT = (
    ('df', 'expstart', 'time_array', 'time_array_key'),
    [
        (pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'TIME': [1, 2, 3]}), None, None, None),
        (pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'some_other_time': [1, 2, 3]}), None, None,
         'some_other_time'),
        (None, [1, 2, 3], [1, 2, 3], None)
    ]
)


class TestAbsoluteTime:

    @pytest.mark.parametrize(*ABSTIME_BAD_INPUT)
    def test_ingest_fails(self, df, expstart, time_array, error):
        with pytest.raises(error):
            ExposureAbsoluteTime(df=df, expstart=expstart, time_array=time_array)

    @pytest.mark.parametrize(*ABSTIME_GOOD_INPUT)
    def test_ingest_works(self, df, expstart, time_array, time_array_key):
        ExposureAbsoluteTime(df=df, expstart=expstart, time_array=time_array, time_array_key=time_array_key)
