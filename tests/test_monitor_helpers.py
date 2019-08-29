import pytest
import pandas as pd

from cosmo.monitor_helpers import convert_day_of_year, fit_line, explode_df, ExposureAbsoluteTime, create_visibility


@pytest.fixture(params=[2017.301, '2017.301'])
def good_date(request):
    return request.param


@pytest.fixture(params=[1, '1'])
def bad_date(request):
    return request.param


class TestConvertDayofYear:

    def test_ingest(self, good_date):
        convert_day_of_year(good_date)

    def test_date_type_fail(self, bad_date):
        with pytest.raises(ValueError):  # The format shouldn't match
            convert_day_of_year(bad_date)


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


@pytest.fixture
def test_df():
    return pd.DataFrame({'a': 1, 'b': [[1, 2, 3]]})


class TestExplodeDf:

    def test_exploded_length(self, test_df):
        exploded = explode_df(test_df, ['b'])
        assert len(exploded) == 3
        assert all(exploded.a == 1)

    def test_different_lengths_fail(self, test_df):
        with pytest.raises(AttributeError):
            explode_df(test_df, ['a', 'b'])  # Column a is not "explode-able"

        test_df['c'] = [[1, 2]]  # Add a column with an array element of a different length than b

        with pytest.raises(ValueError):
            explode_df(test_df, ['c', 'b'])

        # If the first column listed is longer, the procedure won't produce an error, but the result will have NaNs
        with pytest.raises(ValueError):
            explode_df(test_df, ['b', 'c'])


ABSTIME_BAD_INPUT = [
    (pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], }), None, None, AttributeError),
    (pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'TIME': [1, 2, 3]}), [1, 2, 3], [1, 2, 3], ValueError),
    (None, None, None, TypeError),
    (pd.DataFrame({'TIME': [1, 2, 3]}), None, None, AttributeError),
    (
        pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'some_other_time': [1, 2, 3]}),
        None,
        None,
        AttributeError
    ),
    (None, [1, 2, 3], None, TypeError),
    (None, None, [1, 2, 3], TypeError)
    ]

ABSTIME_GOOD_INPUT = [
        (pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'TIME': [1, 2, 3]}), None, None, None),
        (pd.DataFrame({'EXPSTART': [58484.0, 58485.0, 58486.0], 'some_other_time': [1, 2, 3]}), None, None,
         'some_other_time'),
        (None, [1, 2, 3], [1, 2, 3], None)
    ]


@pytest.fixture(params=ABSTIME_BAD_INPUT)
def bad_input(request):
    return request.param


@pytest.fixture(params=ABSTIME_GOOD_INPUT)
def good_input(request):
    return request.param


class TestAbsoluteTime:

    def test_ingest_fails(self, bad_input):
        df, expstart, time, error = bad_input

        with pytest.raises(error):
            ExposureAbsoluteTime(df=df, expstart=expstart, time_array=time)

    def test_ingest_works(self, good_input):
        df, expstart, time, time_key = good_input
        ExposureAbsoluteTime(df=df, expstart=expstart, time_array=time, time_array_key=time_key)

    def test_compute_absolute_time(self, good_input):
        df, expstart, time, time_key = good_input
        test_time = ExposureAbsoluteTime(df=df, expstart=expstart, time_array=time, time_array_key=time_key)

        test_time.compute_absolute_time()

    def test_compute_from_df(self, good_input):
        df, expstart, time, time_key = good_input

        if df is not None:
            ExposureAbsoluteTime.compute_from_df(df, time_array_key=time_key)

    def test_compute_from_arrays(self, good_input):
        df, expstart, time, time_key = good_input

        if expstart is not None and time is not None:
            ExposureAbsoluteTime.compute_from_arrays(expstart=expstart, time_array=time)


class TestCreateVisibility:

    def test_output(self):
        test_trace_lengths = [1, 2, 3]
        test_visible = [True, False, False]

        visible_options = create_visibility(test_trace_lengths, test_visible)

        assert len(visible_options) == 6
        assert visible_options == [True, False, False, False, False, False]
