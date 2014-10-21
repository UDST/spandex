import pandas as pd
import pandas.util.testing as pdt
import pytest

from spandex import targets as tgts


@pytest.fixture(scope='module')
def col():
    return pd.Series([1, 2, 3, 4, 5])


@pytest.fixture(scope='module')
def target_col():
    return 'target_col'


@pytest.fixture(scope='module')
def df(target_col):
    #    a  b  a  b  a  b  a  b  a   b
    l = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    return pd.DataFrame(
        {target_col: l,
         'geo_id': ['a', 'b'] * 5,
         'filter_col': [x + 100 for x in l]})


def test_update_series(col):
    col_copy = col.copy()
    s2 = pd.Series([9, 99], index=[2, 4])

    result = tgts.update_series(col, s2)

    pdt.assert_series_equal(col, col_copy)
    pdt.assert_series_equal(
        result,
        pd.Series([1, 2, 9, 4, 99]))


@pytest.mark.parametrize('metric', ['mean', 'median'])
def test_scale_col_to_target_mean_median(col, metric):
    target = 600
    expected = pd.Series([200, 400, 600, 800, 1000])

    result = tgts.scale_col_to_target(col, target, metric=metric)

    assert getattr(result, metric)() == target
    pdt.assert_series_equal(result, expected, check_dtype=False)


def test_scale_col_to_target_sum(col):
    target = 16
    expected = col * target / col.sum()

    result = tgts.scale_col_to_target(col, target, metric='sum')

    assert result.sum() == target
    pdt.assert_series_equal(result, expected)


def test_scale_col_to_target_clip(col):
    target = 600
    clip_low = 450
    clip_high = 999
    expected = pd.Series([450, 450, 600, 800, 999])

    result = tgts.scale_col_to_target(
        col, target, metric='mean', clip_low=clip_low, clip_high=clip_high)

    pdt.assert_series_equal(result, expected, check_dtype=False)


def test_scale_col_to_target_round(col):
    target = 16

    result = tgts.scale_col_to_target(
        col, target, metric='sum', int_result=True)

    pdt.assert_series_equal(result, col)


def test_scale_to_targets(df, target_col):
    targets = [100, 1000]
    filters = [['geo_id == "a"', 'filter_col < 106'], 'geo_id == "b"']
    metric = 'sum'

    result = tgts.scale_to_targets(df, target_col, targets, metric, filters)

    pdt.assert_index_equal(result.columns, df.columns)
    pdt.assert_series_equal(
        result[target_col],
        pd.Series(
            [11.11111111, 66.66666667, 33.33333333, 133.33333333, 55.55555556,
             200, 7, 266.66666667, 9, 333.33333333]),
        check_dtype=False)


def test_scale_to_targets_no_segments(df, target_col):
    target = [1000]
    metric = 'mean'

    result = tgts.scale_to_targets(df, target_col, target, metric=metric)

    pdt.assert_index_equal(result.columns, df.columns)
    pdt.assert_series_equal(
        result[target_col],
        pd.Series(
            [181.81818182, 363.63636364, 545.45454545, 727.27272727,
             909.09090909, 1090.90909091, 1272.72727273, 1454.54545455,
             1636.36363636, 1818.18181818]),
        check_dtype=False)


def test_scale_to_targets_clip_int(df, target_col):
    target = [1000]
    metric = 'mean'
    clip_low = 400
    clip_high = 999.99
    int_result = True

    result = tgts.scale_to_targets(
        df, target_col, target, metric, clip_low=clip_low, clip_high=clip_high,
        int_result=int_result)

    pdt.assert_index_equal(result.columns, df.columns)
    pdt.assert_series_equal(
        result[target_col],
        pd.Series([400, 400, 545, 727, 909, 1000, 1000, 1000, 1000, 1000]))
