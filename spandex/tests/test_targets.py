import pandas as pd
import pandas.util.testing as pdt
import pytest

from spandex import targets


@pytest.fixture
def col():
    return pd.Series([1, 2, 3, 4, 5])


@pytest.mark.parametrize('metric', ['mean', 'median'])
def test_scale_col_to_target_mean_median(col, metric):
    target = 600
    expected = pd.Series([200, 400, 600, 800, 1000])

    result = targets.scale_col_to_target(col, target, metric=metric)

    assert getattr(result, metric)() == target
    pdt.assert_series_equal(result, expected, check_dtype=False)


def test_scale_col_to_target_sum(col):
    target = 16
    expected = col * target / col.sum()

    result = targets.scale_col_to_target(col, target, metric='sum')

    assert result.sum() == target
    pdt.assert_series_equal(result, expected)


def test_scale_col_to_target_clip(col):
    target = 600
    clip_low = 450
    clip_high = 999
    expected = pd.Series([450, 450, 600, 800, 999])

    result = targets.scale_col_to_target(
        col, target, metric='mean', clip_low=clip_low, clip_high=clip_high)

    pdt.assert_series_equal(result, expected, check_dtype=False)


def test_scale_col_to_target_round(col):
    target = 16

    result = targets.scale_col_to_target(
        col, target, metric='sum', int_result=True)

    pdt.assert_series_equal(result, col)
