import pandas as pd
import pandas.util.testing as pdt

from spandex import targets


def test_scale_col_to_target_mean():
    col = pd.Series([1, 2, 3, 4, 5])
    result = targets.scale_col_to_target_mean(col, 600)
    assert result.mean() == 600
    pdt.assert_series_equal(
        result, pd.Series([200, 400, 600, 800, 1000]), check_dtype=False)
