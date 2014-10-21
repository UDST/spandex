"""
Functionality for scaling agent attributes or the number of agents to
match targets.

"""
from __future__ import division, print_function

import pandas as pd


def apply_filter_query(df, filters=None):
    """
    Use the DataFrame.query method to filter a table down to the
    desired rows.

    Parameters
    ----------
    df : pandas.DataFrame
    filters : list of str or str, optional
        List of filters to apply. Will be joined together with
        ' and ' and passed to DataFrame.query. A string will be passed
        straight to DataFrame.query.
        If not supplied no filtering will be done.

    Returns
    -------
    filtered_df : pandas.DataFrame

    """
    if filters:
        if isinstance(filters, str):
            query = filters
        else:
            query = ' and '.join(filters)
        return df.query(query)
    else:
        return df


def update_series(s1, s2):
    """
    Update values in one Series from another Series without modifying either.

    Parameters
    ----------
    s1, s2 : pandas.Series

    Returns
    -------
    pandas.Series

    """
    s = s1.astype(s2.dtype)

    for k, v in s2.iteritems():
        s[k] = v

    return s


def _scale_col_to_target(col, target, metric_func):
    """
    Scale a column's values so that in aggregate they match some metric,
    for example, mean, median, or sum.

    Parameters
    ----------
    col : pandas.Series
    target : number
    metric_func : callable
        Must accept a Series and return a number.

    Returns
    -------
    scaled : pandas.Series

    """
    current = metric_func(col)
    multiplier = target / current
    return col * multiplier


def scale_col_to_target(
        col, target, metric='mean', clip_low=None, clip_high=None,
        int_result=False):
    """
    Scale a column's values so they match a target aggregate metric.

    Parameters
    ----------
    col : pandas.Series
    target : number
    metric : {'mean', 'median', 'sum'}
        How to aggregate the values in `col` for comparison to `target`.
    clip_low : number, optional
    clip_high : number, optional
    int_result : bool, optional
        If True, results will be rounded and converted to integers.

    """
    if metric == 'mean':
        metric_func = pd.Series.mean
    elif metric == 'sum':
        metric_func = pd.Series.sum
    elif metric == 'median':
        metric_func = pd.Series.median
    else:
        raise ValueError('Unknown metric type: {!r}'.format(metric))

    scaled = _scale_col_to_target(col, target, metric_func)
    scaled = scaled.clip(clip_low, clip_high)

    if int_result:
        scaled = scaled.round().astype('int')

    return scaled


def scale_to_targets(
        df, target_col, targets, metric='mean', filters=None, clip_low=None,
        clip_high=None, int_result=None):
    """
    Parameters
    ----------
    df : pandas.DataFrame
    target_col : str
        Column in `df` that will be scaled.
    targets : sequence
        Sequence of target values. Each target will correspond to a
        different segment identified by `filters`.
    metric : {'mean', 'median', 'sum'}
        How to aggregate the values for comparison to targets.
    filters : sequence, optional
        Filters will be used with DataFrame.query and `df` to make a subset of
        the full table for each scaling operation. Should be the same
        length as `targets`.
        Each individual filter can be a string or a sequence of strings.
        Use ``None`` for no filtering.
    clip_low : number, optional
    clip_high : number, optional
        Bounds for truncating results.
    int_result : bool, optional
        Whether result should be rounded and converted to integers.

    Returns
    -------
    pandas.DataFrame
        New DataFrame with `target_col` updated.

    """
    filters = filters or [None] * len(targets)

    scaled = []

    for t, f, in zip(targets, filters):
        series = apply_filter_query(df, f)[target_col]
        scaled.append(scale_col_to_target(series, t, metric))

    scaled = update_series(df[target_col], pd.concat(scaled))
    scaled = scaled.clip(clip_low, clip_high)

    if int_result:
        scaled = scaled.round().astype('int')

    df = df.copy()
    df[target_col] = scaled

    return df
