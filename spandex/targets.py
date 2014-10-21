"""
Functionality for scaling agent attributes or the number of agents to
match targets.

"""
from __future__ import division, print_function

import pandas as pd


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
