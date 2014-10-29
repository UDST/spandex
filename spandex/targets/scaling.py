"""
Functionality for scaling agent attributes or the number of agents to
match targets.

"""
from __future__ import division, print_function

from collections import namedtuple
from numbers import Number

import numpy as np
import pandas as pd

from .targets import apply_filter_query


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


TargetsRow = namedtuple(
    'TargetsRow',
    ['column', 'target', 'metric', 'filters', 'clip_low', 'clip_high',
     'int_result'])


def _targets_row_to_params(row):
    """
    Convert a row of a targets table to parameters for `scale_col_to_target`.
    Takes care of NaN values appropriately.

    Return value is a namedtuple with attribute names as listed below.

    Parameters
    ----------
    row : pandas.Series

    Returns
    -------
    column : str
    target : number
    metric : str
    filters : list or None
    clip_low : number or None
    clip_high : number or None
    int_result : bool

    """
    column = row.column_name
    target = row.target_value
    metric = row.target_metric

    is_a_thing = lambda x: (
        False if isinstance(x, Number) and np.isnan(x) else bool(x))

    filters = row.filters.split(',') if is_a_thing(row.filters) else None
    clip_low = row.clip_low if is_a_thing(row.clip_low) else None
    clip_high = row.clip_high if is_a_thing(row.clip_high) else None
    int_result = row.int_result if is_a_thing(row.int_result) else False

    return TargetsRow(
        column, target, metric, filters, clip_low, clip_high, int_result)


def scale_to_targets_from_table(df, targets):
    """
    Scale values in a DataFrame based on specifications in a targets table.

    The table is expected to have this format (values are examples)::

        column_name target_value target_metric filters
        'income'    100000       'mean'        'tract_id == 7,num_workers > 2'

        \ clip_low clip_high int_result
          0        1000000   False

    The names in ``column_name`` and ``filters`` are expected to be
    columns in `df`.
    ``target_metric`` may be one of 'mean', 'median', or 'sum'.
    The ``filters``, ``clip_low``, ``clip_high``, and ``int_result``
    columns may be left blank to accept defaults.

    Parameters
    ----------
    df : pandas.DataFrame
        Table with columns to be scaled.
    targets : pandas.DataFrame
        Table of targets and other scaling parameters.

    Returns
    -------
    scaled : pandas.DataFrame

    """
    # make sure we're not modifying any input data
    df = df.copy()
    for col in targets.column_name.unique():
        df[col] = df[col].copy()

    for row in (_targets_row_to_params(r) for ix, r in targets.iterrows()):
        series = apply_filter_query(df, row.filters)[row.column]
        scaled = scale_col_to_target(
            series, row.target, row.metric, row.clip_low, row.clip_high,
            row.int_result)
        df[row.column].loc[scaled.index] = scaled

    return df


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

    scaled = pd.concat(scaled)
    scaled = scaled.clip(clip_low, clip_high)

    if int_result:
        scaled = scaled.round().astype('int')

    df = df.copy()
    df[target_col].loc[scaled.index] = scaled

    return df
