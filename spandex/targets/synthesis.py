import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _allocate_rows(rows_to_add, alloc_id, constraint, stuff=False):
    """
    Allocate rows from a table to containers respecting limits on how
    much those containers can hold.

    Parameters
    ----------
    rows_to_add : pandas.DataFrame
        Rows to update with new container IDs. Modified in place!
    alloc_id : str
        Name of column in `rows_to_add` that holds container IDs.
    constraint : pandas.Series
        The constraint property that limits where new rows can be placed.
        Index must correspond to values in `alloc_id` column of `df`.
    stuff : bool, optional
        Whether it's okay for allocation to go over constraints.
        If False rows are still added to meet targets, but some will
        not be placed.

    """
    rows_allocated = False
    rows = rows_to_add.iterrows()
    for cstr_id, cstr_val in constraint.iteritems():
        if rows_allocated:
            break

        while cstr_val >= 1:
            try:
                idx, row = next(rows)
            except StopIteration:
                rows_allocated = True
                break
            else:
                rows_to_add.set_value(idx, alloc_id, cstr_id)
                cstr_val -= 1

    if not rows_allocated:
        # still have unallocated rows, pick up where we left off
        cstr = constraint.iteritems()
        for idx, row in rows:
            if stuff:
                # spread the new rows out over the containers
                # as opposed to lumping them all in one container
                try:
                    cstr_id, _ = next(cstr)
                except StopIteration:
                    cstr = constraint.iteritems()
                    cstr_id, _ = next(cstr)

            else:
                cstr_id = None

            rows_to_add.set_value(idx, alloc_id, cstr_id)


def _remove_rows(df, num):
    """
    Remove rows at random from a DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
    num : int
        Number of rows to remove.

    Returns
    -------
    smaller : pandas.DataFrame

    """
    if num == 0:
        return df.copy()

    to_remove = np.random.choice(df.index.values, num)
    to_keep = df.index.difference(to_remove)

    return df.loc[to_keep]


def _add_rows(df, num, alloc_id, constraint, stuff=False):
    """
    Add rows to a table and allocate them to containers while respecting
    the constraint limits on the containers.

    Parameters
    ----------
    df : pandas.DataFrame
    num : int
        Number of rows to add.
    alloc_id : str
        Name of column in `df` that corresponds to where new rows are being
        allocated. Should correspond to the index of `constraint`.
    constraint : pandas.Series
        The constraint property that limits where new rows can be placed.
        Index must correspond to values in `alloc_id` column of `df`.
    stuff : bool, optional
        Whether it's okay for allocation to go over constraints.
        If False rows are still added to meet targets, but some will
        not be placed.

    Returns
    -------
    new_df : pandas.DataFrame

    """
    if num == 0:
        return df.copy()

    to_add = np.random.choice(df.index.values, num)
    rows_to_add = df.loc[to_add]

    # update the new rows' index
    max_idx = df.index.max()
    rows_to_add.index = range(max_idx + 1, max_idx + len(rows_to_add) + 1)

    # allocate rows to containers
    _allocate_rows(rows_to_add, alloc_id, constraint, stuff)

    return pd.concat([df, rows_to_add])


def _remove_rows_by_count(df, amount, count):
    """
    Remove rows from a table so that the sum of the values in column
    `count` is reduced by `amount`.

    Parameters
    ----------
    df : pandas.DataFrame
    amount : float
        Amount by which to decrease sum of `count` column.
    count : str
        Name of column to sum for accounting.

    Returns
    -------
    new_df : pandas.DataFrame

    """
    if amount == 0:
        return df.copy()

    sort_count = df[count].sort(ascending=False, inplace=False)
    sort_count = sort_count[(sort_count <= amount) & (sort_count != 0)]

    to_remove = []

    for k, v in sort_count.iteritems():
        if v <= amount:
            to_remove.append(k)
            amount -= v

        if amount == 0:
            break

    to_keep = df.index.difference(to_remove)

    return df.loc[to_keep]


def _add_rows_by_count(df, amount, count, alloc_id, constraint, stuff=False):
    """
    Add rows to a table so that the sum of values in the `count` column
    is increased by `amount`.

    Parameters
    ----------
    df : pandas.DataFrame
    amount : float
        Amount by which to increase sum of `count` column.
    count : str
        Name of the column in `df` to use for accounting.
    alloc_id : str
        Name of column in `df` that specifies container ID.
    constraint : pandas.Series
        The constraint property that limits where new rows can be placed.
        Index must correspond to values in `alloc_id` column of `df`.
    stuff : bool, optional
        Whether it's okay for allocation to go over constraints.
        If False rows are still added to meet targets, but some will
        not be placed.

    Returns
    -------
    new_df : pandas.DataFrame

    """
    if amount == 0:
        return df.copy()

    sort_count = df[count].sort(ascending=False, inplace=False)
    sort_count = sort_count[sort_count != 0]

    to_add = []

    while amount >= 1:
        sort_count = sort_count[sort_count <= amount]

        if len(sort_count) == 0:
            break

        for k, v in sort_count.iteritems():
            if v <= amount:
                to_add.append(k)
                amount -= v

            if amount == 0:
                break

    rows_to_add = df.loc[to_add]

    # update the new rows' index
    max_idx = df.index.max()
    rows_to_add.index = range(max_idx + 1, max_idx + len(rows_to_add) + 1)

    # allocate rows to containers
    _allocate_rows(rows_to_add, alloc_id, constraint, stuff)

    return pd.concat([df, rows_to_add])


def synthesize_rows(
        df, target, alloc_id, constraint, count=None, stuff=False):
    """
    Add or remove rows from a table to meet some target while
    respecting constraints.

    Parameters
    ----------
    df : pandas.DataFrame
        Table being modified.
    target : int
        Target number of things.
    alloc_id : str
        Name of column in `df` that corresponds to where new rows are being
        allocated. Should correspond to the index of `constraint`.
    constraint : pandas.Series
        The constraint property that limits where new rows can be placed.
        Index must correspond to values in `alloc_id` column of `df`.
    count : str, optional
        The name of a column in `df` to sum in order to compare to `target`.
        If None the number of rows will be counted.
    stuff : bool, optional
        Whether it's okay for allocation to go over constraints.
        If False rows are still added to meet targets, but some will
        not be placed.

    Returns
    -------
    new_df : pandas.DataFrame

    """
    if not count:
        current = len(df)
        if current < target:
            # add rows based on number of rows
            logger.debug('adding rows based on number of rows')
            return _add_rows(df, target - current, alloc_id, constraint, stuff)
        elif current > target:
            # remove rows based on number of rows
            logger.debug('removing rows based on number of rows')
            return _remove_rows(df, current - target)
        else:
            # nothing to do, target met!
            logger.debug('target number of rows is met')
            return df.copy()
    elif count:
        current = df[count].sum()
        if current < target:
            # add rows based on total of count column
            logger.debug('adding rows based on total of count column')
            return _add_rows_by_count(
                df, target - current, count, alloc_id, constraint, stuff)
        elif current > target:
            # remove rows based on total of count column
            logger.debug('removing rows based on total of count column')
            return _remove_rows_by_count(df, current - target, count)
        else:
            # target met, nothing to do!
            logger.debug('target total is met')
            return df.copy()
