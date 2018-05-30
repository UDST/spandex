import logging

import numpy as np
import pandas as pd

from .targets import apply_filter_query

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
                rows_to_add.at[idx, alloc_id] = cstr_id
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

            rows_to_add.at[idx, alloc_id] = cstr_id


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

    sort_count = df[count].sort_values(ascending=False, inplace=False)
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

    sort_count = df[count].sort_values(ascending=False, inplace=False)
    sort_count = sort_count[sort_count != 0]
    orig_sort_count = sort_count.copy()

    to_add = []

    while amount >= 1:
        sort_count = sort_count[sort_count <= amount]

        if len(sort_count) == 0:
            # see if we can pop the most recent thing off to_add
            # and try again with a smaller number.
            k = to_add.pop()
            v = orig_sort_count[k]
            amount += v
            sort_count = orig_sort_count[
                (orig_sort_count < v) & (orig_sort_count <= amount)]

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


def _add_or_remove_rows(
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


def synthesize_one(
        df, target, alloc_id, geo_df, geo_col=None, constraint_expr=None,
        filters=None, count=None, stuff=False):
    """
    Add or remove rows to/from a table to meet some target.
    The target can be either a number of rows or the sum of a numeric column.

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
    geo_df : pandas.DataFrame
        Table of containers to which new `df` rows will be allocated.
        Containers are expected to be ID'd by their index.
    geo_col : str, optional
        Name of column in `geo_df` that has constraint values.
        This is optional if `constraint_expr` is provided.
    constraint_expr : str, optional
        An optional constraint expression for translating the values
        in `geo_col` into units. E.g. 'parcel_sqft / 500'.
        This overrides `geo_col`.
    filters : str or sequence, optional
        Expressions for filtering `df` to the rows that will be subject
        to removal or copying.
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
    # calculate constraints by comparing geo constraint values and the
    # current occupancy levels.
    # start with current occupancy
    occupancy = df[alloc_id].value_counts()

    # get the total container limits
    if constraint_expr:
        container_size = geo_df.eval(constraint_expr)
    else:
        container_size = geo_df[geo_col]

    if not occupancy.index.isin(container_size.index).all():
        raise RuntimeError('Rows are assigned to non-existant containers')

    constraints = container_size.sub(occupancy, fill_value=0)

    # separate rows for which this target applies and those for which
    # it doesn't
    filters = filters.split(',') if isinstance(filters, str) else filters
    synth_df = apply_filter_query(df, filters)

    if len(synth_df) == 0 and target != 0:
        raise RuntimeError('No rows to synthesize or remove')

    new_synth_df = _add_or_remove_rows(
        synth_df, target, alloc_id, constraints, count=count, stuff=stuff)

    if len(new_synth_df) > len(synth_df):
        # rows were added and we'll need to do some index resolution
        new_indexes = new_synth_df.index.difference(synth_df.index)
        new_rows = new_synth_df.loc[new_indexes]

        current_idx_max = df.index.max()
        new_rows.index = range(
            current_idx_max + 1, current_idx_max + len(new_rows) + 1)

        return pd.concat([df, new_rows])

    elif len(new_synth_df) < len(synth_df):
        removed = synth_df.index.difference(new_synth_df.index)
        return df.loc[df.index.difference(removed)]

    else:
        return df.copy()


def synthesize_from_table(df, geo_df, targets):
    """
    Add and remove rows from a table based on targets in another table.

    The table is expected to have this format (values are examples)::

        target_value geo_id_col  filters        count     \
        500          'parcel_id'
        10000        'zone_id'   'zone_id == 1' 'persons'

        capacity_col        capacity_expr                stuff
        'residential_units'
                            'non_residential_sqft / 250' True

    Values left blank are optional. The ``geo_id_col``, ``filters``,
    and ``count`` options all apply to the table of agents being modified.
    ``geo_id_col`` contains the identifiers of the geographic containers
    to which new rows allocated. This needs to correspond to the
    index of `geo_df`. ``filters`` specify a subset of `df` to which
    the row's target and other parameters apply. ``count`` speciies a
    column in ``df`` that is counted for comparison to the target.
    If no ``count`` is provided, rows are counted.

    ``capacity_col`` and ``capacity_expr`` refer to columns in `geo_df`.
    They specify the capacity of the geographic containers in `geo_df`.
    The values in ``capacity_col`` will be used unmodified as that
    container's capacity. ``capacity_expr`` will be evaluated using
    Pandas' ``eval`` function to make a Series that will be used
    as the capacity. ``capacity_col`` and ``capacity_expr``
    are mutually exclusive and if both are given ``capacity_expr``
    will be used.

    If more rows are synthesized that can be allocated all the rows will
    always be included in the result. The ``stuff`` parameter indicates
    whether geographic containers should be stuff beyond their capacity.
    If not, rows are left unassigned to any container.

    Parameters
    ----------
    df : pandas.DataFrame
        Table of agents that will have rows added or removed.
    geo_df : pandas.DataFrame
        Table of geographic containers to which new rows in `df` will
        be allocated.
    target : pandas.DataFrame
        Table of synthesis parameters.

    Returns
    -------
    new_df : pandas.DataFrame

    """
    # replace NaNs with None
    targets = targets.where(targets.notnull(), None)

    new_df = df

    for _, row in targets.iterrows():
        new_df = synthesize_one(
            df=new_df,
            target=row['target_value'],
            alloc_id=row['geo_id_col'],
            geo_df=geo_df,
            geo_col=row['capacity_col'],
            constraint_expr=row['capacity_expr'],
            filters=row['filters'],
            count=row['count'],
            stuff=row['stuff'])

    return new_df
