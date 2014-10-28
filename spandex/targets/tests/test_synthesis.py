import numpy as np
import pandas as pd
import pandas.util.testing as pdt
import pytest

from spandex.targets import synthesis as syn


@pytest.fixture
def seed(request):
    current = np.random.get_state()

    def fin():
        np.random.set_state(current)
    request.addfinalizer(fin)

    np.random.seed(0)


@pytest.fixture(scope='module')
def alloc_id():
    return 'thing_id'


@pytest.fixture(scope='module')
def count():
    return 'number'


@pytest.fixture(scope='module')
def df(alloc_id, count):
    return pd.DataFrame(
        {alloc_id: ['a', 'b', 'c', 'b', 'c'],
         count: [1, 2, 3, 4, 5]})


@pytest.fixture(scope='module')
def constraint():
    return pd.Series([0, 1, 3], index=['a', 'b', 'c'])


@pytest.fixture(scope='module')
def constraint_units():
    return 'units'


@pytest.fixture(scope='module')
def constraint_scaled():
    return 'scaled'


@pytest.fixture(scope='module')
def constraint_scale_factor():
    return 400


@pytest.fixture(scope='module')
def constraint_df(
        df, alloc_id, constraint, constraint_units, constraint_scaled,
        constraint_scale_factor):
    current = df[alloc_id].value_counts()
    return pd.DataFrame(
        {constraint_units: constraint + current,
         constraint_scaled: (constraint + current) * constraint_scale_factor})


@pytest.fixture(scope='module')
def constraint_expr(constraint_scaled, constraint_scale_factor):
    return '{} / {}'.format(constraint_scaled, constraint_scale_factor)


def test_allocate_rows(df, alloc_id, constraint, count):
    rows = df.loc[[0, 1]]

    syn._allocate_rows(rows, alloc_id, constraint)

    pdt.assert_frame_equal(
        rows,
        pd.DataFrame(
            {alloc_id: ['b', 'c'],
             count: [1, 2]}))


def test_allocate_rows_stuff(df, alloc_id, constraint, count):
    rows = df.copy(deep=True)
    stuff = True

    syn._allocate_rows(rows, alloc_id, constraint, stuff=stuff)

    pdt.assert_frame_equal(
        rows,
        pd.DataFrame(
            {alloc_id: ['b', 'c', 'c', 'c', 'a'],
             count: df[count]}))


def test_allocate_rows_no_stuff(df, alloc_id, constraint, count):
    rows = df.copy(deep=True)
    stuff = False

    syn._allocate_rows(rows, alloc_id, constraint, stuff=stuff)

    pdt.assert_frame_equal(
        rows,
        pd.DataFrame(
            {alloc_id: ['b', 'c', 'c', 'c', None],
             count: df[count]}))


def test_remove_rows(seed, df, alloc_id, count):
    num = 2

    result = syn._remove_rows(df, num)

    assert len(result) == len(df) - num
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['b', 'c', 'b'],
             count: [2, 3, 4]},
            index=[1, 2, 3]))


def test_remove_rows_noop(seed, df):
    num = 0

    result = syn._remove_rows(df, num)

    assert len(result) == len(df) - num
    pdt.assert_frame_equal(result, df)


def test_add_rows_noop(seed, df):
    num = 0

    result = syn._remove_rows(df, num)

    assert len(result) == len(df) + num
    pdt.assert_frame_equal(result, df)


def test_add_rows(seed, df, alloc_id, count, constraint):
    num = 3

    result = syn._add_rows(df, num, alloc_id, constraint)

    assert len(result) == len(df) + num
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c'],
             count: [1, 2, 3, 4, 5, 5, 1, 4]}))


def test_add_rows_stuff(seed, df, alloc_id, count, constraint):
    num = 5
    stuff = True

    result = syn._add_rows(df, num, alloc_id, constraint, stuff=stuff)

    assert len(result) == len(df) + num
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', 'a'],
             count: [1, 2, 3, 4, 5, 5, 1, 4, 4, 4]}))


def test_add_rows_no_stuff(seed, df, alloc_id, count, constraint):
    num = 5
    stuff = False

    result = syn._add_rows(df, num, alloc_id, constraint, stuff=stuff)

    assert len(result) == len(df) + num
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', None],
             count: [1, 2, 3, 4, 5, 5, 1, 4, 4, 4]}))


def test_remove_rows_by_count_noop(df, count):
    amount = 0

    result = syn._remove_rows_by_count(df, amount, count)

    assert result[count].sum() == df[count].sum()
    pdt.assert_frame_equal(result, df)


def test_remove_rows_by_count(df, count, alloc_id):
    amount = 10

    result = syn._remove_rows_by_count(df, amount, count)

    assert result[count].sum() == df[count].sum() - amount
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['b', 'c'],
             count: [2, 3]},
            index=[1, 2]))


def test_add_rows_by_count_noop(df, count, alloc_id, constraint):
    amount = 0

    result = syn._add_rows_by_count(df, amount, count, alloc_id, constraint)

    assert result[count].sum() == df[count].sum()
    pdt.assert_frame_equal(result, df)


def test_add_rows_by_count(df, count, alloc_id, constraint):
    amount = 8

    result = syn._add_rows_by_count(df, amount, count, alloc_id, constraint)

    assert result[count].sum() == df[count].sum() + amount
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c'],
             count: [1, 2, 3, 4, 5, 5, 3]}))


def test_add_rows_by_count_stuff(df, count, alloc_id, constraint):
    amount = 18
    stuff = True

    result = syn._add_rows_by_count(
        df, amount, count, alloc_id, constraint, stuff=stuff)

    assert result[count].sum() == df[count].sum() + amount
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', 'a', 'b'],
             count: [1, 2, 3, 4, 5, 5, 4, 3, 2, 1, 3]}))


def test_add_rows_by_count_no_stuff(df, count, alloc_id, constraint):
    amount = 18
    stuff = False

    result = syn._add_rows_by_count(
        df, amount, count, alloc_id, constraint, stuff=stuff)

    assert result[count].sum() == df[count].sum() + amount
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: [
                'a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', None, None],
             count: [1, 2, 3, 4, 5, 5, 4, 3, 2, 1, 3]}))


def test_add_or_remove_rows_noop(seed, df, alloc_id, count, constraint):
    target = len(df)

    result = syn._add_or_remove_rows(df, target, alloc_id, constraint)

    assert len(result) == target
    pdt.assert_frame_equal(result, df)


def test_add_or_remove_rows_add(seed, df, alloc_id, count, constraint):
    target = 8

    result = syn._add_or_remove_rows(df, target, alloc_id, constraint)

    assert len(result) == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c'],
             count: [1, 2, 3, 4, 5, 5, 1, 4]}))


def test_add_or_remove_rows_add_stuff(seed, df, alloc_id, count, constraint):
    target = 10
    stuff = True

    result = syn._add_or_remove_rows(
        df, target, alloc_id, constraint, stuff=stuff)

    assert len(result) == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', 'a'],
             count: [1, 2, 3, 4, 5, 5, 1, 4, 4, 4]}))


def test_add_or_remove_rows_add_no_stuff(
        seed, df, alloc_id, count, constraint):
    target = 10
    stuff = False

    result = syn._add_or_remove_rows(
        df, target, alloc_id, constraint, stuff=stuff)

    assert len(result) == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', None],
             count: [1, 2, 3, 4, 5, 5, 1, 4, 4, 4]}))


def test_add_or_remove_rows_remove(seed, df, alloc_id, count, constraint):
    target = 3

    result = syn._add_or_remove_rows(df, target, alloc_id, constraint)

    assert len(result) == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['b', 'c', 'b'],
             count: [2, 3, 4]},
            index=[1, 2, 3]))


def test_add_or_remove_rows_count_noop(seed, df, alloc_id, count, constraint):
    target = df[count].sum()

    result = syn._add_or_remove_rows(
        df, target, alloc_id, constraint, count=count)

    assert result[count].sum() == target
    pdt.assert_frame_equal(result, df)


def test_add_or_remove_rows_count_add(seed, df, alloc_id, count, constraint):
    target = 18

    result = syn._add_or_remove_rows(
        df, target, alloc_id, constraint, count=count)

    assert result[count].sum() == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b'],
             count: [1, 2, 3, 4, 5, 3]}))


def test_add_or_remove_rows_count_add_stuff(
        seed, df, alloc_id, count, constraint):
    target = 30
    stuff = True

    result = syn._add_or_remove_rows(
        df, target, alloc_id, constraint, count=count, stuff=stuff)

    assert result[count].sum() == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', 'a'],
             count: [1, 2, 3, 4, 5, 5, 4, 3, 2, 1]}))


def test_add_or_remove_rows_count_add_no_stuff(
        seed, df, alloc_id, count, constraint):
    target = 30
    stuff = False

    result = syn._add_or_remove_rows(
        df, target, alloc_id, constraint, count=count, stuff=stuff)

    assert result[count].sum() == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', None],
             count: [1, 2, 3, 4, 5, 5, 4, 3, 2, 1]}))


def test_add_or_remove_rows_count_remove(
        seed, df, alloc_id, count, constraint):
    target = 10

    result = syn._add_or_remove_rows(
        df, target, alloc_id, constraint, count=count)

    assert result[count].sum() == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b'],
             count: [1, 2, 3, 4]}))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_noop(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 5

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            constraint_expr=constraint_expr)

    assert len(result) == target
    pdt.assert_frame_equal(result, df)


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_add(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 8

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            constraint_expr=constraint_expr)

    assert len(result) == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c'],
             count: [1, 2, 3, 4, 5, 5, 1, 4]}))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_add_filters(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 3
    filters = '{} < 2'.format(count)

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units,
            filters=filters)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            filters=filters, constraint_expr=constraint_expr)

    assert len(result.query(filters)) == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c'],
             count: [1, 2, 3, 4, 5, 1, 1]}))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_add_stuff(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 10
    stuff = True

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units, stuff=stuff)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            constraint_expr=constraint_expr, stuff=stuff)

    assert len(result) == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', 'a'],
             count: [1, 2, 3, 4, 5, 5, 1, 4, 4, 4]}))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_add_no_stuff(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 10
    stuff = False

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units, stuff=stuff)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            constraint_expr=constraint_expr, stuff=stuff)

    assert len(result) == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', None],
             count: [1, 2, 3, 4, 5, 5, 1, 4, 4, 4]}))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_remove(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 3

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            constraint_expr=constraint_expr)

    assert len(result) == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['b', 'c', 'b'],
             count: [2, 3, 4]},
            index=[1, 2, 3]))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_remove_filters(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 1
    filters = '{} > 3'.format(count)

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units,
            filters=filters)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            filters=filters, constraint_expr=constraint_expr)

    assert len(result.query(filters)) == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'c'],
             count: [1, 2, 3, 5]},
            index=[0, 1, 2, 4]))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_count_noop(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = df[count].sum()

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units, count=count)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            constraint_expr=constraint_expr, count=count)

    assert result[count].sum() == target
    pdt.assert_frame_equal(result, df)


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_count_add(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 18

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units, count=count)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            constraint_expr=constraint_expr, count=count)

    assert result[count].sum() == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b'],
             count: [1, 2, 3, 4, 5, 3]}))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_count_add_filters(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 18
    filters = '{} > 3'.format(count)

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units,
            filters=filters, count=count)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            filters=filters, constraint_expr=constraint_expr, count=count)

    assert result.query(filters)[count].sum() == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c'],
             count: [1, 2, 3, 4, 5, 5, 4]}))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_count_add_stuff(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 30
    stuff = True

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units,
            count=count, stuff=stuff)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            constraint_expr=constraint_expr, count=count, stuff=stuff)

    assert result[count].sum() == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', 'a'],
             count: [1, 2, 3, 4, 5, 5, 4, 3, 2, 1]}))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_count_add_no_stuff(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 30
    stuff = False

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units,
            count=count, stuff=stuff)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            constraint_expr=constraint_expr, count=count, stuff=stuff)

    assert result[count].sum() == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'c', 'c', None],
             count: [1, 2, 3, 4, 5, 5, 4, 3, 2, 1]}))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_count_remove(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 10

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units,
            count=count)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            constraint_expr=constraint_expr, count=count)

    assert result[count].sum() == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['a', 'b', 'c', 'b'],
             count: [1, 2, 3, 4]}))


@pytest.mark.parametrize('scaled_test', [False, True])
def test_synthesize_one_count_remove_filters(
        seed, df, alloc_id, count, constraint_df, constraint_units,
        constraint_scaled, constraint_expr, scaled_test):
    target = 2
    filters = '{} < 3'.format(count)

    if not scaled_test:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_units,
            filters=filters, count=count)
    else:
        result = syn.synthesize_one(
            df, target, alloc_id, constraint_df, constraint_scaled,
            filters=filters, constraint_expr=constraint_expr, count=count)

    assert result.query(filters)[count].sum() == target
    pdt.assert_frame_equal(
        result,
        pd.DataFrame(
            {alloc_id: ['b', 'c', 'b', 'c'],
             count: [2, 3, 4, 5]},
            index=[1, 2, 3, 4]))
