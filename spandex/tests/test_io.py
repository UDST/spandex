import numpy as np
import pandas as pd

from spandex import TableFrame


def test_tableframe(loader):
    table = loader.database.tables.sample.hf_bg
    for cache in [False, True]:
        tf = TableFrame(table, index_name='gid', cache=cache)
        assert isinstance(tf.index, pd.Index)
        num_rows = len(tf)
        assert num_rows > 1
        assert set(tf.columns) == set(table.__table__.columns.keys())
        for column_name in tf.columns:
            if column_name != 'gid':
                if cache:
                    assert column_name not in tf._cached.keys()
                assert isinstance(tf[column_name], pd.Series)
                if cache:
                    assert column_name in tf._cached.keys()
                assert isinstance(getattr(tf, column_name), pd.Series)
        df = tf[['objectid']]
        assert isinstance(df, pd.DataFrame)
        assert len(df) == num_rows
        assert set(df.columns) == set(['objectid'])
        assert np.issubdtype(df.objectid.dtype, int)
