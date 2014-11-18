import numpy as np
import pandas as pd
from pandas.util import testing as pdt
import pytest

from spandex import TableFrame
from spandex.io import db_to_df, df_to_db


def test_tableframe(loader):
    table = loader.tables.sample.hf_bg
    for cache in [False, True]:
        tf = TableFrame(table, index_col='gid', cache=cache)
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


def test_sim_export(loader):
    # Try importing the UrbanSim simulation framework, otherwise skip test.
    sim = pytest.importorskip('urbansim.sim.simulation')
    from spandex.sim import column

    # Register input parcels table.
    parcels = loader.tables.sample.heather_farms
    parcels_in = TableFrame(parcels, index_col='gid')
    sim.add_table('parcels_in', parcels_in, copy=False)

    # Register output parcels table.
    @sim.table()
    def parcels_out(parcels_in):
        return pd.DataFrame(index=parcels_in.parcel_id)

    # Specify default table for output columns as decorator.
    def out(*args, **kwargs):
        return column(table_name='parcels_out', groupby=parcels_in.parcel_id,
                      *args, **kwargs)

    # Specify some output columns.
    @out(astype=str, agg='first')
    def apn(apn='parcels_in.puid'):
        return apn

    @out()
    def county_id():
        return 13

    @out(astype=float, agg='median')
    def area(acr='parcels_in.parcel_acr'):
        return 4047. * acr

    # Register model to export output table to database.
    @sim.model()
    def export(parcels_out):
        schema = loader.tables.sample
        df_to_db(parcels_out.to_frame(), 'parcels_out', schema=schema)

    # Inspect output table.
    column_names = ['apn', 'county_id', 'area']
    parcels_out_df1 = sim.get_table('parcels_out').to_frame()
    assert set(parcels_out_df1.columns) == set(column_names)
    assert parcels_out_df1.county_id.unique() == [13]

    # Export table to database and import back to compare.
    sim.run(['export'])
    parcels_out_table = loader.tables.sample.parcels_out
    parcels_out_df2 = db_to_df(parcels_out_table, index_col='parcel_id')
    pdt.assert_frame_equal(parcels_out_df1[column_names],
                           parcels_out_df2[column_names])
