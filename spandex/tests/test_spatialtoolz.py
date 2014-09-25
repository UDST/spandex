import numpy as np

from spandex import spatialtoolz

# Sanity checks for spatialtoolz. These tests don't ensure correct results,
# but rather check that results "make sense" and are within bounds.
# In the future, we may want to test for prior known values in the results.


def get_srids(loader):
    """Build list of unique geometry column SRIDs."""
    # This deliberately uses raw SQL instead of ORM to provide redundancy
    # against spatialtoolz.conform_srids, which iterates over the ORM.
    srids = []
    with loader.database.cursor() as cur:
        cur.execute("SELECT DISTINCT srid from geometry_columns;")
        for (srid,) in cur:
            srids.append(srid)
    assert len(srids) > 0
    return srids


def get_tables(loader):
    """Build list of table ORM classes."""
    tables = []
    for (key, value) in loader.database.tables.sample.__dict__.items():
        if not key.startswith('_'):
            tables.append(value)
    assert len(tables) > 1
    return tables


def test_tag(loader):
    # Tag parcels with block group ID.
    parcels = loader.database.tables.sample.heather_farms
    bg = loader.database.tables.sample.hf_bg
    assert not hasattr(parcels, 'bg_id')
    spatialtoolz.tag(parcels, 'bg_id', bg, 'objectid')
    assert hasattr(parcels, 'bg_id')

    # Build DataFrame from parcels and block groups tables.
    parcels_df = spatialtoolz.db_to_df(parcels, index_name='parcel_id')
    bg_df = spatialtoolz.db_to_df(bg, index_name='objectid')

    # Assert that all parcels have integer block groups.
    assert not parcels_df.bg_id.isnull().any()
    assert parcels_df.bg_id.dtype == int

    # Assert that there are at least 10 unique parcel block groups.
    parcels_bg_ids = parcels_df.bg_id.unique()
    assert len(parcels_bg_ids) >= 10

    # Assert that parcel block groups are a subset of all block groups.
    assert np.all([bg_id in bg_df.index for bg_id in parcels_bg_ids])


def test_reproject(loader):

    # Assert that SRIDs need to be reprojected.
    srids = get_srids(loader)
    assert srids != [loader.srid]

    # Reproject all non-conforming SRIDs into project SRID.
    spatialtoolz.conform_srids(schema='sample')

    # Assert that all SRIDs are consistent and match defined project SRID.
    srids = get_srids(loader)
    tables = get_tables(loader)
    assert len(srids) == 1
    assert spatialtoolz.srid_equality(tables)
    assert loader.srid and type(loader.srid) == int
    assert srids == [loader.srid]
