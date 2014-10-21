import numpy as np
from sqlalchemy import func

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
        cur.execute("""
            SELECT DISTINCT srid from geometry_columns
            WHERE f_table_schema = 'sample';
        """)
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
    assert np.issubdtype(parcels_df.bg_id.dtype, int)

    # Assert that there are at least 10 unique parcel block groups.
    parcels_bg_ids = parcels_df.bg_id.unique()
    assert len(parcels_bg_ids) >= 10

    # Assert that parcel block groups are a subset of all block groups.
    assert np.all([bg_id in bg_df.index for bg_id in parcels_bg_ids])


def test_proportion_overlap(loader):
    # Calculate proportion of each parcel overlapped by water.
    parcels = loader.database.tables.sample.heather_farms
    water = loader.database.tables.sample.hf_water
    assert not hasattr(parcels, 'proportion_water')
    spatialtoolz.proportion_overlap(parcels, water, 'proportion_water')
    assert hasattr(parcels, 'proportion_water')

    # Build DataFrame from columns of parcels table.
    columns = [parcels.parcel_id, parcels.geom.ST_Area(),
               parcels.proportion_water]
    parcels_df = spatialtoolz.db_to_df(columns, index_name='parcel_id')

    # Assert that proportion overlap values are between 0 and 1.
    assert parcels_df.proportion_water.dtype == float
    assert not (parcels_df.proportion_water < 0).any()
    assert not (parcels_df.proportion_water > 1).any()

    # Assert that sum of overlapped parcel area is <= total water area.
    with loader.database.session() as sess:
        overlapped_area = sess.query(
            func.sum(parcels.proportion_water * parcels.geom.ST_Area())
        ).scalar()
        water_area = sess.query(func.sum(water.geom.ST_Area())).scalar()
    assert overlapped_area <= water_area


def test_trim(loader):
    def calc_overlap(geom_a, geom_b):
        """Calculate area of overlap between two geometry columns."""
        with loader.database.session() as sess:
            q = sess.query(
                func.sum(
                    func.ST_Intersection(geom_a, geom_b).ST_Area()
                )
            ).filter(
                func.ST_Intersects(geom_a, geom_b)
            )
        return q.scalar()

    parcels = loader.database.tables.sample.heather_farms
    water = loader.database.tables.sample.hf_water

    # Assert that some parcel areas overlap water.
    assert calc_overlap(parcels.geom, water.geom) > 0

    # Calculate total areas and number of parcels for comparison after trim.
    with loader.database.session() as sess:
        area_parcel_0 = sess.query(func.sum(parcels.geom.ST_Area())).scalar()
        area_water_0 = sess.query(func.sum(water.geom.ST_Area())).scalar()
        num_parcel_0 = sess.query(parcels.geom).filter(
            parcels.geom.isnot(None)
        ).count()

    # Trim away parcel areas that are water.
    spatialtoolz.trim(parcels.geom, water.geom)

    # Assert that all parcel geometries are still valid.
    invalid = spatialtoolz.geom_invalid(parcels)
    assert invalid.empty

    # Assert that all parcel geometries have area, i.e. no deleted geometries.
    with loader.database.session() as sess:
        q = sess.query(parcels).filter(parcels.geom.ST_Area() < 0.1)
        assert q.count() == 0

    # Recalculate total areas and number of parcels.
    with loader.database.session() as sess:
        area_parcel = sess.query(func.sum(parcels.geom.ST_Area())).scalar()
        area_water = sess.query(func.sum(water.geom.ST_Area())).scalar()
        num_parcel = sess.query(parcels.geom).filter(
            parcels.geom.isnot(None)
        ).count()

    # Assert that water area and number of parcels has not changed.
    assert area_water == area_water_0
    assert num_parcel == num_parcel_0

    # Assert that new parcel area is at least 50% of old parcel area.
    area_parcel > 0.5 * area_parcel_0
    area_parcel < area_parcel_0

    # Assert that there is no parcel area overlapping water.
    assert calc_overlap(parcels.geom, water.geom) < 0.1


def test_invalid(loader):
    # There are currently no invalid geometries in the sample data, so this
    # is not a great test.
    parcels = loader.database.tables.sample.heather_farms
    invalid = spatialtoolz.geom_invalid(parcels, parcels.parcel_id)
    assert invalid.empty
    invalid = spatialtoolz.geom_invalid(parcels)
    assert invalid.empty


def test_reproject(loader):
    # Assert that all SRIDs are consistent and match defined project SRID.
    srids = get_srids(loader)
    tables = get_tables(loader)
    assert len(srids) == 1
    assert spatialtoolz.srid_equality(tables)
    assert loader.srid
    assert np.issubdtype(type(loader.srid), int)
    assert srids == [loader.srid]
