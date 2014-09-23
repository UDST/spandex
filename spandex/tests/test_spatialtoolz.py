from spandex import spatialtoolz


# Sanity checks for spatialtoolz. These tests don't ensure correct results,
# but rather check that results "make sense" and are within bounds.
# In the future, we may want to test for prior known values in the results.


def test_tag(loader):
    # Tag parcels with block group ID.
    parcels = loader.database.tables.sample.heather_farms
    block_groups = loader.database.tables.sample.hf_bg
    assert hasattr(block_groups, 'objectid')
    assert not hasattr(parcels, 'bg_id')
    spatialtoolz.tag(parcels, 'bg_id', block_groups, 'objectid')
    assert hasattr(parcels, 'bg_id')

    # Ensure that parcel block groups are a subset of all block groups.
    with loader.database.session() as sess:
        parcel_values = set(sess.query(parcels.bg_id.distinct()))
        bg_values = set(sess.query(block_groups.objectid.distinct()))
    assert parcel_values.issubset(bg_values)
