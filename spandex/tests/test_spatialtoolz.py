import sys
print sys.path
from spandex import spatialtoolz


def test_dummy():
    loader = spatialtoolz.DataLoader()
    water = loader.database.tables.public.hf_water
    df = spatialtoolz.db_to_df(water)
    assert len(df) == 1
