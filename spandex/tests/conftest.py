import os

import pytest

from spandex.load import DataLoader
from spandex.spatialtoolz import conform_srids


@pytest.fixture(scope='function')
def loader(request):
    """Recreate sample schema from shapefiles and tear down when done."""
    # Configure DataLoader to use data directory containing sample shapefiles.
    root_path = os.path.dirname(__file__)
    data_path = os.path.join(root_path, '../../test_data')
    loader = DataLoader(directory=data_path)

    # Recreate PostgreSQL sample schema.
    with loader.database.cursor() as cur:
        cur.execute("""
            CREATE EXTENSION IF NOT EXISTS postgis;
            DROP SCHEMA IF EXISTS sample CASCADE;
            CREATE SCHEMA sample;
        """)
    loader.database.refresh()

    # Load all shapefiles in test data directory.
    for filename in os.listdir(data_path):
        file_root, file_ext = os.path.splitext(filename)
        if file_ext.lower() == '.shp':
            shp_path = os.path.join(data_path, filename)
            table_name = 'sample.' + file_root
            loader.load_shp(shp_path, table_name)

    # Reproject all non-conforming SRIDs into project SRID.
    conform_srids(loader.srid, schema=loader.database.tables.sample)

    # Tear down sample schema when done.
    def teardown():
        with loader.database.cursor() as cur:
            cur.execute("DROP SCHEMA sample CASCADE;")
    request.addfinalizer(teardown)

    return loader
