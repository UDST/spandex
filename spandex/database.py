from contextlib import contextmanager

from geoalchemy2 import Geometry # Import needed for database reflection.
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class database(object):
    """
    Manages a connection to a Postgres database via Psycopg and GeoAlchemy.

    The class is meant to be used only as a class, never as an instance.

    Objects:
        tables:  GeoAlchemy database table objects, namespaced by schema.
        session: GeoAlchemy session manager.

    """
    tables = None
    session = None
    _connection = None
    _engine = None
    _model = None

    @classmethod
    def connect(cls, *args, **kwargs):
        """
        Create a connection to the database. All arguments are passed
        through to :py:func:`psycopg2.connect`.

        """
        if cls._connection is not None:
            cls.close()

        cls._connection = psycopg2.connect(*args, **kwargs)

        # Build GeoAlchemy engine.
        cls._engine = create_engine('postgresql://',
                                    creator=lambda: cls._connection)

        # Refresh GeoAlchemy ORM.
        cls.refresh()

    @classmethod
    def refresh(cls):
        """
        Refresh ORM and reload reflected tables from database schema.

        Run refresh after modifying database schema to find new tables/columns.

        """
        # Close existing session.
        if cls.session:
            cls.session.close()

        # Rebuild GeoAlchemy ORM.
        cls._model = declarative_base(cls._engine)
        Session = sessionmaker(bind=cls._engine)
        cls.session = Session()

        # Reflect tables in all PostgreSQL schemas.
        cls.tables = type('tables', (),
                          {'__doc__': "Reflected GeoAlchemy tables."})
        with cls.cursor() as cur:
            # Select list of PostgreSQL schemas.
            cur.execute("SELECT nspname FROM pg_namespace;")
            for row in cur:
                schema_name = row[0]
                if (not schema_name.startswith('pg_') and
                    schema_name != 'information_schema'):
                    # Assign each schema as attribute of tables and reflect.
                    schema = type(str(schema_name), (),
                                  {'__doc__': "Reflected GeoAlchemy schema."})
                    setattr(cls.tables, schema_name, schema)
                    cls._model.metadata.reflect(schema=schema_name)

        # Dynamically map all tables to classes.
        for (name, table) in cls._model.metadata.tables.items():
            schema_name, table_name = name.split('.')
            schema = getattr(cls.tables, schema_name)
            table = type(str(table_name), (cls._model,),
                         {'__table__': table,
                          '__doc__': "Reflected GeoAlchemy table."})
            setattr(schema, table_name, table)

    @classmethod
    def close(cls):
        """
        Close an existing connection.

        """
        if cls._connection is not None:
            cls._connection.close()
            cls._connection = None

    @classmethod
    def assert_connected(cls):
        """
        Raises an exception if there is no connection to the database.

        """
        if cls._connection is None or cls._connection.closed != 0:
            raise psycopg2.DatabaseError(
                'There is no connection to a database, '
                'call connection.connect to create one.')

    @classmethod
    @contextmanager
    def connection(cls):
        cls.assert_connected()

        with cls._connection as conn:
            yield conn

    @classmethod
    @contextmanager
    def cursor(cls):
        cls.assert_connected()

        with cls._connection as conn:
            with conn.cursor() as cur:
                yield cur
