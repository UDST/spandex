from contextlib import contextmanager

from geoalchemy2 import Geometry  # Needed for database reflection. # noqa
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class database(object):
    """
    Manages a connection to a Postgres database via Psycopg and GeoAlchemy.

    The class is meant to be used only as a class, never as an instance.

    Objects:
        tables :  GeoAlchemy database table objects, namespaced by schema.
        session : GeoAlchemy session manager.

    """
    tables = None
    _session = None
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

        Run refresh after modifying database schema to find new tables,
        columns, and other schema changes.

        To update all references to the table and column ORM classes, reassign
        class attributes and delete removed attributes. If the class itself
        was reassigned, assignments (references) to the class would be stale.

        """
        # Close existing session.
        if cls._session:
            cls._session.close()

        # Rebuild GeoAlchemy ORM.
        cls._model = declarative_base(cls._engine)
        Session = sessionmaker(bind=cls._engine)
        cls._session = Session()

        # Reflect tables in all PostgreSQL schemas.
        if not cls.tables:
            cls.tables = type('tables', (object,),
                              {'__doc__': "Reflected GeoAlchemy tables."})
        with cls.cursor() as cur:
            # Select list of PostgreSQL schemas.
            cur.execute("SELECT nspname FROM pg_namespace;")
            for row in cur:
                schema_name = row[0]
                if (not schema_name.startswith('pg_') and
                        schema_name != 'information_schema'):
                    # Assign each reflected schema as an attribute.
                    cls._model.metadata.reflect(schema=schema_name,
                                                extend_existing=True,
                                                autoload_replace=True)
                    if not hasattr(cls.tables, schema_name):
                        schema = type(str(schema_name), (object,),
                                      {'__doc__':
                                       "Reflected GeoAlchemy schema."})
                        setattr(cls.tables, schema_name, schema)

        # Dynamically map tables to classes. Update existing table and column
        # classes, which may be referenced, by reassigning their attributes.
        for (name, t) in cls._model.metadata.tables.items():
            schema_name, table_name = name.split('.')
            schema = getattr(cls.tables, schema_name)

            # Create new table class.
            table = type(str(name), (cls._model,),
                         {'__table__': t,
                          '__doc__': "Reflected GeoAlchemy table."})

            if hasattr(schema, table_name):
                # Table class exists. Update by reassigning attributes.
                old_table = getattr(schema, table_name)
                for (key, value) in table.__dict__.items():
                    if hasattr(old_table, key):
                        old_table_attr = getattr(old_table, key)
                        if hasattr(old_table_attr, '__dict__'):
                            # Table class attribute is itself an object, like
                            # a column class, which may be referenced. Update
                            # by reassigning existing object attributes to
                            # those from object in the new table.
                            for (subkey, subvalue) in value.__dict__.items():
                                setattr(old_table_attr, subkey, subvalue)
                            # Delete removed object attributes.
                            for subkey in list(old_table_attr.__dict__):
                                if subkey not in value.__dict__:
                                    delattr(old_table_attr, subkey)
                    else:
                        # Reassign existing table class attribute to
                        # attribute from the new table class.
                        setattr(old_table, key, value)
                # Delete removed table class attributes.
                for key in list(old_table.__dict__):
                    if key not in table.__dict__:
                        delattr(old_table, key)
            else:
                # Table class does not already exist. Set the new table class
                # as a schema attribute.
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

    @classmethod
    @contextmanager
    def session(cls):
        try:
            cls._session.flush()
            yield cls._session
            cls._session.commit()
        except:
            cls._session.rollback()
            raise
