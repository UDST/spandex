from contextlib import contextmanager

import psycopg2


class database(object):
    """
    This class manages a connection to a Postgres database via psycopg2.

    The class is meant to be used only as a class, never as an instance.

    """
    _connection = None

    @classmethod
    def connect(cls, *args, **kwargs):
        """
        Create a connection to the database. All arguments are passed
        through to :py:func:`psycopg2.connect`.

        """
        if cls._connection is not None:
            cls.close()

        cls._connection = psycopg2.connect(*args, **kwargs)

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
