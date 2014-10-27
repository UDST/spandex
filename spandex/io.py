import logging

import pandas as pd
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Query

from .database import database as db, CreateTableAs


# Set up logging system.
logging.basicConfig()
logger = logging.getLogger(__name__)


def update_df(df, column, table):
    """
    Add or update column in DataFrame from database table.

    Database table must contain column with the same name as
    DataFrame's index (df.index.name).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to return an updated copy of.
    column : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column ORM object to update DataFrame with.
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class containing columns to update with and index on.

    Returns
    -------
    df : pandas.DataFrame

    """
    # Get table column to use as index based on DataFrame index name.
    index_column = getattr(table, df.index.name)

    # Query index column and column to update DataFrame with.
    with db.session() as sess:
        q = sess.query(index_column, column)

    # Update DataFrame column.
    new_df = db_to_df(q, index_name=df.index.name)
    df[column.name] = new_df[column.name]
    return df


def add_column(table, column_name, type_name, default=None):
    """
    Add column to table.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to add column to.
    column_name : str
        Name of column to add to table.
    type_name : str
        Name of column type.
    default : str, optional
        Default value for column. Must include quotes if string.

    Returns
    -------
    column : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column ORM object that was added.

    """
    if default:
        default_str = "DEFAULT {}".format(default)
    else:
        default_str = ""

    t = table.__table__
    with db.cursor() as cur:
        cur.execute("""
            ALTER TABLE {schema}.{table}
            ADD COLUMN {column} {type} {default_str};
        """.format(
            schema=t.schema, table=t.name,
            column=column_name, type=type_name, default_str=default_str))
    db.refresh()
    return getattr(table, column_name)


def remove_column(column):
    """Remove column from table."""
    col = column.property.columns[0]
    t = col.table
    with db.cursor() as cur:
        cur.execute("""
            ALTER TABLE {schema}.{table}
            DROP COLUMN {column};
        """.format(schema=t.schema, table=t.name, column=col.name))
    db.refresh()


def exec_sql(query, params=None):
    """Execute SQL query."""
    with db.cursor() as cur:
        cur.execute(query, params)


def db_to_query(orm):
    """Convert table or list of ORM objects to a query."""
    if isinstance(orm, Query):
        # Assume input is Query object.
        return orm
    elif hasattr(orm, '__iter__'):
        # Assume input is list of ORM objects.
        with db.session() as sess:
            return sess.query(*orm)
    else:
        # Assume input is single ORM object.
        with db.session() as sess:
            return sess.query(orm)


def db_to_db(query, name, schema=None, view=False):
    """
    Create a table or view from Query, table, or ORM objects, like columns.

    Do not use to duplicate a table. The new table will not contain
    indexes or constraints, including primary keys.

    Parameters
    ----------
    query : sqlalchemy.orm.Query, sqlalchemy.ext.declarative.DeclarativeMeta,
            or iterable
        Query ORM object, table ORM class, or list of ORM objects to query,
        like columns.
    name : str
        Name of table or view to create.
    schema : schema class, optional
        Schema of table to create. Defaults to public.
    view : bool, optional
        Whether to create a view instead of a table. Defaults to False.

    Returns
    -------
    None

    """
    if schema:
        schema_name = schema.__name__
    else:
        schema_name = 'public'
    qualified_name = schema_name + "." + name

    q = db_to_query(query)

    # Create new table from results of the query.
    with db.session() as sess:
        sess.execute(CreateTableAs(qualified_name, q, view))
    db.refresh()


def db_to_df(query, index_name=None):
    """
    Return DataFrame from Query, table, or ORM objects, like columns.

    Parameters
    ----------
    query : sqlalchemy.orm.Query, sqlalchemy.ext.declarative.DeclarativeMeta,
            or iterable
        Query ORM object, table ORM class, or list of ORM objects to query,
        like columns.
    index_name : str, optional
        Name of column to use as DataFrame index. If provided, column
        must be contained in query.

    Returns
    -------
    df : pandas.DataFrame

    """
    q = db_to_query(query)

    # Convert Query object to DataFrame.
    entities = q.column_descriptions
    if (len(entities) == 1 and
            isinstance(entities[0]['type'], DeclarativeMeta)):
        # If we query a table, column_descriptions refers to the table itself,
        # not its columns.
        table = q.column_descriptions[0]['type']
        column_names = table.__table__.columns.keys()
    else:
        column_names = [desc['name'] for desc in q.column_descriptions]
    data = [rec.__dict__ for rec in q.all()]
    df = pd.DataFrame.from_records(data, index=index_name,
                                   columns=column_names, coerce_float=True)
    return df


def vacuum(table):
    """
    VACUUM and then ANALYZE table.

    VACUUM reclaims storage from deleted or obselete tuples.
    ANALYZE updates statistics used by the query planner to determine the most
    efficient way to execute a query.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to vacuum.

    Returns
    -------
    None

    """
    # Vacuum
    t = table.__table__
    with db.connection() as conn:
        assert conn.autocommit is False
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("VACUUM ANALYZE {schema}.{table};".format(
                    schema=t.schema, table=t.name))
        finally:
            conn.autocommit = False
