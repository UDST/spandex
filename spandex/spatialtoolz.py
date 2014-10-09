import logging
import os

from geoalchemy2 import Geometry
import pandas as pd
from sqlalchemy import func
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import aliased, Query

from .database import database as db, CreateTableAs
from .utils import DataLoader


# Set up logging system.
logging.basicConfig()
logger = logging.getLogger(__name__)


def tag(target_table, target_column_name, source_table, source_column_name,
        how='point_in_poly', df=None):
    """
    Tag target table with attribute of a spatially-related source table.

    Parameters
    ----------
    target_table : sqlalchemy.ext.declarative.DeclarativeMeta
        Target table ORM class to be tagged.
    target_column_name : str
        Name of column in target table to add (if doesn't exist)
        or update (if exists). This where the tag value will be stored.
    source_table : sqlalchemy.ext.declarative.DeclarativeMeta
        Source table ORM class containing information to tag target table.
    source_column_name : str
        Name of column in source table that contains the tagging information.
    how : str, optional
        How to relate the two tables spatially.
        If not specified, defaults to 'point_in_poly'.
        Other spatial relationships are not currently supported.
    df : pandas.DataFrame, optional
        DataFrame to return a tagged copy of.

    Returns
    -------
    None
        However, if df argument is provided, pandas.DataFrame with the
        new or updated column is returned.

    """
    # Other spatial relationships are not supported.
    if how != "point_in_poly":
        raise ValueError("Only how='point_in_poly' is supported, not "
                         "how='{}',".format(how))

    # Table projections must be equal.
    assert srid_equality([target_table, source_table])

    # Get source column ORM object.
    source_column = getattr(source_table, source_column_name)

    # Add target column to target table if it does not already exist.
    if target_column_name in target_table.__table__.columns:
        target_column = getattr(target_table, target_column_name)
    else:
        # Use data type of source column for new column.
        dtype = source_column.property.columns[0].type.compile()
        target_column = add_column(target_table, target_column_name, dtype)

    # Tag target table with column from source table.
    with db.session() as sess:
        sess.query(target_table).filter(
            target_table.geom.ST_Centroid().ST_Within(source_table.geom)
        ).update(
            {target_column: source_column},
            synchronize_session=False
        )

    if df:
        return update_df(df, target_column, target_table)


def proportion_overlap(target_table, over_table, column_name, df=None):
    """
    Calculate proportion of target table geometry overlap.

    Calculate proportion of geometry area in each row of target table that
    is overlapped by another table's geometry. Populate specified column in
    target table with proportion overlap value.

    Parameters
    ----------
    target_table : sqlalchemy.ext.declarative.DeclarativeMeta
        Target table ORM class containing geometry to overlap.
    over_table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class containing overlapping geometry.
    column_name : str
        Name of column in target table to add (if doesn't exist) or
        update (if exists). This is where the proportion overlap value
        will be stored.
    df : pandas.DataFrame, optional
        DataFrame to return a copy of with proportion overlap calculation.

    Returns
    -------
    None
        However, if df argument is provided, pandas.DataFrame with the
        new or updated column is returned.

    """
    # Table projections must be equal.
    assert srid_equality([target_table, over_table])

    # Add column to target table if it does not already exist.
    if column_name in target_table.__table__.columns:
        column = getattr(target_table, column_name)
    else:
        column = add_column(target_table, column_name, 'float')

    # Pre-calculate column area.
    calc_area(target_table)

    # Calculate proportion of overlapping area for each target table row.
    with db.session() as sess:
        proportion_overlap = sess.query(
            func.sum(
                target_table.geom.ST_Intersection(over_table.geom).ST_Area()
            ) / target_table.calc_area
        ).filter(
            target_table.geom.ST_Intersects(over_table.geom)
        ).group_by(
            target_table.geom
        )
        sess.query(target_table).update(
            {column: proportion_overlap.selectable},
            synchronize_session=False
        )

    if df:
        return update_df(df, column, target_table)


def trim(target_col, trim_col):
    """"
    Trim target geometry by removing intersection with a trim column.

    Parameters
    ----------
    target_col : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column ORM object to trim.
    trim_col : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column ORM object to trim target column with.

    Returns
    -------
    None

    """
    # TODO: Aggregate multiple rows in trim_col.
    # Needs testing to make sure that ST_Difference can handle MultiPolygons
    # without data loss.
    with db.session() as sess:
        data_type = target_col.property.columns[0].type
        geom_type = data_type.geometry_type
        if geom_type.lower() == "multipolygon":
            # ST_Difference outputs Polygon, not MultiPolygon.
            # Temporarily change the geometry data type to generic Geometry.
            column_name = target_col.name
            table_name = target_col.parent.tables[0].name
            schema_name = target_col.parent.tables[0].schema
            srid = data_type.srid
            sess.execute("""
                ALTER TABLE {schema}.{table} ALTER COLUMN {column}
                SET DATA TYPE geometry(Geometry, {srid});
            """.format(
                schema=schema_name, table=table_name, column=column_name,
                srid=srid)
            )

        # Update column value with ST_Difference if ST_Intersects.
        table = target_col.parent
        sess.query(table).filter(
            target_col.ST_Intersects(trim_col)
        ).update(
            {target_col: target_col.ST_Difference(trim_col)},
            synchronize_session=False
        )

        if geom_type.lower() == "multipolygon":
            # Coerce the geometry data type back to MultiPolygon.
            sess.execute("""
                ALTER TABLE {schema}.{table} ALTER COLUMN {column}
                SET DATA TYPE geometry(MultiPolygon, {srid})
                USING ST_Multi(geom);
            """.format(
                schema=schema_name, table=table_name, column=column_name,
                srid=srid)
            )


def srid_equality(tables):
    """
    Check whether there is only one projection in list of tables.

    Parameters
    ----------
    tables: iterable
        List of table ORM classes to inspect geometry columns.

    Returns
    -------
    unique: boolean

    """
    # Iterate over all columns to build set of SRIDs.
    srids = set()
    for table in tables:
        for c in table.__table__.columns:
            if isinstance(c.type, Geometry):
                # Column is geometry column.
                srids.add(c.type.srid)

    # Projection is unique if set has single SRID.
    assert len(srids) > 0
    if len(srids) == 1:
        return True
    else:
        return False


def calc_area(table):
    """
    Calculate area in units of projection and store value in calc_area column.

    """
    # Add calc_area column if it does not already exist..
    if 'calc_area' in table.__table__.columns:
        column_added = False
        column = table.calc_area
    else:
        column_added = True
        column = add_column(table, 'calc_area', 'float')

    # Calculate geometric area.
    try:
        with db.session() as sess:
            sess.query(table).update(
                {column: table.geom.ST_Area()},
                synchronize_session=False
            )
    except:
        # Remove column if it was freshly added and exception raised.
        if column_added:
            remove_column(column)
        raise


def calc_dist(table, geom):
    """
    Calculate distance between a table of geometries and a geometry column.

    Calculates the minimum Cartesian distance in units of projection between
    each geometry in the table and the nearest point in the geometry column.
    Geometries must have the same projection (SRID).

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class with geom column to calculate distance from. Value is
        stored in the calc_dist column, which is created if it does not exist.
    geom : sqlalchemy.orm.Query,
           sqlalchemy.orm.attributes.InstrumentedAttribute
        ORM object to calculate distance to, like a column or query.
        Must contain only one column. Rows are aggregated into a MULTI object
        with ST_Collect (faster union that does not dissolve boundaries).

    Returns
    -------
    column : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column containing distances from the table to the geometry column.

    """
    # Add calc_dist column if it does not already exist..
    if 'calc_dist' in table.__table__.columns:
        column_added = False
        column = table.calc_dist
    else:
        column_added = True
        column = add_column(table, 'calc_dist', 'float')

    # Calculate geometric distance.
    try:
        with db.session() as sess:
            # Aggregate geometry column into single MULTI object.
            multi = sess.query(
                func.ST_Collect(
                    db_to_query(geom).label('geom')
                )
            )
            # Calculate distances from table geometries to MULTI object.
            sess.query(table).update(
                {column: table.geom.ST_Distance(multi)},
                synchronize_session=False
            )
        return column
    except:
        # Remove column if it was freshly added and exception raised.
        if column_added:
            remove_column(column)
        raise


def geom_invalid(table, column=None):
    """
    Return DataFrame with information on records with invalid geometry.

    Returned columns include record identifier, whether geometry is simple,
    and reason for invalidity.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to diagnose.
    column : sqlalchemy.orm.attributes.InstrumentedAttribute, optional
        Column ORM object to use as index.

    Returns
    -------
    df : pandas.DataFrame

    """
    # Build list of columns to return, including optional index.
    columns = [func.ST_IsSimple(table.geom).label('simple'),
               func.ST_IsValidReason(table.geom).label('reason'),
               table.geom]
    if column:
        columns.append(column)

    # Query information on rows with invalid geometries.
    with db.session() as sess:
        q = sess.query(
            *columns
        ).filter(
            ~table.geom.ST_IsValid()
        )

    # Convert query to DataFrame.
    if column:
        df = db_to_df(q, index_name=column.name)
    else:
        df = db_to_df(q)
    return df


def geom_duplicate(table):
    """
    Return DataFrame with all records that have identical, stacked geometry.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to diagnose.

    Returns
    -------
    df : pandas.DataFrame

    """
    # Create table aliases to cross join table to self.
    table_a = aliased(table)
    table_b = aliased(table)

    # Query rows with duplicate geometries.
    with db.session() as sess:
        geoms = sess.query(
            table_a.geom
        ).filter(
            table_a.gid < table_b.gid,
            func.ST_Equals(table_a.geom, table_b.geom)
        )
        rows = sess.query(table).filter(
            table.geom.in_(geoms)
        )

    # Convert query to DataFrame.
    df = db_to_df(rows)
    return df


def geom_overlapping(table, key_name, output_table_name):
    """
    Export overlapping geometries from a table into another table.

    The exported table contains the following columns:
        key_name_a, key_name_b: identifiers of the overlapping pair
        relation: DE-9IM representation of their spatial relation
        geom_a, geom_b: corresponding geometries
        overlap: 2D overlapping region (polygons)

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to query for overlapping geometries.
    key_name : str
        Name of column in the queried table containing a unique identifier,
        such as a primary key, to use for cross join and to identify
        geometries in the exported table.
    output_table_name : str
        Name of exported table. Table is created in the same schema as
        the queried table.

    Returns
    -------
    None

    """
    # Create table aliases to cross join table to self.
    table_a = aliased(table)
    table_b = aliased(table)
    table_a_key = getattr(table_a, key_name).label(key_name + '_a')
    table_b_key = getattr(table_b, key_name).label(key_name + '_b')

    # Query for overlaps.
    with db.session() as sess:
        q = sess.query(
            table_a_key, table_b_key,
            func.ST_Relate(table_a.geom, table_b.geom).label('relation'),
            table_a.geom.label('geom_a'), table_b.geom.label('geom_b'),
            # Extract only polygon geometries from intersection.
            func.ST_CollectionExtract(
                func.ST_Intersection(table_a.geom, table_b.geom),
                3
            ).label('overlap')
        ).filter(
            # Use "<" instead of "!=" to prevent duplicates and save time.
            table_a_key < table_b_key,
            func.ST_Intersects(table_a.geom, table_b.geom),
            # Polygon interiors must not intersect.
            ~func.ST_Relate(table_a.geom, table_b.geom, 'FF*F*****')
            # Alternatively, can use ST_Overlaps, ST_Contains, and ST_Within
            # to check for overlap instead of ST_Relate, but this was
            # slightly slower in my testing.
            # or_(
            #     table_a.geom.ST_Overlaps(table_b.geom),
            #     table_a.geom.ST_Contains(table_b.geom),
            #     table_a.geom.ST_Within(table_b.geom)
            # )
        )

    # Create new table from query. This table does not contain constraints,
    # such as primary keys.
    schema = getattr(db.tables, table.__table__.schema)
    db_to_db(q, output_table_name, schema)


def geom_unfilled(table, output_table_name):
    # Query rows containing geometries with interior rings.
    # Add column for unfilled geometry.
    # WIP: Ignore unfilled areas that are overlapped by another row.
    with db.session() as sess:
        q = sess.query(
            table,
            func.ST_Difference(
                func.ST_MakePolygon(
                    func.ST_ExteriorRing(
                        func.ST_Dump(table.geom).geom
                    )
                ),
                table.geom
            ).label('unfilled')
        ).filter(
            func.ST_NRings(table.geom) > 1,
        )

    # Create new table from query. This table does not contain constraints,
    # such as primary keys.
    schema = getattr(db.tables, table.__table__.schema)
    db_to_db(q, output_table_name, schema)


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


def db_to_db(query, name, schema, view=False):
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


def reproject(table=None, column=None):
    """
    Reproject table into the SRID specified in the project config.

    Either a table or a column must be specified. If a table is specified,
    the geom column will be reprojected.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta, optional
        Table ORM class containing geom column to reproject.
    column : sqlalchemy.orm.attributes.InstrumentedAttribute, optional
        Column ORM object to reproject.

    Returns
    -------
    None

    """
    project_srid = DataLoader().srid

    # Get Table and Column objects.
    if column:
        geom = column.property.columns[0]
        t = geom.table
    else:
        t = table.__table__
        geom = t.c.geom

    # Reproject using ST_Transform if column SRID differs from project SRID.
    if project_srid != geom.type.srid:
        with db.cursor() as cur:
            cur.execute("""
                ALTER TABLE {schema}.{table}
                ALTER COLUMN {g_name} TYPE geometry({g_type}, {psrid})
                USING ST_Transform({g_name}, {psrid});
            """.format(
                schema=t.schema, table=t.name,
                g_name=geom.name, g_type=geom.type.geometry_type,
                psrid=project_srid))
    else:
        logger.warn("Table {table} already in SRID {srid}".format(
            table=t.name, srid=project_srid))

    # Refresh ORM.
    db.refresh()


def conform_srids(schema=None):
    """
    Reproject all non-conforming geometry columns into project SRID.

    Parameters
    ----------
    schema : schema class
        If schema is specified, only SRIDs within the specified schema
        are conformed.

    Returns
    -------
    None

    """
    project_srid = DataLoader().srid

    # Iterate over all columns. Reproject geometry columns with SRIDs
    # that differ from project SRID.
    for schema_name, schema_obj in db.tables.__dict__.items():
        if not schema_name.startswith('_'):
            if not schema or schema_name == schema.__name__:
                for table_name, table in schema_obj.__dict__.items():
                    if not table_name.startswith('_'):
                        for c in table.__table__.columns:
                            if isinstance(c.type, Geometry):
                                # Column is geometry column. Reproject if SRID
                                # differs from project SRID.
                                srid = c.type.srid
                                if srid != project_srid:
                                    column = getattr(table, c.name)
                                    reproject(table, column)


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


def load_delimited_file(file_path, table_name, delimiter=',', append=False):
    """
    Load a delimited file to the database.

    Parameters
    ----------
    file_path : str
        The full path to the delimited file.
    table_name : str
        The name given to the table on the database or the table to append to.
    delimiter : str, optional
        The delimiter symbol used in the input file. Defaults to ','.
        Other examples include tab delimited '\t' and
        vertical bar delimited '|'.
    append: boolean, optional
        Determines whether a new table is created (dropping existing table
        if exists) or rows are appended to existing table.
        If append=True, table schemas must be identical.

    Returns
    -------
    None
        Loads delimited file to database

    """
    delimited_file = pd.read_csv(file_path, delimiter=delimiter)
    dtypes = pd.Series(list(delimited_file.dtypes))
    dtypes[dtypes == 'object'] = 'character varying'
    dtypes[dtypes == 'int64'] = 'integer'
    dtypes[dtypes == 'int32'] = 'integer'
    dtypes[dtypes == 'float64'] = 'float'
    cols = pd.Series(list(delimited_file.columns))
    cols = cols.str.replace(' ', '_')
    cols = cols.str.replace('\'', '')
    cols = cols.str.replace('\"', '')
    cols = cols.str.replace('\(', '')
    cols = cols.str.replace('\)', '')
    cols = cols.str.replace('\+', '')
    cols = cols.str.replace('\:', '')
    cols = cols.str.replace('\;', '')
    columns = ''
    for col, tp in zip(list(cols), list(dtypes)):
        columns = columns + col + ' ' + tp + ','
    columns = columns[:-1]
    if not append:
        exec_sql("DROP TABLE IF EXISTS {table};".format(table=table_name))
        exec_sql("CREATE TABLE {table} ({cols});".format(
            table=table_name, cols=columns))
    exec_sql("SET CLIENT_ENCODING='LATIN1';")
    exec_sql(
        "COPY {table} FROM '{file}' DELIMITER '{delim}' CSV HEADER;".format(
            table=table_name, file=file_path, delim=delimiter))


def load_multiple_delimited_files(files, config_filename=None):
    """
     Load multiple delimited text files to Postgres according to a given dictionary
    of file information.

    Parameters
    ----------
    files : dict
        Dictionary of dictionaries where the top-level key is file category,
        which also corresponds to the name of the directory within the data_dir
        containing this category of files. The sub-dictionaries are
        dictionaries where the keys correspond to the geography name and the
        value is a tuple of the form (file_name, table_name, delimiter).  If SRID is
        None, then default config SRID is used.

        Example dictionary
             {'parcels' :  ##Looks for 'parcels' directory within the data_dir
                  ##Looks for 'marin' directory within parcels dir
                  {'alameda':('alameda_parcel_info.txt', 'alameda_pcl_info', '\t'),
                  'napa':('napa_parcel_info.csv', 'napa_pcl_info', ','),
                  }
             }
    config_filename : str, optional
        Path to additional configuration file.
        If None, configuration must be provided in default locations.
        Configuration should specify the input data directory (data_dir).
        The data_dir should contain subdirectories corresponding to each
        shapefile category, which in turn should contain a subdirectory
        for each shapefile.

    Returns
    -------
    None : None
        Loads delimited files to the database (returns nothing)

    """
    def subpath(base_dir):
        def func(shp_table_name, shp_path):
            input_dir = base_dir
            return os.path.join(DataLoader().directory, input_dir, shp_table_name, shp_path)
        return func
    for category in files:
        path_func = subpath(category)
        del_dict = files[category]
        for name in del_dict:
            path = path_func(name, del_dict[name][0])
            table_name = del_dict[name][1]
            delimiter = del_dict[name][2]
            print('Loading %s as %s' % (del_dict[name][0], table_name))
            load_delimited_file(path, table_name, delimiter=delimiter)
