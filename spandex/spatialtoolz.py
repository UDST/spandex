import logging
import os

from geoalchemy2 import Geometry
import pandas as pd
import pandas.io.sql as sql
from sqlalchemy import func

from .database import database as db
from .utils import DataLoader


# Set up logging system.
logging.basicConfig()
logger = logging.getLogger(__name__)


def tag(target_table_name, target_field, source_table_name, source_table_field,
        how='point_in_poly', target_df=None):
    """
    Tags target table with attribute of another table based on
    spatial relationship.

    Parameters
    ----------
    target_table_name : str
        Name of target table to be tagged.
    target_field : str
        Name of field in target table to add (if doesn't exist)
        or update (if exists).
    source_table_name : str
        Name of source table containing information to tag target table with.
    source_table_field : str
        Name of field in source table that contains the information.
    how : str, optional
        How to relate the two tables spatially.
        If not specified, defaults to 'point_in_poly'
    target_df : DataFrame, optional
        DataFrame to return a tagged copy of.

    Returns
    -------
    None : None
        Field is added or updated on the target_table in the database,
        and returns nothing. Unless target_df argument is used,
        in which case return value is pandas.DataFrame with
        the new/updated column.

    """
    check_srid_equality(target_table_name, source_table_name)

    if db_col_exists(target_table_name, target_field) is False:
        add_integer_field(target_table_name, target_field)

    if how == 'point_in_poly':
        if db_col_exists(target_table_name, 'centroid') is True:
            exec_sql(
                ("update {tname} set {tfield} = b.{sfield} "
                 "from {sname} b "
                 "where st_within({tname}.centroid, b.geom)"
                 ).format(
                    tname=target_table_name, tfield=target_field,
                    sfield=source_table_field, sname=source_table_name))
        else:
            exec_sql(
                ("update {tname} set {tfield} = b.{sfield} "
                 "from {sname} b "
                 "where st_within(ST_centroid({tname}.geom), b.geom)"
                 ).format(
                    tname=target_table_name, tfield=target_field,
                    sfield=source_table_field, sname=source_table_name))

    if target_df:
        return update_df(target_df, target_field, target_table_name)


def proportion_overlap(target_table, over_table, column_name, df=None):
    """
    Calculates proportion overlap between target table's geometry and another
    table's geometry. Populates column in target table with proportion
    overlap value.

    Parameters
    ----------
    target_table : sqlalchemy.ext.declarative.api.DeclarativeMeta
        Target table ORM class containing geometry to overlap.
    over_table : sqlalchemy.ext.declarative.api.DeclarativeMeta
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
    if column_name not in target_table.__table__.columns:
        add_column(target_table, column_name, 'numeric')
    column = getattr(target_table, column_name)

    # Pre-calculate column area.
    calc_area(target_table)

    # Do the calculation.
    db.session.query(target_table
    ).filter(
        target_table.geom.ST_Intersects(over_table.geom)
    ).update(
        {column: func.sum(
                 target_table.geom.ST_Intersection(over_table.geom).ST_Area()
                 ).scalar() / target_table.calc_area}
    )

    if df:
        return update_df(df, column_name, target_table)


def get_srid(column):
    """Returns SRID of specified column."""
    col = column.property.columns[0]
    return col.type.srid


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
        for column in table.__table__.columns:
            if isinstance(column.type, Geometry):
                # Column is geometry column.
                srids.add(column.type.srid)

    # Projection is unique if set has single SRID.
    assert len(srids) > 0
    if len(srids) == 1:
        return True
    else:
        return False


def calc_area(table):
    """
    Calculate geometric area and store value in calc_area column.

    """
    # Add calc_area column if it does not already exist..
    if 'calc_area' not in table.__table__.columns:
        column_added = True
        column = add_column(table, 'calc_area', 'numeric')

    try:
        db.session.query(table).update({table.calc_area:
            table.geom.ST_Area()})
        db.session.commit()
    except:
        # Remove column if it was freshly added and exception raised.
        if column_added:
            remove_column(column)
        raise


def invalid_geometry_diagnostic(table_name, id_field):
    """"""
    """
    Returns DataFrame with diagnostic information for only records
    with invalid geometry. Returned columns include record identifier,
    whether geometry is simple, and reason for invalidity.

    Parameters
    ----------
    table_name : str
        Name of database table to diagnose.
    id_field : str
        Name of unique identifier field in database table.  Can be any field.

    Returns
    -------
    df : pandas.DataFrame
        Table with all records that have invalid geometry, with
        diagnostic information.

    """
    return db_to_df(
        ("SELECT * FROM ("
         "SELECT {field}, ST_IsValid(geom) as valid, "
         "ST_IsSimple(geom) as simple,  ST_IsValidReason(geom), geom FROM {tname}"
         ") AS t WHERE NOT(valid);").format(field=id_field, tname=table_name))


def duplicate_stacked_geometry_diagnostic(table_name):
    """
    Returns DataFrame with all records that have duplicate, stacked geometry.

    Parameters
    ----------
    table_name : str
        Name of database table to diagnose.

    Returns
    -------
    df : pandas.DataFrame
        Table with all records that have duplicate, stacked geometry.

    """
    return db_to_df(
        ("SELECT * FROM {tname} "
         "where geom in (select geom from {tname} "
         "group by geom having count(*) > 1)").format(tname=table_name))


def update_df(df, column_name, db_table_name):
    """
    Adds/updates column in DataFrame from database table.
    Database table must contain field with the same name as
    DataFrame's index (df.index.name).

    Parameters
    ----------
    df : DataFrame
        Table to add column to.
    column_name : str
        Name of field in database table to add to DataFrame.
        This is also the name of the column to add/update in the DataFrame.
    db_table_name : str
        Database table containing field to add/update DataFrame.

    Returns
    -------
    df : pandas.DataFrame
        Table with new/updated column.

    """
    df_idx_name = df.index.name
    new_col = db_to_df(
        "select {idx}, {col} from {tname}".format(
            idx=df_idx_name, col=column_name, tname=db_table_name)
        ).set_index(df_idx_name)[column_name]
    df[column_name] = new_col
    return df


def db_col_exists(table, column_name):
    """Return whether column exists in database table."""
    if column_name in table.__table__.columns:
        return True
    else:
        return False


def add_column(table, column_name, type_name, default=None):
    """
    Add column to table.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.api.DeclarativeMeta
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
        Column ORM class that was added.

    """
    # Specify sensible defaults for integer and numeric types.
    if not default:
        default_map = {'integer': '0',
                       'numeric': '0.0'}
        default = default_map[type_name]

    t = table.__table__
    with db.cursor() as cur:
        cur.execute("""
            ALTER TABLE {schema}.{table}
            ADD COLUMN {column} {type} DEFAULT {default};
        """.format(
            schema=t.schema, table=t.name,
            column=column_name, type=type_name, default=default))
    db.refresh()
    return getattr(table, column_name)


def remove_column(column):
    """Remove column from table."""
    col = column.property.columns[0]
    t = col.table
    with db.cursor() as cur:
        cur.execute("""
            ALTER TABLE {schema}.{table}
            DROP COLUMN '{column}';
        """.format(schema=t.schema, table=t.name, column=col.name))
    db.refresh()


def exec_sql(query, params=None):
    """Executes SQL query."""
    with db.cursor() as cur:
        cur.execute(query, params)


def db_to_df(query, params=None):
    """Executes SQL query and returns DataFrame."""
    with db.connection() as conn:
        return sql.read_sql(query, conn, params=params)


def reproject(table=None, column=None):
    """
    Reprojects table into the SRID specified in the project config.

    Either a table or a column must be specified. If a table is specified,
    the geom column will be reprojected.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.api.DeclarativeMeta
        Table ORM class containing column.
    column : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column ORM class to project.

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
                ALTER COLUMN '{g_name}' TYPE geometry({g_type}, {psrid})
                USING ST_Transform('{g_name}', {psrid});
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
    schema : schema object
        If schema is specified, only SRIDs within the specified schema
        are conformed.

    Returns
    -------
    None

    """
    project_srid = DataLoader().srid

    # Iterate over all columns. Reproject geometry columns with SRIDs
    # that differ from project SRID.
    for schema_obj in db.tables:
        if not schema or schema_obj.__name__ == schema.__name__:
            for table in schema_obj:
                for column in table.__table__.columns:
                    if isinstance(column.type, Geometry):
                        # Column is geometry column. Reproject if SRID
                        # differs from project SRID.
                        srid = column.type.srid
                        if srid != project_srid:
                            reproject(table, geometry_column=column.name)


def vacuum(table):
    """
    VACUUM and then ANALYZE table.

    VACUUM reclaims storage from deleted or obselete tuples.
    ANALYZE updates statistics used by the query planner to determine the most
    efficient way to execute a query.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.api.DeclarativeMeta
        Table ORM class to vacuum.

    Returns
    -------
    None

    """
    # Vacuum
    t = table.__table__
    with db.connection() as conn:
        assert conn.autocommit == False
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("VACUUM ANALYZE {schema}.{table};".format(
                schema=t.schema, table=t.name))
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
    dtypes[dtypes == 'float64'] = 'numeric'
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
            return os.path.join(DataLoader().directory,input_dir, shp_table_name, shp_path)
        return func
    for category in files:
        path_func = subpath(category)
        del_dict = files[category]
        for name in del_dict:
            path = path_func(name, del_dict[name][0])
            table_name = del_dict[name][1]
            delimiter = del_dict[name][2]
            print 'Loading %s as %s' % (del_dict[name][0], table_name)
            load_delimited_file(path, table_name, delimiter=delimiter)
